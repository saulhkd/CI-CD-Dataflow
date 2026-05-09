""" 
Script: Dataflow Streaming Pipeline

Description:
This script implements a Dataflow streaming pipeline using Apache Beam to process real-time podcast user interaction events. The pipeline performs the following steps:
1. Ingests playback, engagement, and quality events from Pub/Sub subscriptions.
2. Normalizes and merges the events into a unified format.
3. Computes real-time user metrics using session-based windows.
4. Computes real-time content metrics using sliding windows.
5. Stores user and content metrics in BigQuery.
6. Generates notifications based on user behavior and content trends, storing them in Firestore and publishing them to Pub/Sub.
 
EDEM. Master Big Data & Cloud 2025/2026
Professor: Javi Briones & Adriana Campos
"""

""" Import Libraries """

# A. Apache Beam Libraries
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.transforms.window import Sessions, SlidingWindows
from apache_beam.utils.timestamp import Timestamp

# B. Python Libraries
import argparse
import logging
import uuid
import json

""" Code: Helpful functions """

def parsePubSubMessage(message):

    """
    Parse Pub/Sub message from bytes to dictionary.
    Args:
        message (bytes): Pub/Sub message in bytes.
    Returns:
        dict: Parsed message as a dictionary.
    """

    message_str = message.decode('utf-8')
    message_dict = json.loads(message_str)

    logging.info(f"Parsed message: {message_dict}")

    return message_dict

def normalizePlaybackEvent(event):
    
    """
    Normalize playback event data.
    Args:
        event (dict): Raw playback event data.
    Returns:
        dict: Normalized playback event data.
    """

    w = {
        "PLAY": 1, 
        "PAUSE": 0, 
        "RESUME": 0, 
        "STOP": -1, 
        "COMPLETE": 3
    }.get(event["event_type"], 0)
    
    return {
        "user_id": event["user_id"],
        "episode_id": event["episode_id"],
        "type": event["event_type"].lower(),
        "w": w,
    }

def normalizeEngagementEvent(event):
    
    """
    Normalize engagement event data.
    Args:
        event (dict): Raw engagement event data.
    Returns:
        dict: Normalized engagement event data.
    """

    w = {
        "SAVE_EPISODE": 4, 
        "FOLLOW_SHOW": 3, 
        "SHARE": 2
    }.get(event["event_type"], 0)
    
    return {
        "user_id": event["user_id"],
        "episode_id": event.get("episode_id", ""),
        "type": event["event_type"].lower(),
        "w": w,
    }

def normalizeQualityEvent(event):

    """
    Normalize quality event data.
    Args:
        event (dict): Raw quality event data.
    Returns:
        dict: Normalized quality event data.
    """

    w = {
        "BUFFERING_START": -1, 
        "BUFFERING_END": -0.2, 
        "DROPOUT": -2
    }.get(event["event_type"], 0)
    
    return {
        "user_id": event["user_id"],
        "episode_id": event["episode_id"],
        "type": event["event_type"].lower(),
        "w": w,
    }

class UserMetricsFn(beam.DoFn):

    METRICS = "metrics"
    NOTIFY = "notify"

    def process(self, element, window=beam.DoFn.WindowParam):

        user_id, events = element

        plays = completes = stops = 0
        score = 0.0
        last_episode_id = None
        last_position_sec = 0

        for e in events:
            
            t = e.get("type", "")
            score += float(e.get("w", 0.0))

            if t == "play":
                plays += 1
            elif t == "complete":
                completes += 1
            elif t == "stop":
                stops += 1
                last_episode_id = e.get("episode_id", last_episode_id)
                last_position_sec = int(e.get("position_sec", 0))

            if last_episode_id is None and e.get("episode_id"):
                last_episode_id = e["episode_id"]

        # metrics output
        yield beam.pvalue.TaggedOutput(
            self.METRICS, {
                "user_id": user_id,
                "window_start": window.start.to_utc_datetime().isoformat(),
                "window_end": window.end.to_utc_datetime().isoformat(),
                "plays": plays,
                "completes": completes,
                "score": score,
            }   
        )

        # notification output
        if stops > 0 and completes == 0 and last_episode_id:

            yield beam.pvalue.TaggedOutput(
                self.NOTIFY, {
                    "notification_id": str(uuid.uuid4()),
                    "created_at": Timestamp.now().to_utc_datetime().isoformat(),
                    "type": "CONTINUE_LISTENING",
                    "user_id": user_id,
                    "ttl_sec": 1800,
                    "payload": {"episode_id": last_episode_id, "resume_position_sec": last_position_sec},
                }
            )

class ContentMetricsFn(beam.DoFn):
    
    METRICS = "metrics"
    NOTIFY = "notify"

    def __init__(self, trending_plays_threshold=50):
        self.th = trending_plays_threshold

    def process(self, element, window=beam.DoFn.WindowParam):
        
        episode_id, events = element

        plays = completes = 0
        score = 0.0

        for e in events:
            t = e.get("type", "")
            score += float(e.get("w", 0.0))
            if t == "play":
                plays += 1
            elif t == "complete":
                completes += 1

        # metrics output
        yield beam.pvalue.TaggedOutput(
            self.METRICS, {
                "episode_id": episode_id,
                "window_start": window.start.to_utc_datetime().isoformat(),
                "window_end": window.end.to_utc_datetime().isoformat(),
                "plays": plays,
                "completes": completes,
                "score": score,
            }
        )

        # notification output (simple: trending spike)
        if plays >= self.th:
            yield beam.pvalue.TaggedOutput(
                self.NOTIFY, {
                    "notification_id": str(uuid.uuid4()),
                    "created_at": Timestamp.now().to_utc_datetime().isoformat(),
                    "type": "TRENDING_NOW",
                    "ttl_sec": 600,
                    "payload": {"episode_id": episode_id, "plays_in_window": plays, "score": score},
                }
            )

class FormatFirestoreDocument(beam.DoFn):

    def __init__(self,firestore_collection, project_id):
        self.firestore_collection = firestore_collection
        self.project_id = project_id

    def setup(self):
        from google.cloud import firestore
        self.db = firestore.Client(project=self.project_id)

    def process(self, element):

        doc_ref = self.db.collection(self.firestore_collection).document(element['user_id']).collection('notifications').document(element['notification_id'])
        doc_ref.set(element)

        logging.info(f"Document written to Firestore: {doc_ref.id}")

        yield element


""" Code: Dataflow Process """

def run():

    """ Input Arguments """

    parser = argparse.ArgumentParser(description=('Input arguments for the Dataflow Streaming Pipeline.'))

    parser.add_argument(
                '--project_id',
                required=True,
                help='GCP cloud project name.')
    
    parser.add_argument(
                '--playback_pubsub_subscription_name',
                required=True,
                help='Pub/Sub subscription for playback events.')
    
    parser.add_argument(
                '--engagement_pubsub_subscription_name',
                required=True,
                help='Pub/Sub subscription for engagement events.')
    
    parser.add_argument(
                '--quality_pubsub_subscription_name',
                required=True,
                help='Pub/Sub subscription for quality events.')

    parser.add_argument(
                '--notifications_pubsub_topic_name',
                required=True,
                help='Pub/Sub topic for push notifications.')
    
    parser.add_argument(
                '--firestore_collection',
                required=True,
                help='Firestore collection name.')
    
    parser.add_argument(
                '--bigquery_dataset',
                required=True,
                help='BigQuery dataset name.')
    
    parser.add_argument(
                '--user_bigquery_table',
                required=True,
                help='User BigQuery table name.')
    
    parser.add_argument(
                '--episode_bigquery_table',
                required=True,
                help='Episode BigQuery table name.')
    
    args, pipeline_opts = parser.parse_known_args()

    # Pipeline Options
    options = PipelineOptions(pipeline_opts, streaming=True, project=args.project_id)
    
    setup = options.view_as(SetupOptions)
    setup.save_main_session = True
    
    # Pipeline Object
    with beam.Pipeline(argv=pipeline_opts,options=options) as p:

        playback_event = (
            p 
                | "ReadFromPlayBackPubSub" >> beam.io.ReadFromPubSub(
                    subscription=f"projects/{args.project_id}/subscriptions/{args.playback_pubsub_subscription_name}")
                | "ParsePlaybackMessages" >> beam.Map(parsePubSubMessage)
                | "NormalizePlaybackEvents" >> beam.Map(normalizePlaybackEvent)
        )

        engagement_event = (
            p
                | "ReadFromEngagementPubSub" >> beam.io.ReadFromPubSub(
                    subscription=f"projects/{args.project_id}/subscriptions/{args.engagement_pubsub_subscription_name}")
                | "ParseEngagementMessages" >> beam.Map(parsePubSubMessage)
                | "NormalizeEngagementEvents" >> beam.Map(normalizeEngagementEvent)
        )

        quality_event = (
            p
                | "ReadFromQualityPubSub" >> beam.io.ReadFromPubSub(
                    subscription=f"projects/{args.project_id}/subscriptions/{args.quality_pubsub_subscription_name}")
                | "ParseQualityMessages" >> beam.Map(parsePubSubMessage)
                | "NormalizeQualityEvents" >> beam.Map(normalizeQualityEvent)
        )

        all_events = (playback_event, engagement_event, quality_event) | "MergeEvents" >> beam.Flatten()

        # A. User real-time metrics (Session-based)
        user_data = (
            all_events
                | "WindowIntoSessions" >> beam.WindowInto(Sessions(gap_size=30))
                | "KeyByUserId" >> beam.Map(lambda x: (x["user_id"], x))
                | "GroupByUserId" >> beam.GroupByKey()
                | "ComputeUserMetrics" >> beam.ParDo(UserMetricsFn()).with_outputs(UserMetricsFn.METRICS, UserMetricsFn.NOTIFY)
        )

        (
            user_data.metrics
                | "WriteUserMetricsToBigQuery" >> beam.io.WriteToBigQuery(
                        project=args.project_id,
                        dataset=args.bigquery_dataset,
                        table=args.user_bigquery_table,
                        schema='user_id:STRING, window_start:TIMESTAMP, window_end:TIMESTAMP, plays:INTEGER, completes:INTEGER, score:FLOAT',
                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                    )
        )

        (
            user_data.notify
                | "WriteToFirestore" >> beam.ParDo(FormatFirestoreDocument(firestore_collection=args.firestore_collection, project_id=args.project_id))
        )

        (
            user_data.notify
                | "EncodeUserNotifications" >> beam.Map(lambda x: json.dumps(x).encode('utf-8'))
                | "WriteUserNotificationsToPubSub" >> beam.io.WriteToPubSub(
                    topic=f"projects/{args.project_id}/topics/{args.notifications_pubsub_topic_name}"
                )
        )

        # # B. Content real-time metrics (Sliding window-based)

        content_data = (
            all_events
                | "WindowIntoSliding" >> beam.WindowInto(SlidingWindows(size=60, period=10))
                | "KeyByEpisodeId" >> beam.Map(lambda x: (x["episode_id"], x))
                | "GroupByEpisodeId" >> beam.GroupByKey()
                | "ComputeContentMetrics" >> beam.ParDo(ContentMetricsFn()).with_outputs(ContentMetricsFn.METRICS, ContentMetricsFn.NOTIFY)
        )
          
        (content_data.metrics 
                | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                        project=args.project_id,
                        dataset=args.bigquery_dataset,
                        table=args.episode_bigquery_table,
                        schema='episode_id:STRING, window_start:TIMESTAMP, window_end:TIMESTAMP, plays:INTEGER, completes:INTEGER, score:FLOAT',
                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                    )
        )

if __name__ == '__main__':

    # Set Logs
    logging.basicConfig(level=logging.INFO)

    # Disable logs from apache_beam.utils.subprocess_server
    logging.getLogger("apache_beam.utils.subprocess_server").setLevel(logging.ERROR)

    logging.info("The process started")

    # Run Process
    run()