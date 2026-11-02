import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaConsumer
import boto3
from botocore.exceptions import ClientError


load_dotenv()

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s'
)
logger = logging.getLogger("ComputicketS3Consumer")


KAFKA_BROKER = os.getenv('KAFKA_BROKER', '32.195.69.185:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'Ticket-DataPipeline-Topic')
CONSUMER_GROUP = os.getenv('CONSUMER_GROUP', 'computicket-s3-consumer-group')

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
BUCKET_NAME = os.getenv('BUCKET_NAME', 'computickets-ticketing-data')

# Credentials validation check
if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
    logger.error("AWS credentials not found. Make sure .env file exists and variables are set.")
    exit(1)

# Initialize S3 Client
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# Initialize Kafka Consumer
logger.info(f"Connecting to Kafka Broker ({KAFKA_BROKER}) on topic '{KAFKA_TOPIC}'...")

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=CONSUMER_GROUP
)

logger.info("Consumer active. Listening for ticket streaming events...")

def upload_event_to_s3(payload, s3_key):
    """Uploads single JSON ticket event to S3"""
    try:
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(payload, indent=2),
            ContentType='application/json'
        )
        logger.info(f"[S3 SINK SUCCESS] Uploaded: {s3_key}")
        return True
    except ClientError as e:
        logger.error(f"[S3 UPLOAD FAILED] {s3_key}: {e}")
        return False

# Main Consumption Loop
try:
    for index, record in enumerate(consumer):
        ticket_event = record.value
        
        # Partitioned Path: Glue Crawler aur Athena queries ke liye structured format
        now = datetime.now()
        partition_date = now.strftime("%Y-%m-%d")
        timestamp_str = now.strftime("%Y%m%d_%H%M%S_%f")
        
        service_no = ticket_event.get('serviceNumber', 'UNKNOWN')
        s3_key = f"computicket_raw/date={partition_date}/ticket_{service_no}_{timestamp_str}_{index}.json"
        
        upload_event_to_s3(ticket_event, s3_key)

except KeyboardInterrupt:
    logger.info("Consumer stopped manually.")
except Exception as e:
    logger.error(f"Consumer runtime error: {e}")
finally:
    consumer.close()
    logger.info("Kafka Consumer closed.")