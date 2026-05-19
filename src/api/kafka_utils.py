import json
import logging
from kafka import KafkaProducer, KafkaConsumer

logger = logging.getLogger(__name__)
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'sensor-events'

def get_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        key_serializer=lambda k: k.encode('utf-8'),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3
    )

def publish_reading(sensor_type: str, payload: dict) -> bool:
    """Publish a new reading to Kafka"""
    try:
        producer = get_producer()
        producer.send(TOPIC, key=sensor_type, value=payload)
        producer.flush()
        producer.close()
        return True
    except Exception as e:
        logger.error(f"Failed to publish to Kafka: {e}")
        return False

def get_latest_reading_from_kafka(sensor_type: str) -> dict:
    """Fetch the latest data from Kafka"""
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='latest',
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=2000 
    )
    
    latest_msg = None
    try:
        # Retrieve the most recent small batch of data
        records = consumer.poll(timeout_ms=1000)
        for tp, messages in records.items():
            for msg in messages:
                if msg.key and msg.key.decode('utf-8') == sensor_type:
                    latest_msg = msg.value
    finally:
        consumer.close()
        
    return latest_msg