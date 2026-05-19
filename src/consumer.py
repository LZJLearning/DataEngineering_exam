import json
import logging
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting test consumer for topic: sensor-events")
    logger.info("Press Ctrl+C to exit.")

    try:
        consumer = KafkaConsumer(
            'sensor-events',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='debug-cli-consumer',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

        for message in consumer:
            key = message.key.decode('utf-8') if message.key else "None"
            print(f" Key: {key:<12} |  Value: {message.value}")
            
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Consumer error: {e}")
    finally:
        logger.info("Closing consumer connection.")
        consumer.close()

if __name__ == "__main__":
    main()