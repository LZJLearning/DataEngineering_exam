import json
import time
import random
import argparse
import logging
import sys
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Log output format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Sensor configuration: type, unit, normal range, abnormal range
SENSORS = {
    "temperature": {"unit": "C", "normal": (15.0, 35.0), "anomaly": (35.1, 45.0)},
    "humidity": {"unit": "%", "normal": (30.0, 90.0), "anomaly": (90.1, 95.0)},
    "pressure": {"unit": "hPa", "normal": (990.0, 1030.0), "anomaly_low": (980.0, 989.9), "anomaly_high": (1030.1, 1040.0)}
}

def generate_reading(source: str, force_anomaly: bool = False) -> dict:
    """Generate sensor data that conforms to the JSON Schema convention."""
    sensor_type = random.choice(list(SENSORS.keys()))
    config = SENSORS[sensor_type]
    
    is_anomaly = force_anomaly
    
    # Data is generated according to normal or abnormal ranges.
    if sensor_type == "pressure":
        if is_anomaly:
            value = random.uniform(*config["anomaly_low"]) if random.random() < 0.5 else random.uniform(*config["anomaly_high"])
        else:
            value = random.uniform(*config["normal"])
    else:
        if is_anomaly:
            value = random.uniform(*config["anomaly"])
        else:
            value = random.uniform(*config["normal"])
            
    return {
        "sensor": sensor_type,
        "value": round(value, 2),
        "unit": config["unit"],
        "timestamp": int(time.time() * 1000), 
        "source": source,
        "anomaly": is_anomaly 
    }

def main():
    # 1. Receive command-line arguments
    parser = argparse.ArgumentParser(description="IoT Sensor Data Producer")
    parser.add_argument("--count", type=int, default=200, help="Number of events to produce")
    parser.add_argument("--rate", type=float, default=10.0, help="Events per second")
    parser.add_argument("--source", type=str, default="site-A-rack-12", help="Source identifier")
    args = parser.parse_args()

    logger.info(f"Starting producer: count={args.count}, rate={args.rate}/s, source={args.source}")

    # 2. Kafka producer configuration
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            acks='all', 
            retries=5,  
            max_in_flight_requests_per_connection=1,
            linger_ms=5, 
            batch_size=16384, # 16KB 
            key_serializer=lambda k: k.encode('utf-8'), 
            value_serializer=lambda v: json.dumps(v).encode('utf-8') 
        )
    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        sys.exit(1)

    topic = "sensor-events"
    success_count = 0

    # 3. Generate and send data
    try:
        for i in range(args.count):
            force_anomaly = True if random.random() <= 0.15 else False
            reading = generate_reading(args.source, force_anomaly)
            
            producer.send(
                topic,
                key=reading["sensor"],
                value=reading
            )
            
            success_count += 1
            if success_count % 50 == 0:
                logger.info(f"Published {success_count}/{args.count} events...")
                
            time.sleep(1.0 / args.rate) 
            
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except KafkaError as ke:
        logger.error(f"Kafka error occurred: {ke}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info("Flushing remaining messages to Kafka...")
        producer.flush()
        producer.close()
        logger.info(f"Producer shutdown safely. Total published: {success_count}")

if __name__ == "__main__":
    main()