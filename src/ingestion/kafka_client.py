import json
import os
from kafka import KafkaProducer
from src.utils.logger import get_logger

logger = get_logger(__name__)

class CryptoKafkaPublisher:
    """
    A decoupled client responsible solely for publishing messages to Kafka.
    """
    def __init__(self):
        broker = os.getenv('KAFKA_BROKER')
        self.topic = os.getenv('KAFKA_TOPIC')

        if not broker or not self.topic:
            raise RuntimeError("Missing KAFKA_BROKER or KAFKA_TOPIC environment variables.")

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info(f"Connected to Kafka broker at {broker}")
        except Exception as e:
            raise RuntimeError(f"Failed to connect to Kafka broker at {broker}: {e}") from e

    def publish(self, data: dict):
        """Sends a dictionary payload to the configured Kafka topic."""
        try:
            self.producer.send(self.topic, data)
        except Exception as e:
            logger.error(f"Error publishing to Kafka: {e}")

    def close(self):
        """Flushes remaining messages and closes the connection."""
        self.producer.flush()
        self.producer.close()