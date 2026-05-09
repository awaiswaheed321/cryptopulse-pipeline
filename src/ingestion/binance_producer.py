import os
import json
import websocket
from dotenv import load_dotenv

from src.utils.logger import get_logger
from src.ingestion.kafka_client import CryptoKafkaPublisher

load_dotenv()
logger = get_logger(__name__)

BINANCE_WS_URL = os.getenv('BINANCE_WS_URL')

# Instantiate our decoupled Kafka client
publisher = CryptoKafkaPublisher()

def on_message(ws, message):
    """Triggered every time a new trade arrives from Binance."""
    try:
        raw_payload = json.loads(message)
        
        #UNWRAP THE COMBINED STREAM
        # If the payload has a 'data' wrapper (Combined Stream), extract the inner dictionary.
        # If it doesn't (Single Stream), just use the payload as-is.
        trade_data = raw_payload.get("data", raw_payload)

        # Hand the data off to the Kafka client
        publisher.publish(trade_data)
        
        logger.info(f"Sent {trade_data.get('s')} to Kafka: Price={trade_data.get('p')}, Quantity={trade_data.get('q')}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def on_error(ws, error):
    logger.error(f"WebSocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    logger.info("WebSocket connection closed.")
    publisher.close() # Safely shut down the Kafka connection

def on_open(ws):
    logger.info(f"Connected to Binance WebSocket: {BINANCE_WS_URL}")
    logger.info("Streaming live trades to Kafka...")

if __name__ == "__main__":
    if not BINANCE_WS_URL:
        logger.error("CRITICAL: Missing BINANCE_WS_URL in .env file.")
        exit(1)

    # Create the persistent WebSocket connection
    ws = websocket.WebSocketApp(
        BINANCE_WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    
    ws.run_forever()