from confluent_kafka import Consumer
import json
import logging
from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_GROUP_ID

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': KAFKA_GROUP_ID,
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['session-events'])

def process_message(message_data):
    action = message_data.get('action')
    username = message_data.get('username')

    if action == 'login':
        logger.info(f"User {username} logged in")
    elif action == 'verify':
        logger.info(f"User {username} verified token")
    elif action == 'logout':
        logger.info(f"User {username} logged out")
    else:
        logger.info(f"Unknown action: {action} for user {username}")

if __name__ == "__main__":
    logger.info("Starting Kafka Consumer...")
    logger.info(f"Kafka servers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Group ID: {KAFKA_GROUP_ID}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue

            try:
                data = json.loads(msg.value())
                process_message(data)
                logger.info(f"Processed message: {data}")
            except json.JSONDecodeError:
                logger.error("Failed to parse JSON message")

    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    finally:
        consumer.close()
