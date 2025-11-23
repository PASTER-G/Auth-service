from confluent_kafka import Producer
import json
from src.config import KAFKA_BOOTSTRAP_SERVERS

conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')

def produce_session_event(action: str, username: str, token: str = None):
    message = {
        "action": action,
        "username": username,
        "token": token
    }
    producer.produce(
        'session-events',
        key=username,
        value=json.dumps(message),
        callback=delivery_report
    )
    producer.flush()
