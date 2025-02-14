
from kafka import KafkaProducer
import requests
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# API_URL = "http://localhost:8000/generate-event"
API_URL = "http://localhost:8000/generate-event?count=10"

while True:
    response = requests.get(API_URL)
    event = response.json()
    producer.send("user_events", event)
    print(f"Sent: {event}")
    time.sleep(20)
