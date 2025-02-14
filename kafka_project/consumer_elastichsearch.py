from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json
import pandas as pd
from dotenv import load_dotenv
import os


KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user_events")
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:29092")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")
ES_HOST = os.getenv("ES_HOST")
ES = os.getenv("ES")


KAFKA_TOPIC = os.getenv

# Kết nối Elasticsearch
es = Elasticsearch(ES_HOST)

# Tạo Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

def send_to_elasticsearch(event):
    """
    Gửi dữ liệu từ Kafka vào Elasticsearch.
    """
    es.index(index="user_events", document=event)
    

def calculate_conversion_rate(events):
    """
    Tính tỷ lệ giữa các sự kiện 'purchase' so với 'add_to_cart' hoặc 'remove_from_cart'.
    
    Args:
        events (list): Danh sách các sự kiện người dùng (dạng list của dictionary).
    
    Returns:
        dict: Tỷ lệ chuyển đổi giữa các sự kiện.
    """
    df = pd.DataFrame(events)
    
    # Đếm số lần xảy ra của mỗi loại sự kiện
    event_counts = df['event_type'].value_counts()
    
    add_to_cart = event_counts.get("add_to_cart", 0)
    remove_from_cart = event_counts.get("remove_from_cart", 0)
    purchase = event_counts.get("purchase", 0)
    
    # Tính tỷ lệ chuyển đổi
    conversion_rate = {
        "add_to_cart_to_purchase_rate": round((purchase / add_to_cart) * 100, 2) if add_to_cart > 0 else 0,
        "remove_from_cart_to_purchase_rate": round((purchase / remove_from_cart) * 100, 2) if remove_from_cart > 0 else 0,
        "total_purchase_ratio": round((purchase / (add_to_cart + remove_from_cart)) * 100, 2) if (add_to_cart + remove_from_cart) > 0 else 0
    }
    
    return conversion_rate





# Lắng nghe dữ liệu từ Kafka và gửi vào Elasticsearch
# for message in consumer:
#     event = message.value
#     conversion_rate = calculate_conversion_rate(event)
#     send_to_elasticsearch(conversion_rate)
#     print("send dataa success to ES")
for message in consumer:
    event = message.value
    
    for single_customer_behavier in range(0,len(event)):
        send_to_elasticsearch(event[single_customer_behavier],"user_events_raw")
