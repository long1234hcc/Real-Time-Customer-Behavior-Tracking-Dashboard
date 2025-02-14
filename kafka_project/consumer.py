
from kafka import KafkaConsumer
import json
import pandas as pd
from dotenv import load_dotenv
import os


load_dotenv()


KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_SERVER = os.getenv("KAFKA_SERVER")



# Khởi tạo Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

events = []

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


def product_conversion_rate(events):
    """
    Tính tỷ lệ chuyển đổi của từng sản phẩm từ 'add_to_cart' → 'purchase'.
    
    Args:
        events (list): Danh sách các sự kiện người dùng.
    
    Returns:
        dict: Dictionary chứa tỷ lệ chuyển đổi của từng sản phẩm.
    """
    df = pd.DataFrame(events)

    add_to_cart_df = df[df["event_type"] == "add_to_cart"]
    purchase_df = df[df["event_type"] == "purchase"]

    add_to_cart_counts = add_to_cart_df["metadata"].apply(lambda x: x.get("product", {}).get("id") if x.get("product") else None).value_counts()
    purchase_counts = purchase_df["metadata"].apply(lambda x: x.get("product", {}).get("id") if x.get("product") else None).value_counts()
    
    product_dict = {}

    for product in set(add_to_cart_counts.index).union(set(purchase_counts.index)):
        add_count = add_to_cart_counts.get(product, 0)
        purchase_count = purchase_counts.get(product, 0)
        conversion_rate = round((purchase_count / add_count) * 100, 2) if add_count > 0 else 0

        product_dict[product] = {
            "add_to_cart": int(add_count),
            "purchase": int(purchase_count),
            "conversion_rate (%)": conversion_rate
        }
    
    return product_dict



def total_revenue_per_product(events):
    """
    Tính tổng doanh thu theo sản phẩm.

    Args:
        events (list): Danh sách các sự kiện người dùng.

    Returns:
        dict: Dictionary chứa tổng doanh thu của từng sản phẩm.
    """
    df = pd.DataFrame(events)

    purchase_df = df[df["event_type"] == "purchase"]

    revenue_data = (
        purchase_df["metadata"]
        .apply(lambda x: (x["product"]["id"], x["total_price"]) if "product" in x and "total_price" in x else None)
        .dropna()
        .tolist()
    )

    revenue_df = pd.DataFrame(revenue_data, columns=["product_id", "total_revenue"])
    revenue_summary = revenue_df.groupby("product_id").sum().to_dict()["total_revenue"]

    return {key: int(value) for key, value in revenue_summary.items()}


def analyze_search_terms(events):
    """
    Phân tích từ khóa tìm kiếm phổ biến và nhóm chúng theo danh mục.

    Args:
        events (list): Danh sách các sự kiện người dùng.

    Returns:
        dict: Từ khóa tìm kiếm phổ biến theo danh mục.
    """
    df = pd.DataFrame(events)

    # Lọc các sự kiện tìm kiếm
    search_terms = df[df["event_type"] == "search"]["metadata"].apply(lambda x: x.get("search_keyword") if "search_keyword" in x else None).dropna()

    # Nhóm từ khóa vào các danh mục
    categories = {
        "smartphones": ["iphone", "samsung", "phone", "mobile"],
        "laptops": ["laptop", "macbook", "gaming laptop"],
        "headphones": ["headphones", "earbuds", "wireless headphones"],
        "tablets": ["tablet", "ipad"],
        "smartwatches": ["smartwatch", "watch"]
    }

    category_count = {category: 0 for category in categories.keys()}
    keyword_count = {}

    for term in search_terms:
        keyword_count[term] = keyword_count.get(term, 0) + 1
        for category, keywords in categories.items():
            if any(keyword in term.lower() for keyword in keywords):
                category_count[category] += 1

    result = {
        "top_search_keywords": dict(sorted(keyword_count.items(), key=lambda item: item[1], reverse=True)[:10]),
        "category_search_distribution": category_count
    }
    
    return result





# Lắng nghe dữ liệu từ Kafka
for message in consumer:
    event = message.value
    conversion_rate = calculate_conversion_rate(event)
    production_conversion_rate = product_conversion_rate(event)
    revenue_per_product = total_revenue_per_product(event)
    search_terms = analyze_search_terms(event)
    print("-------------------conversion_rate-------------------")
    print(conversion_rate)
    print("-------------------product_conversion_rate-------------------")
    print(production_conversion_rate)
    print("-------------------revenue_per_product-------------------")
    print(revenue_per_product)
    print("-------------------revenue_per_product-------------------")
    print(search_terms)
    print("-------------------search_terms-------------------")
