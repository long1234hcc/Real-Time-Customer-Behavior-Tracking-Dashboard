from fastapi import FastAPI, Query
from pydantic import BaseModel
import random
import uvicorn
from faker import Faker
from typing import List, Optional, Dict
from collections import Counter
import logging
from datetime import datetime
from datetime import datetime, timedelta


# Khởi tạo ứng dụng FastAPI
app = FastAPI()

# Khởi tạo Faker
fake = Faker()

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Định nghĩa loại sự kiện
EVENT_TYPES = [
    "page_view", "click", "add_to_cart", "remove_from_cart",
    "purchase", "search", "session_time"
]

# Danh sách sản phẩm
PRODUCTS = [
    {"id": "p1", "name": "iPhone 15", "price": 999},
    {"id": "p2", "name": "Samsung Galaxy S23", "price": 899},
    {"id": "p3", "name": "MacBook Pro 16", "price": 2499},
    {"id": "p4", "name": "iPad Air", "price": 699},
    {"id": "p5", "name": "Sony WH-1000XM5", "price": 399}
]

# Danh sách tìm kiếm giả lập
SEARCH_TERMS = [
    "iphone 15 pro max", "best wireless headphones", "gaming laptop",
    "cheap tablets", "smartwatch 2024", "macbook air m3"
]

# Danh sách trang web
PAGE_URLS = [
    "/home", "/product/iphone-15", "/product/macbook-pro", "/cart", "/checkout",
    "/category/smartphones", "/category/laptops", "/category/accessories"
]

# Danh sách thiết bị và trình duyệt
DEVICES = {
    "mobile": ["Chrome Mobile", "Safari Mobile", "Samsung Internet"],
    "desktop": ["Chrome", "Firefox", "Edge", "Safari"],
    "tablet": ["Safari Mobile", "Chrome Mobile"]
}

# Danh sách địa điểm
LOCATIONS = [
    {"country": "USA", "city": "New York"},
    {"country": "UK", "city": "London"},
    {"country": "Germany", "city": "Berlin"},
    {"country": "Japan", "city": "Tokyo"},
    {"country": "India", "city": "Mumbai"}
]

# Định nghĩa Schema với Pydantic
class ProductModel(BaseModel):
    id: str
    name: str
    price: int

class LocationModel(BaseModel):
    country: str
    city: str

class MetadataModel(BaseModel):
    page_url: str
    device: str
    browser: str
    location: LocationModel
    session_duration: Optional[int] = None
    product: Optional[ProductModel] = None
    quantity: Optional[int] = None
    total_price: Optional[int] = None
    search_keyword: Optional[str] = None

class EventModel(BaseModel):
    user_id: str
    event_type: str
    timestamp: str
    metadata: MetadataModel

# Hàm tạo sự kiện giả lập
def generate_event() -> EventModel:
    event_type = random.choice(EVENT_TYPES)
    device_type = random.choice(list(DEVICES.keys()))
    browser = random.choice(DEVICES[device_type])
    location = random.choice(LOCATIONS)

    # Tạo timestamp trong khoảng từ năm 2020 đến 2025
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2025, 12, 31)
    random_days = random.randint(0, (end_date - start_date).days)
    event_time = start_date + timedelta(days=random_days, seconds=random.randint(0, 86400))

    event_data = {
        "user_id": fake.uuid4(),
        "event_type": event_type,
        "timestamp": event_time.isoformat(),
        "metadata": {
            "page_url": random.choice(PAGE_URLS),
            "device": device_type,
            "browser": browser,
            "location": location
        }
    }

    if event_type == "session_time":
        event_data["metadata"]["session_duration"] = random.randint(5, 1800)
    elif event_type == "purchase":
        product = random.choice(PRODUCTS)
        quantity = random.randint(1, 3)
        event_data["metadata"].update({
            "product": product,
            "quantity": quantity,
            "total_price": product["price"] * quantity
        })
    elif event_type in ["add_to_cart", "remove_from_cart"]:
        event_data["metadata"]["product"] = random.choice(PRODUCTS)
    elif event_type == "search":
        event_data["metadata"]["search_keyword"] = random.choice(SEARCH_TERMS)

    return EventModel(**event_data)

# Endpoint tạo sự kiện
@app.get("/generate-event", response_model=List[EventModel])
async def generate_events(count: int = Query(1, ge=1, le=1000)):  # Cho phép count lên tới 1000
    """
    Tạo danh sách các sự kiện giả lập.
    
    - `count`: Số lượng sự kiện muốn lấy (1 - 1000).
    """
    events = [generate_event() for _ in range(count)]
    logger.info(f"Generated {count} events")
    return events

@app.get("/health")
async def health_check():
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
