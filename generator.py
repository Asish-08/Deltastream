from confluent_kafka import Producer
import json
import time
from faker import Faker
import random
import uuid
from datetime import datetime, timedelta

fake = Faker()
producer = Producer({'bootstrap.servers': 'localhost:19092'}) # data generator will run on local machine.
topic_name= 'user_activity_stream'

# Sample product catalog for generating purchase events
PRODUCT_CATALOG = [
    {"product_id": "prod_001", "category": "electronics", "price": 499.99},
    {"product_id": "prod_002", "category": "electronics", "price": 129.99},
    {"product_id": "prod_003", "category": "fashion", "price": 59.99},
    {"product_id": "prod_004", "category": "fashion", "price": 89.99},
    {"product_id": "prod_005", "category": "books", "price": 19.99},
    {"product_id": "prod_006", "category": "books", "price": 29.99},
    {"product_id": "prod_007", "category": "home", "price": 79.99},
    {"product_id": "prod_008", "category": "home", "price": 149.99},
    {"product_id": "prod_009", "category": "sports", "price": 39.99},
    {"product_id": "prod_010", "category": "sports", "price": 119.99},
]
DEVICES = ["mobile", "desktop", "tablet"]
COUNTRIES = ["US", "India", "UK", "Canada", "Germany"]
TRAFFIC_SOURCES = ["organic", "ads", "email", "social", "direct"]

# Function to handle delivery reports from Kafka producer
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")

# Function to generate a random user activity event
def generate_session():
    session_id = str(uuid.uuid4())
    user_id= random.randint(1, 10000)
    device = random.choice(DEVICES)
    country = random.choice(COUNTRIES)
    traffic_source = random.choice(TRAFFIC_SOURCES)
    session_start_time = datetime.utcnow()
    # event_type = random.choices(
    #     ["page_view", "click", "purchase"],
    #     weights=[0.7, 0.25, 0.05]
    # )[0]
    return {
        # "event_id": str(uuid.uuid4()),
        # "user_id": random.randint(1, 10000),
        # "event_type": event_type,
        # "product_id": fake.ean(),
        # "timestamp": datetime.utcnow().isoformat()
        "session_id": session_id,
        "user_id": user_id,
        "device": device,
        "country": country,
        "traffic_source": traffic_source,
        "session_start_time": session_start_time
    }

# Function to generate a sequence of events for a user session
def generate_event_sequence():
    sequence = ["page_view"]

    if random.random() < 0.7:
        sequence.append("click")

    if "click" in sequence and random.random() < 0.35:
        sequence.append("add_to_cart")

    if "add_to_cart" in sequence and random.random() < 0.25:
        sequence.append("purchase")

    return sequence

# Function to build an event based on the event type and product details
def build_event(event_type, session, product, event_time):
    event = {
        "event_id": str(uuid.uuid4()),
        "session_id": session["session_id"],
        "user_id": session["user_id"],
        "event_type": event_type,
        "product_id": product["product_id"],
        "category": product["category"],
        "price": product["price"] if event_type == "purchase" else 0.0,
        "device": session["device"],
        "country": session["country"],
        "traffic_source": session["traffic_source"],
        "event_timestamp": event_time.isoformat()
    }
    return event

def generate_session_events():
    session=generate_session()
    product = random.choice(PRODUCT_CATALOG)
    event_sequence = generate_event_sequence()
    events = []
    current_time = session["session_start_time"]
    for event_type in event_sequence:
        event=build_event(event_type,session, product, current_time)
        events.append(event)
        # advance time inside session
        current_time += timedelta(seconds=random.randint(2, 10))
    return events

    
def send_event(event):
    # event = {
    #     "user_id": 1,
    #     "event_type": "page_view"
    # } prototype event
    # event = generate_event()
    producer.produce(topic_name, json.dumps(event), callback=delivery_report)
    # producer.flush() # removing this for optimization
    producer.poll(0) # trigger delivery report callbacks
try:    
    while True:
        session_events=generate_session_events()
        for event in session_events:
            send_event(event)
            print(event)
            time.sleep(random.uniform(0.1, 0.5))
except KeyboardInterrupt:
    print("Stopping event generation.")
finally:
    producer.flush()