import json
import time
import random
from kafka import KafkaProducer

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'db_change_log'

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Starting CDC producer simulator for topic: {KAFKA_TOPIC}")

mock_data_changes = [
    {"op": "c", "after": {"id": 101, "score": 50.0, "status": "Active", "timestamp": int(time.time())}},
    {"op": "u", "before": {"id": 102, "score": 85.5, "status": "Active", "timestamp": int(time.time() - 3600)}, "after": {"id": 102, "score": 90.1, "status": "Active", "timestamp": int(time.time() + 1)}},
    {"op": "d", "before": {"id": 103, "score": 45.0, "status": "Inactive", "timestamp": int(time.time() - 7200)}},
    {"op": "u", "before": {"id": 101, "score": 50.0, "status": "Active", "timestamp": int(time.time())}, "after": {"id": 101, "score": 50.0, "status": "Suspended", "timestamp": int(time.time() + 2)}},
    {"op": "c", "after": {"id": 104, "score": 77.0, "status": "Active", "timestamp": int(time.time() + 3)}}
]

try:
    for change in mock_data_changes:
        producer.send(KAFKA_TOPIC, value=change)
        print(f"Sent CDC record (Op: {change['op']})")
        time.sleep(1) 
        
    print("Finished sending simulated CDC records.")
except KeyboardInterrupt:
    print("\nStopping producer...")
    producer.close()