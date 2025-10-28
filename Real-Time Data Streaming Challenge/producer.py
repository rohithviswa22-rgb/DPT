import json
import time
import random
from kafka import KafkaProducer

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'sensor_data_stream'

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Starting producer for topic: {KAFKA_TOPIC}")

def generate_sensor_data(sensor_id):
    data = {
        'sensor_id': sensor_id,
        'timestamp': int(time.time()),
        'temperature': round(random.uniform(20.0, 35.0), 2),
        'humidity': round(random.uniform(40.0, 70.0), 2),
        'pressure': round(random.uniform(900, 1100), 2),
    }
    data['is_critical'] = 1 if data['temperature'] > 30 and data['humidity'] > 60 else 0
    return data

try:
    sensor_ids = ['S101', 'S102', 'S103', 'S104']
    
    while True:
        for sensor_id in sensor_ids:
            message = generate_sensor_data(sensor_id)
            
            producer.send(KAFKA_TOPIC, value=message)
            print(f"Sent: {message}")
            
        time.sleep(random.uniform(0.1, 0.5))
        
except KeyboardInterrupt:
    print("\nStopping producer...")
    producer.close()