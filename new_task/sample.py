from kafka import KafkaConsumer

import json


consumer = KafkaConsumer(
    'OCR',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)   


try:
    for message in consumer:
        data = message.value
        print("data recieved")
        
except KeyboardInterrupt:
    print("Process interrupted by user.")