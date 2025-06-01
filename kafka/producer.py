from kafka import KafkaProducer
import csv
import time
import json
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = 'crime-topic'
FILENAME = '/home/iryandae/kafka_2.13-3.7.0/project/data/raw/Crime_Data_from_2020_to_Present.csv'

with open(FILENAME) as file:
    reader = csv.DictReader(file)
    for row in reader:
        producer.send(TOPIC, row)
        print(f"Sent: {row}")
        time.sleep(random.uniform(0.5, 1.5))
