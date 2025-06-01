from kafka import KafkaConsumer
import json
import csv
import os

TOPIC = 'crime-topic'
BATCH_SIZE = 100
BATCH_FOLDER = '/home/iryandae/kafka_2.13-3.7.0/project/data/batch/'

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers='localhost:9092',
    group_id='crime-consumer-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

batch_data = []
batch_number = 1
fieldnames = None

for message in consumer:
    data = message.value
    print(f"📥 Received: {data}")

    if not fieldnames:
        fieldnames = list(data.keys())

    batch_data.append(data)
    print(f"📊 Current batch size: {len(batch_data)} / {BATCH_SIZE}")

    if len(batch_data) >= BATCH_SIZE:
        filename = f'{BATCH_FOLDER}/batch_{batch_number}.csv'
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(batch_data)

            print(f'✅ Saved {filename} with {len(batch_data)} rows')
            batch_data = []
            batch_number += 1

        except Exception as e:
            print(f'❌ Error saving batch: {e}')
