import pandas as pd
import json
from kafka import KafkaProducer
import os


def run_producer(file_path):
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found.")
        return

    df = pd.read_csv(file_path)
    topic_name = 'bronze_raw'

    print(f"Starting streaming of {len(df)} records to Kafka...")
    for _, row in df.iterrows():
        producer.send(topic_name, value=row.to_dict())

    producer.flush()
    print(f"Successfully pushed data to topic: {topic_name}")