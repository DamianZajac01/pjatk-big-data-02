import json
import duckdb
from kafka import KafkaConsumer
import os


def run_consumer(db_path='data/taxi_pipeline.db'):
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    db_path = os.path.join(base_dir, 'data', 'taxi_pipeline.db')

    # Upewnij się, że folder data istnieje w głównym katalogu
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    print(f"Connecting to database at: {db_path}")
    con = duckdb.connect(db_path)

    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze_taxi_data (
            VendorID INTEGER,
            tpep_pickup_datetime TIMESTAMP,
            tpep_dropoff_datetime TIMESTAMP,
            passenger_count DOUBLE,
            trip_distance DOUBLE,
            RatecodeID DOUBLE,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            payment_type INTEGER,
            fare_amount DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE
        )
    """)

    consumer = KafkaConsumer(
        'bronze_raw',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Consumer started. Waiting for streaming data...")

    try:
        for message in consumer:
            data = message.value

            con.execute("""
                INSERT INTO bronze_taxi_data 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                data.get('VendorID'), data.get('tpep_pickup_datetime'),
                data.get('tpep_dropoff_datetime'), data.get('passenger_count'),
                data.get('trip_distance'), data.get('RatecodeID'),
                data.get('PULocationID'), data.get('DOLocationID'),
                data.get('payment_type'), data.get('fare_amount'),
                data.get('tip_amount'), data.get('total_amount')
            ])
            print(f"Injected record from Vendor {data.get('VendorID')} to Bronze Layer.")
    except KeyboardInterrupt:
        print("Consumer stopped by user.")
    finally:
        con.close()


if __name__ == "__main__":
    run_consumer()