from dagster import asset, Definitions, Config, AssetExecutionContext
import os
from scripts.kafka.producer import run_producer
from scripts.kafka.consumer import run_consumer

class TaxiConfig(Config):
    # 'batch' or 'streaming'
    mode: str = "batch"
    month: str = "2020-01"
    new_data_file: str = "data/raw/yellow_tripdata_new.csv"

@asset
def check_bronze_files():
    required_files = ["taxi+_zone_lookup.csv"]
    for f in required_files:
        if not os.path.exists(f"data/bronze/{f}"):
            raise Exception(f"Missing critical file: {f}")
    return "Bronze Files Validated"

# --- DECOUPLING LAYER ---

@asset(deps=[check_bronze_files])
def kafka_streaming_ingest(context: AssetExecutionContext, config: TaxiConfig):
    """
    PRODUCER: Reads from source and pushes to Kafka queue.
    Used for incremental/fresh data only.
    """
    if config.mode == "streaming":
        context.log.info(f"Starting streaming ingestion for {config.new_data_file}")
        run_producer(config.new_data_file)
        return "Data Pushed to Kafka"
    return "Streaming skipped (Batch mode active)"

@asset(deps=[kafka_streaming_ingest])
def bronze_automated_injection(context: AssetExecutionContext, config: TaxiConfig):
    """
    CONSUMER: Pulls from Kafka and injects into Bronze layer.
    """
    if config.mode == "streaming":
        context.log.info("Starting automated injection from Kafka to Bronze...")
        run_consumer()
        return "Bronze Layer Updated via Streaming"
    return "Automated injection skipped"

# --- BATCH & TRANSFORMATIONS ---

@asset(deps=[check_bronze_files, bronze_automated_injection])
def silver_taxi_data(context: AssetExecutionContext, config: TaxiConfig):
    """
    Processes data from Bronze to Silver.
    Can run as a large batch (history) or incremental update.
    """
    cmd = f"python scripts/spark_engine.py {config.mode} {config.month}"
    exit_code = os.system(cmd)
    if exit_code != 0:
        raise Exception("Spark/DuckDB Job failed!")
    return "Silver Parquet Table Updated"

@asset(deps=[silver_taxi_data])
def gold_analysis_report():
    print("Gold Analysis Finished. Results ready in data/silver/taxi_table.")
    return "Gold Reports Ready"

defs = Definitions(
    assets=[
        check_bronze_files,
        kafka_streaming_ingest,
        bronze_automated_injection,
        silver_taxi_data,
        gold_analysis_report
    ]
)