from dagster import asset, Definitions, Config
import os

class TaxiConfig(Config):
    mode: str = "incremental"
    month: str = "2020-01"

@asset
def check_bronze_files():
    required_files = ["taxi+_zone_lookup.csv"]
    for f in required_files:
        if not os.path.exists(f"data/bronze/{f}"):
            raise Exception(f"Missing critical file: {f}")
    return "Bronze Files Validated"

@asset(deps=[check_bronze_files])
def silver_taxi_data(config: TaxiConfig):
    cmd = f"python scripts/spark_engine.py {config.mode} {config.month}"
    exit_code = os.system(cmd)
    if exit_code != 0:
        raise Exception("Spark Job failed!")
    return "Silver Parquet Table Created"

@asset(deps=[silver_taxi_data])
def gold_analysis_report():
    print("Gold Analysis Finished. Results ready in data/silver/taxi_table.")
    return "Gold Reports Ready"

defs = Definitions(assets=[check_bronze_files, silver_taxi_data, gold_analysis_report])