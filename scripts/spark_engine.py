import sys
import duckdb
import os


def run_job(mode, month=None):
    con = duckdb.connect()

    if mode == "full":
        input_path = "data/bronze/yellow_tripdata_*.csv"
    else:
        input_path = f"data/bronze/yellow_tripdata_{month}.csv"

    zone_lookup_path = "data/bronze/taxi+_zone_lookup.csv"
    output_folder = "data/silver/taxi_table"

    print(f"--- STARTING JOB: mode={mode}, month={month} ---")

    os.makedirs(output_folder, exist_ok=True)

    query = f"""
        COPY (
            SELECT 
                t.*,
                z.Borough,
                z.Zone,
                strftime(CAST(t.tpep_pickup_datetime AS TIMESTAMP), '%Y-%m') as data_month
            FROM read_csv_auto('{input_path}') t
            LEFT JOIN read_csv_auto('{zone_lookup_path}') z 
                ON t.PULocationID = z.LocationID
            WHERE t.total_amount > 0 
              AND t.trip_distance > 0
        ) TO '{output_folder}' (FORMAT PARQUET, PARTITION_BY (data_month), OVERWRITE_OR_IGNORE 1);
    """

    try:
        con.execute(query)
        print("--- JOB FINISHED SUCCESSFULLY ---")
    except Exception as e:
        print(f"--- ERROR: {str(e)} ---")
        sys.exit(1)


if __name__ == "__main__":
    run_job(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None)