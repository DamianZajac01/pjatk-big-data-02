{{ config(materialized='table') }}

-- To zapytanie dbt wygeneruje tabelę Gold w Twojej bazie danych (np. DuckDB)
SELECT
    Borough,
    data_month,
    ROUND(AVG(tip_amount), 2) as avg_tip,
    COUNT(*) as total_trips
FROM read_parquet('data/silver/taxi_table/*/*.parquet')
GROUP BY Borough, data_month
ORDER BY avg_tip DESC