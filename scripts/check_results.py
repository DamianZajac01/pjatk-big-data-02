import duckdb

con = duckdb.connect('../data/taxi_db.duckdb')

print("--- TOP 5 BOROUGHS WITH THE HIGHEST AVERAGE TIPS ---")

report = con.execute("""
    SELECT 
        Borough, 
        ROUND(AVG(avg_tip), 2) as final_avg_tip,
        SUM(total_trips) as total_trips
    FROM gold_tips
    WHERE Borough IS NOT NULL
    GROUP BY Borough
    ORDER BY final_avg_tip DESC
    LIMIT 5
""").df()

print(report)

con.close()