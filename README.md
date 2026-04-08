# NYC Taxi Pipeline - Data Orchestration Project

## Architektura Potoku (Medallion Architecture)

```mermaid
graph LR
    subgraph Bronze_Layer
        A[yellow_tripdata_*.csv]
        B[taxi+_zone_lookup.csv]
    end

    subgraph Silver_Layer
        C[PySpark: Join & Clean]
        D[(Parquet Partitioned Table)]
    end

    subgraph Gold_Layer
        E[dbt: Aggregations]
        F[(Final Reports)]
    end

    A --> C
    B --> C
    C --> D
    D --> E
    E --> F

    style D fill:#f9f,stroke:#333
    style F fill:#bbf,stroke:#333