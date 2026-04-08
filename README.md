# NYC Taxi Pipeline - Data Orchestration Project

## Pipeline Architecture (Medallion Architecture)

```mermaid
graph LR
    subgraph Bronze_Layer [Bronze Layer - Raw Data]
        direction TB
        B1[yellow_tripdata_*.csv]
        B2[taxi+_zone_lookup.csv]
    end

    subgraph Silver_Layer [Silver Layer - Cleaned & Partitioned]
        direction TB
        S1(DuckDB: Join & Clean)
        S2[(Parquet Partitioned Table)]
    end

    subgraph Gold_Layer [Gold Layer - Business Insights]
        direction TB
        G1(dbt: Aggregations)
        G2[(Final Reports / gold_tips)]
    end

    %% Connections
    B1 --> S1
    B2 --> S1
    S1 --> S2
    S2 --> G1
    G1 --> G2

    %% Styling for visibility and contrast
    style Bronze_Layer fill:#f9f9f9,stroke:#333,stroke-width:2px,color:#333
    style Silver_Layer fill:#f0f0f0,stroke:#333,stroke-width:2px,color:#333
    style Gold_Layer fill:#e9e9e9,stroke:#333,stroke-width:2px,color:#333

    %% Styling nodes for contrast (black text on lighter colors)
    style S1 fill:#333,stroke:#333,color:#fff
    style G1 fill:#333,stroke:#333,color:#fff
    
    %% Fixing Parquet & Final Reports visibility
    %% Using lighter pastel colors with black text
    style S2 fill:#ffccff,stroke:#cc00cc,color:#000,stroke-width:2px
    style G2 fill:#ccccff,stroke:#0000ff,color:#000,stroke-width:2px