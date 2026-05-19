# Platform Architecture

## Pipeline Diagram
```text
+-------------------+       +-----------------------+       +---------------------------------+
|   IoT Sensors     |       |   Kafka Cluster       |       |   Spark Structured Streaming    |
|  (Python Script)  | ----> |  (3 Brokers, KRaft)   | ----> |  (Validation, Watermarking,     |
|                   | JSON  | Topic: sensor-events  |       |   Anomaly Detection)            |
+-------------------+       +-----------------------+       +---------------------------------+
                                                                 |         |         |
                                                           (JSON)|(Parquet)|(Parquet)|
                                                                 v         v         v
+-------------------+       +-----------------------+       +---------------------------------+
| Consuming Apps /  | <---- |   REST API Gateway    | <---- |         Data Lake               |
| BI Dashboards     | JSON  |   (Flask + Pandas)    | Reads |  [Raw]  [Curated] [Consumption] |
+-------------------+       +-----------------------+       +---------------------------------+

Component Description
1. Python Generator (producer.py): Acts as the simulated IoT network. It generates high-frequency readings for temperature, humidity, and pressure, intentionally injecting ~15% anomalies. It uses strict reliability settings (acks='all') and key-based partitioning to ensure strict ordering per sensor type.

2. Message Bus (Apache Kafka): A 3-node KRaft cluster acting as the central nervous system. It buffers incoming bursts of traffic, decoupling the data generation layer from the slower processing layer. min.insync.replicas=2 guarantees high availability.

3. Processing Engine (PySpark): The core ETL engine. It subscribes to Kafka, casts JSON payloads to structured schemas, filters out physical impossibilities, detects business anomalies, and computes 5-minute rolling averages using Event Time and Watermarks.

4. Storage Layer (Data Lake): Divided into three zones:
 Raw: Immutable JSON storage partitioned by ingestion time.
 Curated: Cleaned, schema-enforced Parquet storage partitioned by event time and sensor type for fast analytics.
 Consumption: Aggregated metrics for downstream dashboards.

5. Serving Layer (app.py): A Flask-based REST API that exposes real-time data directly from Kafka and historical data from the Parquet Data Lake (using Pandas for low-latency retrieval instead of Spark).