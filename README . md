AeroSense IoT Data Engineering Platform

1. Overview
[cite_start]**Objective:** Design and implement an end-to-end, fault-tolerant data platform for AeroSense to ingest, process, and expose high-frequency IoT sensor readings (temperature, humidity, pressure) [cite: 44-51].
[cite_start]**Scope:** The pipeline covers data generation, real-time ingestion (Kafka KRaft), windowed aggregation & anomaly detection (PySpark Structured Streaming), historical storage (Parquet Data Lake), and exposure (Flask REST API) [cite: 53-67].
[cite_start]**Technologies:** Python 3.9+, Apache Kafka 7.5 (KRaft mode), PySpark 3.5.3, Flask, Pandas[cite: 77].

2. Architecture
*(Note: A detailed architecture diagram is available in `docs/architecture.md`)*

The platform operates through decoupled layers:
 1. **Ingestion:** A Python producer simulates IoT devices, pushing JSON payloads to a 3-node Kafka cluster (topic: `sensor-events`, partitioned by sensor type).
 2. **Processing:** Spark Structured Streaming consumes the topics, parses payloads, validates physical ranges, detects anomalies, and computes 5-minute rolling statistics using Event Time and Watermarks.
 3. [cite_start]**Storage (Medallion Architecture):** * **Raw Zone:** Immutable JSON append-only log.
   * [cite_start]**Curated Zone:** Cleansed Parquet partitioned by sensor and event_time.
   * [cite_start]**Consumption Zone:** Aggregated Parquet datasets[cite: 253].
 4. [cite_start]**Serving:** A Flask REST API powered by Pandas directly reads the Parquet files for ultra-low-latency responses [cite: 66, 288-290].

3. Instructions & Execution
[cite_start]**Prerequisites:** Docker Compose, Python 3.9+, Java (for Spark)[cite: 77].

Step-by-step Execution:
1. **Start Infrastructure:** ```bash
   docker compose up -d

2.Setup Kafka Topic: ```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --create --topic sensor-events --partitions 3 --replication-factor 3

3.Start Spark Pipeline: ```bash
python src/spark_pipeline.py

4.Generate Data: In a new terminal, run:
python src/producer.py --count 2000 --rate 100

5.Run Offline Analytics: ```bash
python src/analytics.py

6.Start REST API: ```bash
python api/app.py

Test API (cURL):
Run the provided test script: bash tests/test_curl_commands.sh or manually:
curl -s [http://127.0.0.1:5000/api/v1/health](http://127.0.0.1:5000/api/v1/health)
curl -s "[http://127.0.0.1:5000/api/v1/anomalies?sensor=temperature&limit=5](http://127.0.0.1:5000/api/v1/anomalies?sensor=temperature&limit=5)"

4. Technical Choices & Justifications

Partitioning strategy for the curated zone: Partitioned by sensor, year, month, and day. Why: Downstream queries usually filter by specific sensor types and time windows. This hive-style layout enables Spark's Partition Pruning, skipping irrelevant directories entirely (Zero I/O), which drastically speeds up analytics as demonstrated in Part 4.  

Spark Structured Streaming outputMode: We strictly used append mode across all sinks. Why: Parquet files are immutable and do not natively support update mode. For the consumption zone, append works perfectly with our 2-minute watermark; Spark holds state in memory and only appends the final aggregated row to disk once the watermark crosses the window boundary.


Replication factor and min.insync.replicas: RF=3 and min.ISR=2. Why: This ensures High Availability. If one broker crashes, we still have 2 active replicas (satisfying min.ISR), allowing producers using acks=all to continue publishing without downtime or data loss.  

Use of event_time vs ingestion_time: The Raw zone uses ingestion_time (processing time) to reflect system reception accurately. Curated and Consumption zones use event_time from the JSON payload. Why: Business logic (like 5-minute temperature averages) must be calculated based on when the event actually occurred in the real world, handling late-arriving data gracefully via watermarks.

End-to-end delivery semantics: We achieve At-Least-Once semantics. Why: The producer guarantees delivery to Kafka (acks=all, retries=5). Spark guarantees exactly-once processing internally via checkpointing. However, writing to standard Parquet files is not fully ACID-compliant across crashes. A crash during a write might leave partial files, making the absolute end-to-end guarantee at-least-once.

5. Results & Evidence
Partition Pruning: Demonstrated successfully. Pruned queries execute significantly faster by avoiding full table scans.

Fault Tolerance: Kafka successfully re-elected partition leaders and shrunk the ISR when kafka2 was manually stopped (Details in docs/fault_tolerance.md).

API Delivery: Served analytical queries rapidly using Pandas without needing to boot a JVM cluster for every HTTP request.

6. Limitations & Future Improvements
Table Formats: Currently using raw Parquet. Upgrading the Data Lake to Delta Lake or Apache Iceberg would provide ACID transactions, enabling safe updates and true exactly-once pipelines.

API Scaling: Currently using a development Flask server. For production, it should be wrapped in gunicorn and placed behind an NGINX reverse proxy.



