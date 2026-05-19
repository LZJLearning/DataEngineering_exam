# Reflection Questions

**1. Your pipeline crashes during processing, after writing to the raw zone but before writing to the curated zone. What is the impact on the data? Which checkpoint strategy prevents this issue?**
* **Impact:** This creates a data inconsistency between lake zones. The data will exist in the Raw zone but will be missing from the Curated and Consumption zones. Any downstream analytical queries relying on the Curated zone will yield incomplete results.
* **Strategy:** This issue is prevented by using **independent checkpoint directories** for each specific `writeStream` sink. Because the Curated stream has its own checkpoint log, when the Spark application restarts, it will read its specific offset log, realize it has not yet processed those specific Kafka offsets for the Curated sink, and resume exactly where it left off. This allows it to catch up without duplicating data in the Raw zone.

**2. You scale the producer up to 50,000 messages per second. In your opinion, what would be the first bottlenecks in your current architecture, and how would you fix them?**
* **Bottleneck 1 (Kafka Partitions):** Our `sensor-events` topic only has 3 partitions. This hard-caps the maximum Spark consumer parallelism to exactly 3 concurrent tasks. 
  * **Fix:** Increase the number of topic partitions to a much higher number (e.g., 100+) to allow massive horizontal scaling of consumer executors.
* **Bottleneck 2 (Compute Resources):** Running Spark locally (`local[*]`) will quickly exhaust the CPU and memory of a single machine. 
  * **Fix:** Deploy the Spark application onto a distributed cluster manager (such as YARN, Kubernetes, or AWS EMR) to distribute the processing load across multiple worker nodes.

**3. Compare the advantages and drawbacks of using Kafka as the source of truth for historical data, versus a Parquet data lake. In which scenarios should each be preferred?**
* **Apache Kafka:** * *Pros:* Native real-time replayability, strict event ordering, and an immutable append-only log. 
  * *Cons:* Highly expensive storage (usually SSD-backed), and terrible for complex ad-hoc analytical queries (no secondary indexes or columnar compression). 
  * *Preferred for:* Short-to-medium term event buffering and driving real-time streaming microservices.
* **Parquet Data Lake:** * *Pros:* Extremely cost-effective object storage. The columnar format drastically speeds up analytical queries (aggregations) and supports partition pruning (Zero I/O on skipped directories). 
  * *Cons:* High latency for individual row reads, and its immutable nature makes single-row updates difficult. 
  * *Preferred for:* Long-term historical storage, BI dashboarding, and Machine Learning model training.

**4. A sensor breaks and emits aberrant values for 2 hours. How does your architecture detect this case? How would you isolate these data points without deleting them?**
* **Detection:** The Spark Structured Streaming job automatically flags these readings by evaluating predefined physical/business thresholds and setting the `is_anomaly` column to `True`. Additionally, the API's `/api/v1/anomalies` endpoint would reveal a massive spike in alerts for this specific source.
* **Isolation:** In Data Engineering, we never delete raw data. The aberrant data remains in the Data Lake but is logically isolated using the `is_anomaly` flag. Standard business dashboards query with `WHERE is_anomaly = False` to view normal operations, while Predictive Maintenance teams can actively query `WHERE is_anomaly = True` to diagnose the faulty hardware.

**5. You must add a new sensor type `co2`. Which parts of your pipeline must be modified? Give a precise list of files and changes.**
1. **`src/producer.py`**: Add `"co2"` to the `SENSORS` dictionary, defining its unit (e.g., "ppm") and its normal/anomaly value ranges so the simulator can generate realistic test data.
2. **`src/spark_pipeline.py`**: Update the physical validation filter to include plausible ranges for CO2 (e.g., 400 to 5000 ppm), and add a new `.when()` condition in the `is_anomaly` column generation to define what constitutes a CO2 alert.
3. **`api/app.py`**: Add `"co2"` to the `VALID_SENSORS` set to allow the REST API gateway to accept incoming requests and queries for this new sensor type.