# Analytics and Partition Pruning Report

## 1. Overview
This document summarizes the results of the analytical queries executed on the Curated Zone of the data lake (Parquet format) using Spark SQL. The raw CSV outputs are available in the `outputs/analytics/` directory.

## 2. Analytical Queries
* **Query 1 (Top Anomaly Hours):** Successfully aggregated data by `year`, `month`, `day`, and extracted `hour` from the `event_time` to identify operational bottlenecks.
* **Query 2 (Global Stats):** Computed global metrics (mean, min, max, stddev, anomaly rate %) for all sensor types. The anomaly rate effectively reflected the 10-15% injection rate defined in the producer.
* **Query 3 (Temperature Evolution):** Filtered specifically on `sensor='temperature'` to build a daily time-series of average temperatures and anomaly counts.

## 3. Partition Pruning Demonstration
**Objective:** Prove that partitioning the data lake by `sensor`, `year`, `month`, and `day` drastically improves query performance.

**Methodology:**
We ran two queries that yield the exact same result (counting temperature readings) but are written differently to toggle Spark's optimizer:
1. **With Pruning:** `WHERE sensor = 'temperature'`
   * *Mechanism:* Spark reads the Hive-style directory structure (`sensor=temperature/`) and completely ignores directories like `sensor=humidity/`. It avoids reading useless data into memory (Zero I/O on excluded partitions).
2. **Without Pruning:** `WHERE lower(sensor) = 'temperature'`
   * *Mechanism:* By wrapping the partition column in a `lower()` function, we blind the Spark Catalyst Optimizer. It is forced to perform a **Full Table Scan**, opening every single Parquet file across all sensors to evaluate the condition.

**Results:**
*(Refer to `outputs/analytics/q4_pruning_demo.txt` for exact run timings)*
The query **WITH pruning** executed significantly faster, demonstrating a substantial speedup factor. As the data lake grows to terabytes over months, this partition pruning mechanism is the critical factor preventing pipeline failure.