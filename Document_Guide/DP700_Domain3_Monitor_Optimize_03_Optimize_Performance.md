# DP-700: Optimize Performance
## Monitor and Optimize an Analytics Solution (30–35%) — Phần 3/3


> [!IMPORTANT]
> **Bản hiệu chỉnh ngày 09/08/2026.** Nội dung được đối chiếu với blueprint DP-700 có hiệu lực từ **21/07/2026**. Microsoft cho biết phần lớn câu hỏi tập trung vào tính năng GA, nhưng tính năng Preview phổ biến vẫn có thể xuất hiện. Với các bảng “supported items”, runtime, limits và trạng thái Preview/GA, hãy kiểm tra Microsoft Learn lần cuối trước ngày thi.

> **Skills measured as of July 21, 2026**  
> Tài liệu chuẩn bị thi chứng chỉ **Microsoft Fabric Data Engineer Associate (DP-700)**  
> Tham chiếu: [Microsoft Learn — Study Guide DP-700](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)

> [!IMPORTANT]
> ### Cách dùng tài liệu này
> Đây là file cần ưu tiên cao vì **Optimize performance** là một trong Top 3 skill areas yếu nhất trong score report của bạn. Luôn theo chu trình:
>
> **Measure → identify bottleneck → choose smallest effective optimization → benchmark → monitor regression.**


---

## MỤC LỤC

1. [Optimize a Lakehouse Table](#1-optimize-a-lakehouse-table)
2. [Optimize a Pipeline](#2-optimize-a-pipeline)
3. [Optimize a Data Warehouse](#3-optimize-a-data-warehouse)
4. [Optimize Eventstreams and Eventhouses](#4-optimize-eventstreams-and-eventhouses)
5. [Optimize Spark Performance](#5-optimize-spark-performance)
6. [Optimize Query Performance](#6-optimize-query-performance)
7. [Câu hỏi mô phỏng bài thi (Practice Questions)](#7-câu-hỏi-mô-phỏng-bài-thi-practice-questions)

---


## 1. Optimize a Lakehouse Table

Tài liệu trọng yếu:
- [Delta table maintenance](https://learn.microsoft.com/en-us/fabric/data-engineering/delta-lake-table-maintenance)
- [V-Order](https://learn.microsoft.com/en-us/fabric/data-engineering/delta-optimization-and-v-order)
- [Liquid clustering](https://learn.microsoft.com/en-us/fabric/data-engineering/native-execution-engine-z-order-liquid-clustering)

### 1.1. OPTIMIZE vs VACUUM

**OPTIMIZE = compact active files**

```sql
OPTIMIZE gold.fact_sales;
```

Dùng khi:
- nhiều small files;
- read overhead tăng;
- sau large batch/merge khi file fragmentation rõ rệt.

**VACUUM = xóa unreferenced old files**

```sql
VACUUM gold.fact_sales;
VACUUM gold.fact_sales RETAIN 168 HOURS;
```

Default retention của Lakehouse maintenance là 7 ngày. Retention ngắn hơn có thể phá time travel/concurrent readers.

> 📌 **Exam reflex:** small files → `OPTIMIZE`; reclaim old storage → `VACUUM`.

### 1.2. V-Order

V-Order là **write-time Parquet optimization**:
- cải thiện read-heavy workloads;
- file vẫn open Parquet/Delta compatible;
- Microsoft nêu write overhead thường khoảng 15% trung bình;
- **disabled by default trong workspace mới**.

Session:

```python
spark.conf.set("spark.sql.parquet.vorder.default", "true")
```

Per write:

```python
(df.write
   .format("delta")
   .option("parquet.vorder.enabled", "true")
   .mode("append")
   .saveAsTable("gold.fact_sales"))
```

Qua maintenance:

```sql
OPTIMIZE gold.fact_sales VORDER;
```

### 1.3. Z-Order

```sql
OPTIMIZE gold.fact_sales
ZORDER BY (CustomerId, ProductId);
```

- chọn ít cột hay filter;
- giúp data skipping;
- phù hợp established workflows.

### 1.4. Liquid Clustering — cập nhật Runtime 2.0

```sql
CREATE TABLE gold.fact_sales
USING DELTA
CLUSTER BY (OrderDate, CustomerId)
AS
SELECT * FROM silver.sales_clean;
```

```sql
ALTER TABLE gold.fact_sales
CLUSTER BY (OrderDate, CustomerId);

OPTIMIZE gold.fact_sales;
```

Điểm 2026:
- Runtime 2.0+ có **incremental clustering**: chỉ files cần thiết được recluster.
- Runtime 1.3 có thể rewrite nhiều Z-cubes → write amplification.
- Liquid clustering không dùng chung với partitioning/Z-Order trên cùng table.
- `OPTIMIZE FULL` chỉ dùng có chủ đích sau đổi keys/quality reset.
- Delta 4.2-specific experimental features của Runtime 2.0 chỉ Spark; kiểm tra interoperability trước khi table được nhiều Fabric engines dùng.

### 1.5. Lakehouse Maintenance Activity

[Lakehouse Maintenance Activity](https://learn.microsoft.com/en-us/fabric/data-factory/lakehouse-maintenance-activity) trong Pipeline hiện hỗ trợ trọng tâm:
- **Optimize**
- **Vacuum**

```text
Copy / Notebook
   ↓
Lakehouse Maintenance Activity
   ├─ Optimize
   └─ Vacuum
```

> ⚠️ Hiệu chỉnh: V-Order **không phải một operation độc lập** được current Pipeline activity docs liệt kê. Trong Lakehouse portal Maintenance UI, Optimize có thể chọn **Apply V-Order**.

### 1.6. Materialized Lake View — đúng ngữ cảnh là Lakehouse

[Materialized Lake Views](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/overview-materialized-lake-view) thuộc Lakehouse/Spark.

```sql
CREATE MATERIALIZED LAKE VIEW gold.mv_sales_summary AS
SELECT
    CustomerId,
    SUM(Revenue) AS TotalRevenue
FROM silver.sales
GROUP BY CustomerId;
```

```sql
REFRESH MATERIALIZED LAKE VIEW gold.mv_sales_summary FULL;
```

> ⚠️ Fabric Warehouse T-SQL hiện **không hỗ trợ materialized views**. Không dùng `CREATE MATERIALIZED VIEW` trong Warehouse.


## 2. Optimize a Pipeline

Tài liệu: [Copy Activity Performance and Scalability Guide](https://learn.microsoft.com/en-us/fabric/data-factory/copy-activity-performance-and-scalability-guide).

### 2.1. Bottleneck mental model

```text
Achievable throughput ≈ min(source, network, destination, copy compute)
```

Tăng parallelism không đảm bảo nhanh hơn nếu source/sink đã saturated.

### 2.2. Tối ưu theo thứ tự

1. Filter rows ở source.
2. Project chỉ columns cần.
3. Incremental load thay full load khi phù hợp.
4. Tối ưu source query/storage.
5. Tune **Intelligent Throughput Optimization (ITO)**.
6. Tune **Degree of copy parallelism**.
7. Source partitioning nếu connector hỗ trợ.
8. Theo dõi output metrics.
9. Kiểm tra destination/capacity throttling.
10. Benchmark.

### 2.3. Intelligent Throughput Optimization

Current options:
- Auto
- Standard
- Balanced
- Maximum
- custom `4–256`

Đừng luôn chọn Maximum.

### 2.4. Degree of copy parallelism

`parallelCopies` là maximum threads. Runtime có thể dùng ít hơn.

```text
Tăng parallel copies
├─ tốt nếu source/network/sink còn headroom
└─ xấu nếu source/sink throttle
```

### 2.5. Adaptive performance tuning (Preview)

Copy Activity có **Adaptive performance tuning (Preview)**.

> Không nhầm với **Spark Autotune**.

### 2.6. Parallel orchestration

Independent activities có thể chạy song song; concurrency phải phù hợp source/sink/capacity.

### 2.7. Idempotency

```text
Watermark/CDC → Staging → Dedup → MERGE → Commit state after success
```

Retry phải không tạo duplicate.


## 3. Optimize a Data Warehouse

Tài liệu:
- [Warehouse performance guidelines](https://learn.microsoft.com/en-us/fabric/data-warehouse/guidelines-warehouse-performance)
- [Query Insights](https://learn.microsoft.com/en-us/fabric/data-warehouse/query-insights)
- [Data clustering (Preview)](https://learn.microsoft.com/en-us/fabric/data-warehouse/data-clustering)
- [T-SQL surface area](https://learn.microsoft.com/en-us/fabric/data-warehouse/tsql-surface-area)

### 3.1. Measure với Query Insights

```sql
SELECT TOP 20
    distributed_statement_id,
    query_hash,
    allocated_cpu_time_ms,
    data_scanned_remote_storage_mb,
    data_scanned_memory_mb,
    data_scanned_disk_mb,
    command
FROM queryinsights.exec_requests_history
ORDER BY data_scanned_remote_storage_mb DESC;
```

`data_scanned_remote_storage_mb > 0` → query phải fetch từ OneLake (cold-start effect). Không đánh giá performance chỉ bằng first run.

```sql
SELECT TOP 20 *
FROM queryinsights.long_running_queries;
```

### 3.2. Query plan

Current Warehouse guidance dùng **SHOWPLAN_XML trong SSMS** để xem plan mà không execute.

> ⚠️ Không học `EXPLAIN SELECT...` như Warehouse command mặc định.

### 3.3. Query-shape optimization

```sql
-- Không tốt
SELECT *
FROM dbo.FactSales;

-- Tốt hơn
SELECT OrderDate, CustomerKey, Revenue
FROM dbo.FactSales
WHERE OrderDate >= '2026-01-01';
```

Nguyên tắc:
- filter sớm;
- tránh `SELECT *`;
- data type phù hợp;
- transaction ngắn;
- tránh trickle DML, batch writes;
- retry transient write conflicts.

### 3.4. Statistics

Fabric có proactive/incremental statistics automation.

```sql
UPDATE STATISTICS dbo.FactSales;
```

Current T-SQL surface area không hỗ trợ manually created **multi-column** stats.

### 3.5. Data Clustering (Preview)

```sql
CREATE TABLE dbo.FactSales
(
    OrderDate DATE NOT NULL,
    CustomerKey BIGINT NOT NULL,
    Revenue DECIMAL(18,2)
)
WITH
(
    CLUSTER BY (OrderDate, CustomerKey)
);
```

Rules:
- 1–4 clustering columns;
- chọn cột thường filter;
- có ingestion overhead;
- current feature Preview.

### 3.6. Warehouse V-Order

V-Order enabled by default ở Warehouse.

```sql
SELECT name, is_vorder_enabled
FROM sys.databases;
```

Disable:

```sql
ALTER DATABASE CURRENT SET VORDER = OFF;
```

> ⚠️ Current docs: disable ở Warehouse là **irreversible**. Không tắt nếu chưa benchmark.

### 3.7. Materialized view trap

Warehouse T-SQL current surface area liệt kê **materialized views unsupported**.

- **Lakehouse** → Materialized Lake View.
- **Warehouse** → Query Insights, query shape, stats, data clustering, caching/automatic maintenance.


## 4. Optimize Eventstreams and Eventhouses

### 4.1. Hot cache vs retention

[KQL Data Policies](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/data-policies):

- **Retention** = data sống bao lâu.
- **Caching** = bao nhiêu dữ liệu gần đây nằm ở hot SSD để query nhanh.
- cache period phải `<= retention`.

```kusto
.show table SensorData policy caching
.show table SensorData policy retention
```

```kusto
.alter table SensorData policy caching hot = 30d
```

Không đặt hot cache dài nhất cho mọi table; chọn theo query recency.

### 4.2. Capacity Planner

[Capacity Planner](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/eventhouse-smart-capacity-control) là tên hiện tại của capability trước gọi Always-On.

Dùng khi:
- strict uptime/wake-up latency;
- predictable high-load windows;
- cần guaranteed baseline CUs.

Có 7-day recurring schedule theo 60-minute blocks; autoscale vẫn hoạt động.

### 4.3. Streaming ingestion policy

```kusto
.alter table SensorData policy streamingingestion
'{"IsEnabled": true}'
```

> ⚠️ Không dùng syntax cũ/sai `.alter ... policy streamingingestion enable`.

### 4.4. KQL query optimization

```kusto
SensorData
| where EventTime > ago(1h)
| project DeviceId, EventTime, Temperature
| summarize AvgTemp = avg(Temperature)
  by DeviceId, bin(EventTime, 5m)
```

Repeated aggregation:

```kusto
.create materialized-view SensorHourlySummary on table SensorData
{
    SensorData
    | summarize
        AvgTemp = avg(Temperature),
        MaxTemp = max(Temperature)
      by DeviceId, bin(EventTime, 1h)
}
```

### 4.5. Eventstream

- filter early;
- drop unused fields early;
- minimize unnecessary transforms;
- choose destination đúng workload;
- monitor source → processor → destination;
- backlog tăng → tìm bottleneck trước khi scale.


## 5. Optimize Spark Performance

Tài liệu:
- [Spark Autotune](https://learn.microsoft.com/en-us/fabric/data-engineering/autotune)
- [Runtime 2.0](https://learn.microsoft.com/en-us/fabric/data-engineering/runtime-2-0)

### 5.1. Physical plan trước

```python
result.explain("formatted")
```

Tìm shuffle/exchange, join strategy, predicate pushdown, partition count, skew.

### 5.2. Broadcast join

```python
from pyspark.sql.functions import broadcast

result = fact.join(
    broadcast(dim),
    "CustomerId",
    "left"
)
```

Chỉ broadcast nếu table đủ nhỏ để fit executor memory. Row width quan trọng, không chỉ row count.

### 5.3. Partition tuning

```python
print(df.rdd.getNumPartitions())

df2 = df.repartition(200, "CustomerId")
df3 = df2.coalesce(50)

spark.conf.set("spark.sql.shuffle.partitions", "200")
```

### 5.4. Adaptive Query Execution (AQE)

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

### 5.5. Spark Autotune — correction quan trọng

Current Microsoft Learn:
- **Preview**
- **disabled by default**
- enable bằng `spark.ms.autotune.enabled=true`
- tune **3 settings**:
  1. `spark.sql.shuffle.partitions`
  2. `spark.sql.autoBroadcastJoinThreshold`
  3. `spark.sql.files.maxPartitionBytes`
- best với repetitive Spark SQL queries >~15s;
- thường cần khoảng 20–25 iterations.

```python
spark.conf.set("spark.ms.autotune.enabled", "true")
```

#### Compatibility trap

Current docs nói Autotune **chỉ tương thích Runtime 1.2** và không bật được trên runtime >1.2.

Runtime 1.2 đã end-of-support 31/03/2026; Microsoft khuyến nghị production Runtime 1.3.

> ⚠️ **Không ghép “Runtime 2.0 + Spark Autotune” thành best practice.**

### 5.6. Runtime 2.0

Runtime 2.0 Preview có Spark 4.1/Delta 4.2 và Native Execution Engine improvements.

Delta 4.2-specific experimental features chỉ Spark; nếu same Delta tables được nhiều Fabric workloads dùng, kiểm tra table-format interoperability.

### 5.7. Cache có chủ đích

```python
dim = spark.table("gold.dim_customer").cache()

r1 = fact1.join(dim, "CustomerKey")
r2 = fact2.join(dim, "CustomerKey")

dim.unpersist()
```

Không cache DataFrame dùng một lần hoặc quá lớn gây spill.


## 6. Optimize Query Performance

### 6.1. Cross-engine decision matrix

| Symptom | First move |
|---|---|
| Lakehouse small files | `OPTIMIZE` |
| Old unreferenced Delta files | `VACUUM` |
| Lakehouse scan quá nhiều files | Liquid clustering / Z-Order |
| Read-heavy multi-engine Delta | benchmark V-Order |
| Copy slow | source/network/sink bottleneck + ITO/parallelCopies |
| Warehouse first run slow | Query Insights remote scan |
| Warehouse repeated heavy filters | query shape / data clustering / stats |
| KQL recent queries slow | time filter + hot cache |
| Spark large shuffle | plan + broadcast/partition/AQE |
| Few Spark stragglers | skew |
| Eventstream backlog | source/processor/destination throughput |

### 6.2. Measure-first checklist

```text
1. Baseline duration
2. Data volume
3. Files/partitions
4. CU/CPU
5. Remote scan / shuffle
6. Apply ONE major change
7. Rerun comparable workload
8. Record regression/improvement
```

### 6.3. Anti-patterns

- “Tăng capacity” trước khi biết bottleneck.
- `VACUUM` để giải quyết small files.
- Broadcast table chỉ vì “dimension”.
- Tăng parallelCopies vô hạn.
- Cache mọi DataFrame.
- Bật experimental Delta feature trên shared cross-engine table mà không test.
- Dùng Warehouse materialized view syntax.
- Bật Spark Autotune trên Runtime 2.0.


## 7. Câu hỏi mô phỏng bài thi (Practice Questions)

### Q1 — Small files
50.000 active Delta files nhỏ.  
A. VACUUM · B. OPTIMIZE · C. Git · D. Capacity Planner  
**✅ B**

### Q2 — Old files
Storage cao do unreferenced versions.  
A. VACUUM · B. Broadcast · C. Activator · D. Folding  
**✅ A**

### Q3 — Read-heavy Delta
Table chủ yếu phục vụ Direct Lake/SQL analytics, writes ít.  
A. Cân nhắc V-Order + benchmark · B. Retry · C. #temp · D. Disable cache  
**✅ A**

### Q4 — Runtime 2.0 flexible layout
Incremental table, changing filter keys.  
A. Liquid clustering · B. VACUUM each query · C. Manual multi-column stats · D. DirectLakeOnly  
**✅ A**, sau khi kiểm tra interoperability.

### Q5 — Copy bottleneck
Tăng parallel copies nhưng source DB CPU 100%.  
A. Tăng tiếp · B. Source bottleneck; optimize/scale source · C. V-Order · D. Spark Autotune  
**✅ B**

### Q6 — Warehouse first run
First run slow, subsequent fast, remote scan >0 lần đầu.  
A. Cold-cache/remote read · B. Spark skew · C. Git · D. Eventstream  
**✅ A**

### Q7 — Warehouse materialized view
`CREATE MATERIALIZED VIEW` trong Fabric Warehouse?  
A. GA · B. Preview · C. Unsupported; MLV thuộc Lakehouse · D. V-Order only  
**✅ C**

### Q8 — Spark Autotune
Câu đúng?  
A. Runtime2 default-on · B. Preview/off-default/tunes 3 configs/Runtime1.2 compatibility · C. executor memory only · D. Copy feature  
**✅ B**

### Q9 — Eventhouse hot cache
Retention 365d, dashboard query 7d.  
A. Hot cache theo query recency + benchmark · B. delete >7d · C. VACUUM · D. coalesce  
**✅ A**

### Q10 — Pipeline maintenance
Current Lakehouse Maintenance Activity hỗ trợ trọng tâm?  
A. Optimize + Vacuum · B. V-Order only · C. Git · D. KQL MV  
**✅ A**

---

## 8. CODE CHEAT SHEET

```sql
-- Lakehouse / Spark SQL
OPTIMIZE gold.fact_sales;
VACUUM gold.fact_sales RETAIN 168 HOURS;
OPTIMIZE gold.fact_sales VORDER;
OPTIMIZE gold.fact_sales ZORDER BY (CustomerId);
DESCRIBE DETAIL gold.fact_sales;
DESCRIBE HISTORY gold.fact_sales;
```

```sql
-- Liquid clustering
ALTER TABLE gold.fact_sales
CLUSTER BY (OrderDate, CustomerId);

OPTIMIZE gold.fact_sales;
```

```sql
-- Warehouse Query Insights
SELECT TOP 10
    query_hash,
    allocated_cpu_time_ms,
    data_scanned_remote_storage_mb,
    data_scanned_memory_mb,
    data_scanned_disk_mb,
    command
FROM queryinsights.exec_requests_history
ORDER BY allocated_cpu_time_ms DESC;
```

```kusto
SensorData
| where EventTime > ago(24h)
| project EventTime, DeviceId, Temperature
| summarize P95 = percentile(Temperature, 95)
  by DeviceId, bin(EventTime, 15m)
```

```python
from pyspark.sql.functions import broadcast

result = fact.join(broadcast(dim), "CustomerKey")
result.explain("formatted")
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

---

## 9. LAB THỰC HÀNH

- **Lab 1:** small files → OPTIMIZE; sau đó VACUUM riêng và giải thích khác biệt.
- **Lab 2:** A/B V-Order: write time, read time, compression.
- **Lab 3:** Runtime 2.0 liquid clustering + OPTIMIZE + files scanned.
- **Lab 4:** Copy Activity: ITO Auto/Maximum + tuned parallelism, theo dõi source CPU.
- **Lab 5:** Warehouse: first/second run + Query Insights remote scan; test Data Clustering nếu Preview enabled.
- **Lab 6:** Spark: plan trước/sau broadcast, skew + AQE; giải thích vì sao không bật Autotune trên Runtime 2.0.

### Readiness gate
Không nhìn tài liệu, giải thích được:
- OPTIMIZE vs VACUUM vs V-Order.
- Z-Order vs Liquid Clustering.
- Lakehouse MLV vs Warehouse limitation.
- ITO vs parallelCopies.
- Warehouse cold-cache metric.
- hot cache vs retention vs Capacity Planner.
- broadcast/AQE/partition/skew.
- Spark Autotune compatibility.

---

## 10. NGUỒN MICROSOFT LEARN TRỌNG YẾU

- [DP-700 Study Guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)
- [Delta table maintenance](https://learn.microsoft.com/en-us/fabric/data-engineering/delta-lake-table-maintenance)
- [V-Order](https://learn.microsoft.com/en-us/fabric/data-engineering/delta-optimization-and-v-order)
- [Liquid clustering](https://learn.microsoft.com/en-us/fabric/data-engineering/native-execution-engine-z-order-liquid-clustering)
- [Lakehouse Maintenance Activity](https://learn.microsoft.com/en-us/fabric/data-factory/lakehouse-maintenance-activity)
- [Materialized Lake Views](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/overview-materialized-lake-view)
- [Copy Activity performance](https://learn.microsoft.com/en-us/fabric/data-factory/copy-activity-performance-and-scalability-guide)
- [Warehouse performance guidelines](https://learn.microsoft.com/en-us/fabric/data-warehouse/guidelines-warehouse-performance)
- [Query Insights](https://learn.microsoft.com/en-us/fabric/data-warehouse/query-insights)
- [Warehouse Data Clustering](https://learn.microsoft.com/en-us/fabric/data-warehouse/data-clustering)
- [T-SQL surface area](https://learn.microsoft.com/en-us/fabric/data-warehouse/tsql-surface-area)
- [Eventhouse Capacity Planner](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/eventhouse-smart-capacity-control)
- [KQL data policies](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/data-policies)
- [Spark Autotune](https://learn.microsoft.com/en-us/fabric/data-engineering/autotune)
- [Runtime 2.0](https://learn.microsoft.com/en-us/fabric/data-engineering/runtime-2-0)
