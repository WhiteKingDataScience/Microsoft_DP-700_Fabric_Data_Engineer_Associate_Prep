# DP-700: Optimize Performance
## Monitor and Optimize an Analytics Solution (30–35%) — Phần 3/3


> [!IMPORTANT]
> **Bản cập nhật ngày 05/11/2026.** Đối chiếu với [Study Guide DP-700](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700) (Microsoft cập nhật trang ngày **30/10/2026**) và tài liệu Microsoft Learn tháng 10–11/2026. Bản trước (09/08/2026) dựa trên blueprint 21/07/2026; những gì đã đổi nằm ở chương **⚡ CẬP NHẬT 05/11/2026** cuối file và ở đầu file [MASTER](DP700_00_MASTER_Trong_Tam_On_Thi.md). Microsoft cho biết phần lớn câu hỏi tập trung vào tính năng GA, nhưng tính năng Preview phổ biến vẫn có thể xuất hiện. Với các bảng “supported items”, runtime, limits và trạng thái Preview/GA, hãy kiểm tra Microsoft Learn lần cuối trước ngày thi.

> **Đối chiếu Study Guide DP-700 — trang cập nhật 30/10/2026**  
> Tài liệu chuẩn bị thi chứng chỉ **Microsoft Fabric Data Engineer Associate (DP-700)**  
> Tham chiếu: [Microsoft Learn — Study Guide DP-700](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)

> [!IMPORTANT]
> ### Cách dùng tài liệu này
> Đây là file cần ưu tiên cao vì **Optimize performance** là nhóm kỹ năng khó nhất của DP-700: đề mô tả triệu chứng rồi hỏi nguyên nhân/biện pháp. Luôn theo chu trình:
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

Rules (đối chiếu Learn 11/2026):
- 1–4 clustering columns; thứ tự cột trong `CLUSTER BY` **không** ảnh hưởng;
- chọn cột **mid/high cardinality** thường xuất hiện trong `WHERE` (ID, ngày); cột ít giá trị (giới tính, vùng) lợi ích thấp; **equality join không hưởng lợi**;
- chỉ khai báo lúc `CREATE TABLE` hoặc **CTAS**; **không** thêm/đổi sau; `SELECT INTO` không hỗ trợ; không dùng cột `IDENTITY`;
- kiểu không hỗ trợ: `bit`, `varchar(max)`, `varbinary`, `uniqueidentifier`; `decimal` precision > 18 không push-down predicate;
- có ingestion overhead (chậm hơn, tốn CU hơn);
- current feature **Preview**.

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

Runtime 1.2 đã end-of-support 31/03/2026. Từ 10/2026, **Runtime 2.0 là GA và mặc định** (Runtime 1.3 = EOSA) → trên cả hai runtime đang dùng, Autotune **không bật được**.

> ⚠️ **Không ghép “Runtime 2.0 + Spark Autotune” thành best practice.**

### 5.6. Runtime 2.0

Runtime 2.0 — **GA, mặc định cho workspace/Environment mới, khuyến nghị cho production (10/2026)** — có Spark 4.1/Delta 4.2 và Native Execution Engine improvements. Các default liên quan hiệu năng thay đổi so với 1.3 (xem chương **⚡ CẬP NHẬT 05/11/2026** cuối file).

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

---

# 🎯 CHƯƠNG BỔ SUNG 06/09/2026 — HỌC LẠI TỪ GỐC

> **Vì sao có chương này:** *Optimize performance* là nhóm kỹ năng khó nhất của DP-700, vì đề gần như không bao giờ hỏi "V-Order là gì". Đề mô tả một **triệu chứng** rồi hỏi nguyên nhân hoặc biện pháp. Sai lầm phổ biến nhất là chọn "biện pháp nghe hợp lý" thay vì biện pháp **khớp đúng triệu chứng**. Chương này viết lại theo hướng **chẩn đoán trước, kê đơn sau**.
> Xem thêm: [File tổng chỉ huy](DP700_00_MASTER_Trong_Tam_On_Thi.md)

## A. NGUYÊN TẮC SỐ MỘT: ĐO TRƯỚC, TỐI ƯU SAU

```text
Triệu chứng  →  Công cụ chẩn đoán  →  Nguyên nhân  →  Biện pháp khớp nguyên nhân
     ▲                                                        │
     └──────────────── đo lại để xác nhận ────────────────────┘
```

Nếu một đáp án **nhảy thẳng từ triệu chứng sang biện pháp** mà không có bước chẩn đoán (ví dụ "chậm → scale lên SKU lớn hơn"), khả năng cao đó là đáp án bẫy. Fabric có bốn cửa chẩn đoán, và bạn phải chọn đúng cửa:

| Vấn đề nằm ở | Mở công cụ nào |
|---|---|
| Một Spark notebook/job | **Spark UI** (Jobs → Stages → tasks, spill, skew) |
| Một query T-SQL trong Warehouse | **Query Insights** + query plan |
| Một query KQL trong Eventhouse | Query stats, caching policy, materialized view |
| Một pipeline/Copy | Activity run detail (throughput, DIU, duration) |
| **Nhiều thứ cùng chậm một lúc** | **Capacity Metrics App** (throttling/smoothing) |

## B. BẢNG TRIỆU CHỨNG → CHẨN ĐOÁN → XỬ LÝ (học thuộc bảng này)

| # | Triệu chứng trong đề | Chẩn đoán bằng | Nguyên nhân | Biện pháp đúng |
|:--:|---|---|---|---|
| 1 | Query Delta chậm dần theo thời gian; thư mục có hàng nghìn file nhỏ | Đếm file / Lakehouse maintenance | **Small file problem** | `OPTIMIZE` (định kỳ) |
| 2 | Dung lượng OneLake phình to dù số dòng không tăng | Storage metrics | File cũ vẫn được giữ cho time travel | `VACUUM` (giữ ≥ retention, mặc định 7 ngày) |
| 3 | Report Power BI Direct Lake chậm lúc cold, nhanh dần về sau | Direct Lake memory state | Chưa transcoding xong / thiếu V-Order | Bật **V-Order**, `OPTIMIZE ... VORDER`, tránh Overwrite |
| 4 | Direct Lake bỗng chạy DirectQuery | `EVALUATE TABLETRAITS()` → `DirectLakeFallbackInfo` | RLS/CLS/DDM ở SQL endpoint, view, vượt guardrail | Gỡ RLS khỏi SQL endpoint (đưa RLS vào model), thay view bằng bảng, giảm số file/row group |
| 5 | 1 task chạy 30 phút, 199 task chạy 2 giây | **Spark UI → Stages** (task duration min/median/max) | **Data skew** | Salting, `spark.sql.adaptive.skewJoin.enabled`, repartition, broadcast bảng nhỏ |
| 6 | Stage Spark có cột spill lớn | **Spark UI → Stages/Executors** | Shuffle vượt RAM → **spill** ra disk | Giảm dữ liệu shuffle (lọc/chiếu sớm), chỉnh `spark.sql.shuffle.partitions`, tăng node size |
| 7 | Join Spark chậm, một bảng rất nhỏ | Physical plan (`.explain()`) | Không dùng broadcast | **Broadcast join** (`broadcast(df)` hoặc chỉnh `autoBroadcastJoinThreshold`) |
| 8 | Notebook mất vài phút mới bắt đầu chạy | Session start time | Đang dùng **custom pool** (khởi động nguội) | Starter pool cho dev; **high concurrency** để dùng lại session |
| 9 | Copy activity chậm, tăng parallel không cải thiện | Activity output (throughput, DIU) | **Source hoặc sink đã bão hoà** | Lọc/chiếu ở nguồn, incremental thay full, partition nguồn; đừng tăng `parallelCopies` mù quáng |
| 10 | Query Warehouse chậm ngay sau bulk load lớn | **Query Insights** + plan | Statistics lệch | `UPDATE STATISTICS` / `CREATE STATISTICS` cho cột filter/join |
| 11 | Query Warehouse tốn RAM bất thường, có spill | Plan + kiểu cột | `VARCHAR(MAX)` hoặc kiểu quá rộng | Khai báo độ dài sát thực tế |
| 12 | Query KQL trên dữ liệu 90 ngày rất chậm, 7 ngày rất nhanh | Query stats | Dữ liệu ngoài **hot cache** | Tăng **caching policy** (không phải retention) |
| 13 | Eventhouse ingest trễ | Ingestion metrics / capacity planner | Batching policy hoặc thiếu tài nguyên | Chỉnh ingestion (streaming vs batching), xem capacity |
| 14 | Toàn bộ workspace chậm cùng lúc mỗi sáng 9h | **Capacity Metrics** (timepoint, background vs interactive) | **Throttling do trùng giờ cao điểm** | Dời lịch job nặng, scale-up SKU, surge protection |
| 15 | Pipeline fail ngay khi trigger vào giờ cao điểm | Capacity Metrics | **Background rejection** (giai đoạn throttling nặng) | Giảm tải, dời lịch, scale-up |

> 📌 Cách dùng bảng này để luyện: che cột "Biện pháp đúng", đọc cột triệu chứng và tự nói ra chẩn đoán + biện pháp. Đây gần như là mô phỏng chính xác dạng câu hỏi của phần Optimize.

## C. CAPACITY: SMOOTHING, BURSTING, THROTTLING — HIỂU CƠ CHẾ, ĐỪNG HỌC SỐ

```text
Job chạy  →  tiêu thụ CU
              │
              ├── BURSTING: cho phép dùng nhiều CU hơn baseline trong thời gian ngắn
              │              (để job ngắn chạy nhanh)
              │
              └── SMOOTHING: phần CU đã dùng được TRẢI RA theo thời gian
                             ├── Interactive (query Power BI, ad-hoc): trải theo phút
                             └── Background (pipeline theo lịch, Spark job, scheduled refresh): trải 24 GIỜ

Nếu tổng mức tiêu thụ đã trải vượt quá capacity trong tương lai:
   Giai đoạn 1  →  INTERACTIVE DELAY        (thêm độ trễ cho thao tác tương tác)
   Giai đoạn 2  →  INTERACTIVE REJECTION    (chặn request tương tác mới; background vẫn chạy)
   Giai đoạn 3  →  BACKGROUND REJECTION     (chặn cả background)
```

**Ba hệ quả hay được hỏi:**

1. **Vượt 100% không có nghĩa là bị chặn ngay.** Có overage protection cho phép "vay" một khoảng capacity tương lai. Đáp án "CU > 100% nên job bị reject" thường sai.
2. **Background trải 24 giờ** nghĩa là một job Spark nặng lúc 2h sáng vẫn có thể ảnh hưởng tới capacity buổi chiều.
3. **Scale-up SKU có hiệu lực gần như tức thì** — đây là biện pháp cấp cứu hợp lệ khi capacity đang bị throttle nặng; nhưng nếu nguyên nhân là skew/small files thì scale chỉ đốt tiền.

## D. LAKEHOUSE/DELTA — SÁU KỸ THUẬT, DÙNG ĐÚNG CHỖ

| Kỹ thuật | Câu hỏi nó trả lời | Lưu ý thi |
|---|---|---|
| `OPTIMIZE` | "Quá nhiều file nhỏ" | Chỉ chạy được từ **Spark** (notebook/job/maintenance UI), **không** chạy từ SQL analytics endpoint |
| `VACUUM` | "Thu hồi dung lượng" | Mặc định giữ 7 ngày; đặt quá ngắn phá time travel và có thể gây lỗi cho reader đang chạy |
| **V-Order** | "Đọc nhanh hơn cho Direct Lake/VertiPaq" | **Tắt mặc định ở workspace mới**; ghi chậm hơn ~15%, nén tốt hơn |
| **Z-Order** | "Data skipping theo vài cột hay lọc" | `OPTIMIZE t ZORDER BY (col1, col2)`; chọn ít cột |
| **Partitioning** | "Cắt bớt dữ liệu quét" | Chỉ partition theo cột **low-cardinality** (ngày, vùng). Partition sai tạo ra chính vấn đề small files |
| **Liquid clustering** | "Thay thế partition + Z-Order, linh hoạt" | `CLUSTER BY (...)`; **không** dùng chung với partitioning/Z-Order trên cùng bảng |

**Lịch bảo trì gợi ý (hay xuất hiện trong câu hỏi thiết kế):**

```text
OPTIMIZE   bảng giá trị cao, ghi nhiều   → hằng đêm hoặc hằng tuần
Z-ORDER    bảng có query pattern ổn định → hằng tuần/tháng
VACUUM     mọi bảng                      → hằng tháng, giữ retention mặc định
```

Trong pipeline, dùng **Lakehouse Maintenance activity** để chạy Optimize/Vacuum theo lịch thay vì tự viết notebook.

## E. DIRECT LAKE — BỐN KHÁI NIỆM PHẢI PHÂN BIỆT

```text
TRANSCODING   Nạp DỮ LIỆU theo cột từ OneLake vào cache VertiPaq,
              chỉ khi query chạm cột đó lần đầu.
              Trạng thái bộ nhớ: cold → semiwarm → warm → hot

FRAMING       Thao tác lúc refresh: đọc METADATA Delta, chốt tập file Parquet
              mà model được phép nhìn thấy. Query chỉ thấy dữ liệu tại thời điểm
              framing gần nhất — không nhất thiết là mới nhất.

FALLBACK      Direct Lake ON SQL ENDPOINT chuyển sang DirectQuery khi gặp:
              RLS/CLS/DDM ở tầng SQL · view chưa materialize · vượt guardrail SKU
              → chậm hơn nhiều.
              Direct Lake ON ONELAKE chạy DirectLakeOnly: KHÔNG fallback, nó BÁO LỖI.

GUARDRAIL     Giới hạn theo SKU về số parquet file / row group / số dòng.
              Một bảng vượt guardrail có thể làm cả model mất chế độ Direct Lake.
```

**Cách chẩn đoán fallback:** chạy DAX `EVALUATE TABLETRAITS()` và đọc cột `DirectLakeFallbackInfo` (giá trị `None` nghĩa là đang chạy Direct Lake thật).

**Thiết lập `DirectLakeBehavior` (chỉ áp dụng cho Direct Lake on SQL endpoint):**

| Giá trị | Hành vi | Dùng khi |
|---|---|---|
| `Automatic` (mặc định) | Fallback im lặng sang DirectQuery | Production |
| `DirectLakeOnly` | Báo lỗi thay vì fallback | **Dev** — để phát hiện nguyên nhân fallback |
| `DirectQueryOnly` | Luôn DirectQuery | Đo hiệu năng đối chứng |

**Bốn quy tắc vận hành để giữ Direct Lake nhanh:**

1. Áp **V-Order** cho bảng Gold.
2. Nhắm row group khoảng **1–16 triệu dòng**; chạy `OPTIMIZE` thưa (cuối tuần/cuối tháng) vì nó làm invalidate segment đang nằm trong bộ nhớ.
3. **Tránh Overwrite toàn bảng** — nó khiến model về cold. Ưu tiên append/MERGE.
4. Partition theo cột **low-cardinality** (dưới khoảng 100–200 giá trị).

## F. SPARK — NĂM ĐÒN BẨY

| Đòn bẩy | Khi nào dùng | Bẫy |
|---|---|---|
| **Broadcast join** | Một bảng đủ nhỏ để nhân bản sang mọi executor | Broadcast bảng quá lớn gây OOM |
| **AQE** (adaptive query execution) | Bật sẵn; xử lý skew join, gộp partition sau shuffle | Không thay được việc sửa skew nghiêm trọng bằng salting |
| **Partition tuning** | `spark.sql.shuffle.partitions` quá cao → nhiều task tí hon; quá thấp → spill | Không có con số vạn năng; đo rồi chỉnh |
| **High concurrency** | Nhiều notebook cùng một user, cấu hình tương thích → dùng lại session | Không chia sẻ giữa hai user; không đi cùng Autotune |
| **Native Execution Engine (NEE)** | Tăng tốc vectorized (Velox/Gluten, C++) | Bật ở cấp **Environment** (Spark compute → Acceleration) |

> ⚠️ **Autotune** là **Preview, tắt mặc định**, và chỉ tương thích **Runtime 1.2** (đã hết hỗ trợ; không bật được trên 1.3/2.0); không chạy khi bật high concurrency. Đáp án "bật Autotune cho production Runtime mới nhất" là bẫy.

**Nhận diện skew và spill trên Spark UI — đọc đúng chỗ:**

```text
Spark UI → Stages → chọn stage chậm → bảng "Summary Metrics for Tasks"
    Duration:  Min | 25th | Median | 75th | Max
               2s  | 2s   | 3s     | 4s   | 30 phút   ← Max lệch hẳn = SKEW
    Spill (memory) / Spill (disk):  > 0  ← thiếu RAM cho shuffle
```

## G. WAREHOUSE & EVENTHOUSE — GẠCH ĐẦU DÒNG PHẢI THUỘC

**Warehouse:**

- Đo trước bằng **Query Insights**: `queryinsights.exec_requests_history`, `frequently_run_queries`, `long_running_queries`; DMV `sys.dm_exec_requests`, `sys.dm_exec_sessions`.
- **Statistics**: tự động, nhưng sau bulk load lớn nên `UPDATE STATISTICS` thủ công.
- **Kiểu dữ liệu**: tránh `VARCHAR(MAX)`/kiểu quá rộng — bộ nhớ cấp theo khai báo, không theo dữ liệu thực.
- **Warehouse không hỗ trợ materialized view.** Materialized **Lake** View là tính năng của Lakehouse/Spark.
- Load bằng **CTAS / `COPY INTO`**, tránh `INSERT` từng dòng.

**Eventhouse/KQL:**

- **Caching policy (hot cache)** quyết định **tốc độ**; **retention policy** quyết định **giữ bao lâu**. Nhầm hai cái này là lỗi kinh điển.
- **Materialized view** trong KQL để dựng sẵn kết quả tổng hợp hay dùng.
- **Update policy** để biến đổi ngay lúc ingest; nhớ giới hạn không join khi source có streaming ingestion policy.
- **Partitioning policy** cho bảng rất lớn; **query acceleration** cho OneLake shortcut khi cần tốc độ mà vẫn zero-copy.

## H. ACTIVE RECALL — 14 CÂU (che đáp án, nói thành tiếng)

<details><summary><b>1.</b> Bước đầu tiên khi đề mô tả "chậm" là gì?</summary>

**Chẩn đoán**, không phải tối ưu. Xác định vấn đề nằm ở một job hay ở cả capacity, rồi mở đúng công cụ (Spark UI / Query Insights / Capacity Metrics / activity run detail).
</details>

<details><summary><b>2.</b> `OPTIMIZE` chạy được từ SQL analytics endpoint không?</summary>

**Không.** Chỉ chạy từ Spark (notebook, Spark job, Lakehouse maintenance UI) hoặc Lakehouse Maintenance activity trong pipeline.
</details>

<details><summary><b>3.</b> `OPTIMIZE` có làm giảm dung lượng lưu trữ không?</summary>

**Không** — nó tạo file mới đã gộp, file cũ vẫn giữ cho time travel nên dung lượng còn tăng. Muốn thu hồi phải `VACUUM`.
</details>

<details><summary><b>4.</b> V-Order đánh đổi cái gì, và mặc định ở workspace mới?</summary>

Ghi chậm hơn khoảng 15% để đọc nhanh hơn (đặc biệt Direct Lake), nén tốt hơn. **Tắt mặc định ở workspace mới**.
</details>

<details><summary><b>5.</b> Ba nguyên nhân phổ biến của Direct Lake fallback?</summary>

RLS/CLS/DDM ở tầng SQL endpoint; model trỏ vào view; bảng vượt guardrail SKU. Direct Lake on OneLake thì báo lỗi thay vì fallback.
</details>

<details><summary><b>6.</b> Transcoding và framing khác nhau thế nào?</summary>

Transcoding nạp **dữ liệu** cột vào cache VertiPaq khi query chạm lần đầu. Framing cập nhật **metadata** để chốt tập file Delta model được nhìn thấy tại thời điểm refresh.
</details>

<details><summary><b>7.</b> Vì sao Overwrite toàn bảng hại cho Direct Lake?</summary>

Nó thay toàn bộ file/Delta log → cột đang resident bị evict → model về cold → query đầu tiên chậm. Ưu tiên append/MERGE/incremental.
</details>

<details><summary><b>8.</b> Nhìn ở đâu để khẳng định có data skew?</summary>

Spark UI → Stages → Summary Metrics for Tasks: **Max duration lệch hẳn** so với median. Xử lý bằng salting, AQE skew join, repartition, broadcast.
</details>

<details><summary><b>9.</b> Spill là gì và sửa thế nào?</summary>

Shuffle/aggregation vượt RAM nên tràn xuống disk. Sửa bằng cách giảm dữ liệu shuffle (lọc/chiếu sớm), chỉnh số shuffle partitions, tránh cột thừa, tăng node size khi thật cần.
</details>

<details><summary><b>10.</b> Tăng `parallelCopies` lên tối đa có luôn nhanh hơn không?</summary>

Không. Throughput ≈ min(source, network, destination, copy compute). Nếu source/sink đã bão hoà thì tăng song song chỉ gây throttle và lỗi.
</details>

<details><summary><b>11.</b> Warehouse: khi nào chạy `UPDATE STATISTICS` thủ công?</summary>

Sau bulk load rất lớn hoặc khi phân phối dữ liệu đổi đột ngột, khiến optimizer chọn plan lệch.
</details>

<details><summary><b>12.</b> Query KQL chậm với dải thời gian dài — chỉnh policy nào?</summary>

**Caching policy (hot cache)**, không phải retention. Retention chỉ quyết định giữ dữ liệu bao lâu.
</details>

<details><summary><b>13.</b> Ba giai đoạn throttling theo thứ tự?</summary>

Interactive delay → interactive rejection → background rejection. Trước đó có overage protection cho phép vay capacity tương lai.
</details>

<details><summary><b>14.</b> Vì sao "scale-up SKU" không phải đáp án mặc định cho mọi câu hỏi chậm?</summary>

Vì nó không sửa nguyên nhân khi vấn đề là skew, small files, statistics lệch hay fallback. Chỉ hợp lý khi chẩn đoán cho thấy capacity thật sự thiếu.
</details>

## I. MINI-LAB 90 PHÚT — TẠO TRIỆU CHỨNG RỒI CHỮA

1. **Tạo small file problem:** ghi 200 lần, mỗi lần vài dòng, vào một bảng Delta. Đếm số file. Chạy một query aggregate, ghi lại thời gian.
2. Chạy `OPTIMIZE`, đếm lại file, chạy lại query → so sánh thời gian. Sau đó `DESCRIBE HISTORY` để xem operation.
3. Chạy `VACUUM ... RETAIN 168 HOURS`, kiểm tra dung lượng trước/sau. Giải thích bằng lời vì sao `OPTIMIZE` không giảm dung lượng.
4. **Tạo skew:** dựng bảng có 90% dòng cùng một key, join với bảng khác. Mở Spark UI → Stages, chỉ ra ô số liệu chứng minh skew. Áp salting hoặc broadcast, đo lại.
5. **Direct Lake:** tạo semantic model Direct Lake trên bảng Gold. Chạy `EVALUATE TABLETRAITS()` xem `DirectLakeFallbackInfo`. Thêm RLS ở SQL endpoint → chạy lại → quan sát thay đổi. Gỡ RLS ra.
6. **Warehouse:** load một bảng lớn, chạy query trước và sau `UPDATE STATISTICS`; đọc `queryinsights.long_running_queries`.
7. **Capacity:** mở Capacity Metrics App, tìm timepoint tiêu thụ cao nhất trong 14 ngày, xác định operation nào gây ra, phân loại interactive hay background.

**Ngưỡng đạt:** che cột cuối của bảng mục B và tự nói đúng biện pháp cho ≥13/15 triệu chứng; trả lời đúng ≥13/14 câu ở mục H.

---

# ⚡ CẬP NHẬT 05/11/2026 — OPTIMIZE PERFORMANCE

## U1. Sáu bullet chính thức — không đổi

Lakehouse table · pipeline · data warehouse · eventstreams & eventhouses · Spark performance · query performance.

## U2. Runtime 2.0 (GA, mặc định) — default ảnh hưởng tới tối ưu

| Cấu hình | Runtime 1.3 | **Runtime 2.0** | Hệ quả khi tối ưu |
|---|---|---|---|
| `spark.databricks.delta.optimizeWrite.enabled` | `true` | **không set** | Không mặc định có optimize write → theo dõi small files sau write/MERGE |
| `spark.databricks.delta.optimizeWrite.partitioned.enabled` | `false` | **`true`** | Bảng có partition được gộp file khi ghi |
| `spark.microsoft.delta.targetFileSize.adaptive.enabled` | `false` | **`true`** | Kích thước file đích tự điều chỉnh theo bảng |
| `spark.microsoft.delta.optimize.fast.enabled` | `false` | **`true`** | OPTIMIZE bỏ qua phần đã tối ưu, nhanh hơn |
| `spark.microsoft.delta.optimize.fileLevelTarget.enabled` | `false` | **`true`** | OPTIMIZE nhắm theo file |
| `spark.sql.files.maxPartitionBytes` | 128 MiB | **512 MiB** | Ít read partition hơn → nếu thiếu song song, giảm giá trị này |
| `spark.hadoop.parquet.block.size` | 1 GiB | **128 MiB** | Row group nhỏ hơn |
| `spark.databricks.delta.properties.defaults.enableDeletionVectors` | — | **`true`** | DELETE/UPDATE/MERGE nhanh hơn; cần OPTIMIZE định kỳ |
| `spark.sql.sources.v2.bucketing.enabled` | `false` | **`true`** | Storage-partitioned join có thể tránh shuffle |
| `spark.speculation.multiplier` / `quantile` | 1.5 / 0.75 | **3 / 0.9** | Speculative task ít kích hoạt hơn |
| `spark.sql.parquet.vorder.default` | `false` ở workspace mới | `false` ở workspace mới | V-Order vẫn phải **bật có chủ đích** cho bảng read-heavy |
| Spark Autotune | Không hỗ trợ | **Không hỗ trợ** | Chỉ Runtime 1.2 |

> Không học thuộc mọi con số. Ba điều phải nhớ: **(1)** Runtime 2.0 là đáp án mặc định cho Spark mới; **(2)** Autotune không phải đáp án; **(3)** feature Delta 4.2 chỉ an toàn khi chỉ Spark đọc bảng.

## U3. Tối ưu ở cấp capacity — workspace-level surge protection (Preview)

Khi triệu chứng là "**một** workspace (sandbox, dev) ăn hết CU làm các workspace khác bị throttling", đáp án mới là **workspace-level surge protection**: đặt ngưỡng % CU trong 24h trượt, workspace quan trọng đặt **Mission critical**. Khi triệu chứng là "background job dồn dập làm interactive chậm", đáp án vẫn là **capacity-level surge protection** (rejection/recovery threshold) hoặc dời lịch/scale SKU.

## U4. Warehouse — `QUALIFY` (Preview) giúp gọn truy vấn dedup/top-N

```sql
-- Top 3 sản phẩm doanh thu cao nhất mỗi danh mục, không cần CTE
SELECT CategoryKey, ProductKey, Revenue
FROM gold.ProductRevenue
QUALIFY ROW_NUMBER() OVER (PARTITION BY CategoryKey ORDER BY Revenue DESC) <= 3;
```

Đây là cải thiện **cú pháp**, không phải kỹ thuật tối ưu hiệu năng tự thân. Tối ưu Warehouse vẫn theo: **Query Insights → query plan → statistics → data clustering (Preview) → tránh `SELECT *`/cột rộng**.

## U5. Câu hỏi tình huống cập nhật

<details><summary><b>OP-U1.</b> Sau khi chuyển sang Runtime 2.0, job đọc bảng lớn tạo ít task hơn trước và cluster không dùng hết core. Cấu hình nào cần xem trước?</summary>

`spark.sql.files.maxPartitionBytes` — mặc định tăng từ 128 MiB lên **512 MiB** trên Runtime 2.0, nên số read partition giảm. Giảm giá trị (ví dụ 128m) cho job cần song song cao, rồi benchmark.
</details>

<details><summary><b>OP-U2.</b> Bảng Delta nhận MERGE mỗi giờ trên Runtime 2.0; số file nhỏ tăng dần. Hai việc nên làm?</summary>

(1) Lên lịch **OPTIMIZE** (fast optimize đã bật mặc định nên chi phí thấp hơn) — có thể dùng Lakehouse maintenance hoặc notebook; (2) cân nhắc bật **optimize write** cho bảng/phiên ghi (Runtime 2.0 không còn set mặc định) hoặc liquid clustering nếu filter thay đổi. **Không** dùng Autotune.
</details>

<details><summary><b>OP-U3.</b> Bảng Warehouse `FactSales` 2 tỷ dòng, dashboard lọc theo `OrderDate` và `CustomerKey`; bảng hiện không có clustering. Làm sao áp dụng data clustering?</summary>

Không `ALTER` được. Tạo bảng mới bằng **CTAS** `WITH (CLUSTER BY (OrderDate, CustomerKey))`, chuyển dữ liệu, đổi tên. Chấp nhận ingestion chậm hơn và tốn CU hơn; tính năng còn Preview.
</details>

<details><summary><b>OP-U4.</b> Ba workspace dùng chung F64. Workspace "sandbox" chạy notebook thử nghiệm làm report production bị trễ mỗi chiều. Biện pháp ít tác động nhất tới các workspace khác?</summary>

**Workspace-level surge protection (Preview)** với ngưỡng % CU hợp lý cho sandbox, workspace production đặt **Mission critical**. Nếu đề chỉ chấp nhận GA: tách sandbox sang capacity nhỏ riêng hoặc dùng capacity-level surge protection.
</details>
