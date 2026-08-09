# DP-700: Optimize Performance
## Monitor and Optimize an Analytics Solution (30–35%) — Phần 3/3


> [!IMPORTANT]
> **Bản hiệu chỉnh ngày 04/08/2026.** Nội dung được đối chiếu với blueprint DP-700 có hiệu lực từ **21/07/2026**. Microsoft cho biết phần lớn câu hỏi tập trung vào tính năng GA, nhưng tính năng Preview phổ biến vẫn có thể xuất hiện. Với các bảng “supported items”, runtime, limits và trạng thái Preview/GA, hãy kiểm tra Microsoft Learn lần cuối trước ngày thi.

> **Skills measured as of July 21, 2026**  
> Tài liệu chuẩn bị thi chứng chỉ **Microsoft Fabric Data Engineer Associate (DP-700)**  
> Tham chiếu: [Microsoft Learn — Study Guide DP-700](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)

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

### 1.1. Vấn đề Small Files

Mỗi lần `INSERT`, `MERGE`, hoặc `UPDATE` trên Delta table tạo ra file mới nhỏ. Tích lũy → **"small file problem"** → query chậm do phải scan nhiều files.

### 1.2. Bốn công cụ tối ưu chính

#### A. OPTIMIZE (File Compaction)

Hợp nhất nhiều small files thành ít large files hơn.

```sql
-- Basic OPTIMIZE
OPTIMIZE gold.fact_sales;

-- OPTIMIZE với V-Order (tối ưu read performance)
OPTIMIZE gold.fact_sales VORDER;

-- OPTIMIZE với Z-Order (co-locate related data)
OPTIMIZE gold.fact_sales
ZORDER BY (CustomerID, ProductID);
```

```python
# PySpark
spark.sql("OPTIMIZE gold.fact_sales VORDER")
```

**Khi nào chạy**: Sau batch MERGE/INSERT lớn, hoặc theo schedule (daily/weekly)

#### B. VACUUM (Cleanup Old Files)

Xóa files cũ không còn được tham chiếu bởi Delta transaction log.

```sql
-- VACUUM giữ lại files trong 7 ngày (default)
VACUUM gold.fact_sales;

-- VACUUM với retention period tùy chỉnh
VACUUM gold.fact_sales RETAIN 168 HOURS;  -- 7 ngày

-- Kiểm tra files sẽ bị xóa (dry run)
VACUUM gold.fact_sales DRY RUN;
```

> ⚠️ **Cảnh báo**: VACUUM xóa files vĩnh viễn. Sau VACUUM, **time travel** tới versions cũ hơn retention period sẽ KHÔNG thể.

#### C. V-Order (Write-time Optimization)

V-Order tối ưu cách dữ liệu được **viết** vào Parquet files — cải thiện encoding/compression → read performance tăng.

**Đặc điểm**:
- Áp dụng khi WRITE (thêm ~15% write overhead)
- Cải thiện read performance cho TẤT CẢ engines (Spark, SQL, Power BI Direct Lake)
- **Mặc định TẮT** cho workspace mới (phải enable thủ công)

**Cách enable**:
```python
# Enable V-Order cho specific write operation
df.write.format("delta") \
    .option("parquet.vorder.enabled", "true") \
    .mode("overwrite") \
    .saveAsTable("gold.fact_sales")
```

```sql
-- Apply V-Order qua OPTIMIZE
OPTIMIZE gold.fact_sales VORDER;
```

**Enable workspace-level**:
- Workspace Settings → Spark → Enable V-Order for writes

#### D. Liquid Clustering (2026 — Khuyến nghị)

**Liquid Clustering** là chiến lược data layout linh hoạt, thường được ưu tiên cho workload mới trên Runtime 2.0. Nó không “tự động thay thế” partitioning trong mọi trường hợp; partitioning vẫn có thể phù hợp cho concurrent-writer isolation và yêu cầu cụ thể.

**Ưu điểm so với Partitioning/Z-Order**:
| Tiêu chí | Partitioning | Z-Order | Liquid Clustering |
|----------|:-------------|:--------|:-----------------|
| **Flexibility** | Cố định khi create | Cố định | Có thể thay đổi |
| **Cardinality** | Low cardinality only | Medium | High cardinality |
| **Maintenance** | Manual | Manual | Incremental, tự động |
| **Multi-column** | Hỗ trợ | Hỗ trợ | Hỗ trợ (tối ưu hơn) |
| **Recommended** | Chỉ khi partitioning thực sự phù hợp | Hữu ích trên runtime cũ/established workflow | Ưu tiên cho nhiều workload mới trên Runtime 2.0; vẫn phải kiểm tra interoperability |

**Cách sử dụng**:
```sql
-- Tạo table mới với Liquid Clustering
CREATE TABLE gold.fact_sales
USING DELTA
CLUSTER BY (OrderDate, CustomerID)
AS SELECT * FROM silver.sales_clean;

-- Thêm Liquid Clustering vào table hiện có
ALTER TABLE gold.fact_sales
CLUSTER BY (OrderDate, CustomerID);

-- Chạy OPTIMIZE để apply clustering
OPTIMIZE gold.fact_sales;
```

```python
# PySpark/DeltaTable builder: tạo table có clustering metadata
from delta.tables import DeltaTable

(DeltaTable.create(spark)
 .tableName("gold.fact_sales")
 .addColumn("OrderDate", "DATE")
 .addColumn("CustomerID", "BIGINT")
 .addColumn("Amount", "DECIMAL(18,2)")
 .clusterBy("OrderDate", "CustomerID")
 .execute())
```

### 1.3. Lakehouse Maintenance Activity (Pipeline)

Tự động hóa maintenance bằng pipeline:

```
Pipeline: Daily_Maintenance
├── Lakehouse Maintenance Activity
│   ├── Lakehouse: SalesLakehouse
│   ├── Tables: gold.fact_sales, gold.dim_customer
│   ├── Operations:
│   │   ├── ✅ OPTIMIZE
│   │   ├── ✅ VACUUM (7 days retention)
│   │   └── ✅ V-Order
│   └── Schedule: Daily at 2:00 AM
```

### 1.4. Tóm tắt Lakehouse Optimization

| Vấn đề | Công cụ | Tác dụng |
|--------|---------|----------|
| Nhiều small files | **OPTIMIZE** | Compact thành large files |
| Files cũ tốn storage | **VACUUM** | Xóa orphaned files |
| Read performance chậm | **V-Order** | Tối ưu Parquet encoding |
| Query scan quá nhiều data | **Liquid Clustering** | File skipping dựa trên cluster keys |
| Manual maintenance burden | **Lakehouse Maintenance Activity** | Tự động hóa trong pipeline |

> 📌 **Điểm thi quan trọng**: 
> - OPTIMIZE ≠ VACUUM (OPTIMIZE compact files, VACUUM xóa files cũ)
> - V-Order tối ưu WRITE format, Liquid Clustering tối ưu DATA LAYOUT
> - Liquid Clustering thường là lựa chọn ưu tiên cho nhiều table mới trên Runtime 2.0; không bỏ qua compatibility và workload-specific partitioning needs

---

## 2. Optimize a Pipeline

### 2.1. Best Practices tối ưu Pipeline

| Kỹ thuật | Mô tả | Tác dụng |
|----------|--------|----------|
| **Parallel activities** | Chạy activities không phụ thuộc nhau song song | Giảm tổng thời gian |
| **Reduce Copy Activity scope** | Chỉ copy data cần thiết (filter, column projection) | Giảm data transfer |
| **Incremental load** | Chỉ load data thay đổi (watermark/CDC) | Giảm processing time/cost |
| **Optimize copy throughput** | Tune Intelligent throughput optimization, parallel copy và source partitioning; theo dõi used DIUs/parallel copies | Balance throughput vs capacity/source limits |
| **Avoid unnecessary activities** | Loại bỏ Lookup/Set Variable không cần thiết | Giảm overhead |
| **Parameterize pipelines** | Dùng parameters thay vì hardcode | Reusability, dễ maintain |
| **Modular design** | Chia pipeline lớn thành child pipelines | Dễ debug, retry từng phần |

### 2.2. Parallel Execution

```
Pipeline: Load_All_Tables
│
├── ForEach Activity (Sequential = false → PARALLEL)
│   Items: ["customers", "orders", "products"]
│   └── Execute Pipeline Activity → Child_Copy_Pipeline
│       Parameters: TableName = @item()
│
│ Kết quả: 3 tables load đồng thời thay vì tuần tự
```

### 2.3. Copy Activity Optimization

```
Source Query Optimization:
├── ❌ Sai: SELECT * FROM large_table
├── ✅ Đúng: SELECT col1, col2 FROM large_table WHERE date > '2026-01-01'
│
Copy throughput optimization:
├── Intelligent throughput optimization: Auto hoặc mức phù hợp connector
├── Degree of copy parallelism / source partitioning: tăng có kiểm soát
├── Theo dõi usedDataIntegrationUnits và usedParallelCopies trong output
│
Staging:
├── Enable Staging: Cải thiện performance khi copy giữa 2 sources khác nhau
└── Compression: Enable compression cho network transfer
```

### 2.4. Retry và Error Handling

```
Robust Pipeline Design:
├── Activity Level:
│   ├── Retry Count: 3
│   ├── Retry Interval: 30 seconds
│   └── Timeout: 1 hour
│
├── Pipeline Level:
│   ├── On Failure → Send notification
│   ├── On Success → Continue to next step
│   └── On Completion → Cleanup activities
│
└── Idempotency:
    ├── MERGE INTO thay vì INSERT (tránh duplicate khi retry)
    └── Watermark table để track progress
```

---

## 3. Optimize a Data Warehouse

### 3.1. Tính năng Performance chính (2026)

| Tính năng | Mô tả | Cấu hình |
|-----------|--------|----------|
| **Result-Set Caching** | Cache kết quả query lặp lại | Tự động (GA), không cần config |
| **Execution engine / query plan** | Tối ưu plan, statistics, caching và engine-supported acceleration | Không giả định GPU tự động cho mọi Warehouse query |
| **Proactive Statistics** | Tự động update column statistics | Tự động |
| **Incremental Statistics** | Update stats incrementally cho INSERT-heavy tables | Tự động |
| **Materialized Lake Views** | Persist complex query results dưới dạng Delta table | Cần tạo thủ công |

### 3.2. Result-Set Caching

```sql
-- Kiểm tra cache hit
SELECT 
    command,
    result_cache_hit,
    DATEDIFF(ms, start_time, end_time) AS duration_ms
FROM queryinsights.exec_requests_history
WHERE command LIKE '%fact_sales%'
ORDER BY start_time DESC;

-- Result: 
-- First run:  result_cache_hit = 0, duration = 5200ms
-- Second run: result_cache_hit = 1, duration = 50ms  ← 100x faster!
```

**Khi nào cache bị invalidated**:
- Table data thay đổi (INSERT/UPDATE/DELETE)
- Table schema thay đổi
- Security context thay đổi
- Cache timeout (automatic)

### 3.3. Materialized Lake Views (MLVs)

```sql
-- Tạo Materialized Lake View cho aggregation phức tạp
CREATE MATERIALIZED VIEW gold.mv_sales_summary AS
SELECT 
    d.Year,
    d.Quarter,
    p.Category,
    SUM(f.Revenue) AS TotalRevenue,
    COUNT(f.OrderID) AS OrderCount
FROM gold.fact_sales f
JOIN gold.dim_date d ON f.DateKey = d.DateKey
JOIN gold.dim_product p ON f.ProductKey = p.ProductKey
GROUP BY d.Year, d.Quarter, p.Category;
```

**Đặc điểm MLVs**:
- Tự động refresh khi source data thay đổi
- Persist dưới dạng Delta table trong OneLake
- Query optimizer tự động sử dụng MLV khi phù hợp
- Giảm compute cost cho repeated complex queries

### 3.4. Schema Design Optimization

```sql
-- ✅ Best Practices
-- 1. Dùng đúng data types (không dùng VARCHAR cho dates)
CREATE TABLE gold.fact_sales (
    OrderDate DATE,           -- ✅ Đúng: DATE type
    -- OrderDate VARCHAR(20), -- ❌ Sai: VARCHAR cho date
    Revenue DECIMAL(18,2),    -- ✅ Đúng: Precise decimal
    Quantity INT              -- ✅ Đúng: Integer
);

-- 2. Star Schema > Flat Table
-- Dimensional model (star) tối ưu hơn wide flat table

-- 3. Tránh SELECT *
SELECT OrderDate, Revenue, Quantity  -- ✅ Column projection
FROM gold.fact_sales
WHERE OrderDate >= '2026-01-01';     -- ✅ Predicate pushdown
```

### 3.5. Statistics Management

```sql
-- Kiểm tra statistics hiện tại
-- (Fabric tự động manage, nhưng có thể force update)
UPDATE STATISTICS gold.fact_sales;

-- Xem execution plan
EXPLAIN SELECT * FROM gold.fact_sales WHERE CustomerKey = 12345;
```

---

## 4. Optimize Eventstreams and Eventhouses

### 4.1. Optimize Eventhouse

#### Caching Policy (Hot vs Cold)

```kql
// Xem caching policy hiện tại
.show table SensorData policy caching

// Set hot cache = 30 ngày (data gần đây trên SSD)
.alter table SensorData policy caching hot = 30d

// Set hot cache cho toàn bộ database
.alter database MyDB policy caching hot = 14d
```

**Chiến lược Caching**:

| Loại data | Hot Cache | Lý do |
|-----------|-----------|-------|
| Real-time dashboard data | 7–30 ngày | Truy vấn thường xuyên, cần tốc độ |
| Historical analytics | 1–7 ngày | Ít truy vấn, tiết kiệm SSD |
| Compliance/Audit logs | 1 ngày | Lưu trữ lâu nhưng hiếm query |

#### Retention Policy

```kql
// Set retention = 365 ngày
.alter table SensorData policy retention softdelete = 365d

// Cho dev/test: retention ngắn để tiết kiệm
.alter table SensorData policy retention softdelete = 7d
```

#### Ingestion Optimization

```kql
// Batching policy: tối ưu how data gets batched for ingestion
.alter table SensorData policy ingestionbatching @'{
    "MaximumBatchingTimeSpan": "00:00:30",
    "MaximumNumberOfItems": 10000,
    "MaximumRawDataSizeMB": 256
}'

// Streaming ingestion (real-time, < 10 sec latency)
.alter table SensorData policy streamingingestion enable
```

### 4.2. Optimize Eventstream

| Kỹ thuật | Mô tả |
|----------|--------|
| **Filter early** | Thêm Filter transformation gần source để giảm data volume |
| **Minimize transformations** | Mỗi transformation thêm latency |
| **Choose right destination** | Eventhouse cho real-time, Lakehouse cho batch |
| **Monitor backlog** | Nếu backlog tăng → source quá nhanh cho destination |
| **Scale capacity** | Tăng CU nếu throughput không đủ |

### 4.3. KQL Query Optimization

```kql
// ❌ Sai: Query toàn bộ data
SensorData
| summarize avg(Temperature) by DeviceId

// ✅ Đúng: Thêm time filter
SensorData
| where EventTime > ago(1h)           // Giới hạn time range
| summarize avg(Temperature) by DeviceId

// ✅ Tốt hơn: Project chỉ columns cần thiết
SensorData
| where EventTime > ago(1h)
| project DeviceId, Temperature        // Column pruning
| summarize avg(Temperature) by DeviceId

// ✅ Best: Dùng materialized view cho repeated queries
.create materialized-view SensorHourlySummary on table SensorData {
    SensorData
    | summarize AvgTemp = avg(Temperature), MaxTemp = max(Temperature)
      by DeviceId, bin(EventTime, 1h)
}
```

---

## 5. Optimize Spark Performance

### 5.1. Ma trận Spark Optimization

| Vấn đề | Dấu hiệu | Giải pháp |
|--------|----------|-----------|
| **Slow joins** | Long stage duration, high shuffle | Use `broadcast()` cho small tables |
| **Data skew** | Few tasks much slower than others | Key salting, repartition |
| **Out of Memory** | OOM errors, high disk spill | Tăng executor memory, repartition |
| **Too many partitions** | Many tiny tasks (<1 sec each) | Giảm `shuffle.partitions` |
| **Too few partitions** | Few long-running tasks | Tăng `shuffle.partitions`, repartition |
| **Slow writes** | Write operations take long | Enable V-Order, use Delta format |
| **Full table scan** | Query reads all data | Liquid Clustering, partition pruning |

### 5.2. Join Optimization — Broadcast Join

```python
from pyspark.sql.functions import broadcast

# Scenario: Join fact table (100M rows) với dimension table (10K rows)

# ❌ Sai: Shuffle join (tốn network, slow)
result = fact_table.join(dim_table, "CustomerID")

# ✅ Đúng: Broadcast join (dim table gửi tới tất cả executors)
result = fact_table.join(broadcast(dim_table), "CustomerID")
```

**Cấu hình auto broadcast threshold**:
```python
# Mặc định: 10MB. Tables nhỏ hơn ngưỡng này tự động broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50MB")  # Tăng lên 50MB
```

### 5.3. Partition Optimization

```python
# Kiểm tra số partitions hiện tại
print(df.rdd.getNumPartitions())  # Ví dụ: 8

# Quá ít partitions → mỗi partition quá lớn → OOM
df = df.repartition(200)  # Tăng partitions

# Quá nhiều partitions → overhead → chậm
df = df.coalesce(50)  # Giảm partitions (không shuffle)

# Repartition theo key (tối ưu cho downstream joins)
df = df.repartition("CustomerID")

# Cấu hình shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "200")  # Default: 200
```

### 5.4. Adaptive Query Execution (AQE)

AQE tự động tối ưu Spark queries tại runtime:

| Feature | Mô tả |
|---------|--------|
| **Dynamic Partition Coalescing** | Gộp small partitions sau shuffle |
| **Skew Join Optimization** | Tự động split skewed partitions |
| **Dynamic Join Strategy** | Chuyển sort-merge join → broadcast join nếu phù hợp |

```python
# AQE mặc định enabled trong Fabric. Verify:
print(spark.conf.get("spark.sql.adaptive.enabled"))  # true

# Nếu cần enable thủ công:
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

### 5.5. Autotune (2026)

Fabric **Spark Autotune** tự động điều chỉnh:
- `spark.sql.shuffle.partitions`
- `spark.sql.autoBroadcastJoinThreshold`
- Dựa trên workload thực tế

**Cách enable**:
- Workspace Settings → Spark → Enable Autotune
- Hoặc trong notebook: `spark.conf.set("spark.ms.autotune.enabled", "true")`

### 5.6. Caching Strategy

```python
# Cache: Giữ DataFrame trong memory cho repeated access
# ✅ Cache khi df được dùng ≥ 2 lần
dim_customer = spark.table("gold.dim_customer")
dim_customer.cache()

# Dùng dim_customer nhiều lần
result1 = fact.join(dim_customer, "key")
result2 = another_fact.join(dim_customer, "key")

# Giải phóng cache khi xong
dim_customer.unpersist()

# ❌ KHÔNG cache DataFrames dùng 1 lần
# ❌ KHÔNG cache DataFrames quá lớn (tràn memory)
```

### 5.7. Environment-Level Configuration

Thay vì config trong mỗi notebook → config tại **Environment level**:

```
Fabric Environment: Production_Spark_Config
├── Spark Properties:
│   ├── spark.sql.shuffle.partitions = 200
│   ├── spark.sql.autoBroadcastJoinThreshold = 50MB
│   ├── spark.executor.memory = 8g
│   ├── parquet.vorder.enabled = true
│   └── spark.ms.autotune.enabled = true
├── Libraries:
│   ├── delta-spark==3.x
│   └── pandas-profiling==4.x
└── Assign to: All production notebooks
```

---

## 6. Optimize Query Performance

### 6.1. Tổng hợp tối ưu theo Engine

| Engine | Kỹ thuật tối ưu chính |
|--------|----------------------|
| **Spark SQL** | Broadcast join, partition pruning, Liquid Clustering, AQE |
| **T-SQL (Warehouse)** | Result-set caching, Statistics, MLVs, column projection |
| **KQL (Eventhouse)** | Time filter, materialized views, caching policy |
| **Power Query (Dataflow)** | Query folding, reduce steps, column selection |

### 6.2. Quy tắc vàng cho mọi Engine

```
Tối ưu Query Performance — 5 quy tắc vàng:

1️⃣  FILTER EARLY
    Thêm WHERE/filter càng sớm càng tốt → giảm data scan

2️⃣  SELECT ONLY NEEDED COLUMNS
    Tránh SELECT * → chỉ lấy columns cần thiết

3️⃣  USE APPROPRIATE JOINS
    Small table join → broadcast()
    Same key join → co-locate data (clustering)

4️⃣  OPTIMIZE DATA LAYOUT
    Lakehouse: OPTIMIZE + Liquid Clustering
    Warehouse: Statistics + MLVs
    Eventhouse: Caching policy

5️⃣  MONITOR AND ITERATE
    Spark UI, queryinsights, KQL diagnostics
    → Identify bottleneck → Fix → Verify
```

### 6.3. Common Anti-patterns

| Anti-pattern | Vấn đề | Fix |
|-------------|--------|-----|
| `SELECT *` | Scan tất cả columns | Column projection |
| No time filter (KQL) | Full table scan | `where EventTime > ago(1h)` |
| `df.collect()` on large DF | Driver OOM | Write to table, use limit() |
| `mode("overwrite")` for row-level incremental | Thay toàn table/partition thay vì chỉ upsert thay đổi | Use MERGE/CDC; overwrite vẫn tạo Delta version mới và history còn tới khi VACUUM |
| `%pip install` in pipeline | Blocked in orchestration | Use Fabric Environment |
| Manual partitioning | Inflexible, stale | Use Liquid Clustering |
| No OPTIMIZE after MERGE | Small files accumulate | Schedule OPTIMIZE |

---

## 7. Câu hỏi mô phỏng bài thi (Practice Questions)

### Question 1

**You have a Fabric Lakehouse table that has accumulated thousands of small Parquet files after weeks of incremental MERGE operations. Query performance has degraded by 10x.**

**Which command should you run FIRST to improve performance?**

A. `VACUUM gold.fact_sales`  
B. `OPTIMIZE gold.fact_sales`  
C. `DROP TABLE gold.fact_sales`  
D. `ALTER TABLE gold.fact_sales SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')`

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — `OPTIMIZE` compact small files thành larger files → giảm file count → cải thiện query performance ngay lập tức. Đây là bước ĐẦU TIÊN khi gặp small file problem.
> - **(A) Sai** — VACUUM xóa old files nhưng KHÔNG compact small files. Nó giải quyết storage cost, không phải query performance.
> - **(C) Sai** — Drop table mất toàn bộ dữ liệu.
> - **(D) Sai** — `autoOptimize.optimizeWrite` giúp writes tương lai nhưng KHÔNG fix files hiện có.

---

### Question 2

**You are creating a NEW Delta table in a Fabric Lakehouse. The table will store 500 million rows of sales data. Queries will frequently filter on `OrderDate` and `CustomerID` columns with high cardinality.**

**Which optimization technique should you apply to the table?**

A. Hash partitioning by CustomerID  
B. Range partitioning by OrderDate  
C. Liquid Clustering by (OrderDate, CustomerID)  
D. Z-Order by OrderDate

**✅ Correct Answer: C**

> **Giải thích**:
> - **(C) Đúng** — Liquid Clustering là phương pháp **khuyến nghị cho tables mới** (2026). Nó hỗ trợ **high cardinality** columns (OrderDate, CustomerID), tự động incremental maintenance, và có thể thay đổi cluster keys mà không cần recreate table.
> - **(A) Sai** — Hash partitioning KHÔNG được hỗ trợ native trong Delta Lake.
> - **(B) Sai** — Range partitioning phù hợp cho low cardinality (Year, Month), KHÔNG phù hợp cho high cardinality dates.
> - **(D) Sai** — Z-Order là legacy technique. Liquid Clustering thay thế Z-Order cho tables mới.

---

### Question 3

**You need to optimize a Spark notebook that joins a 100 million row fact table with a 5,000 row dimension table. The join currently takes 15 minutes.**

**Which optimization should you apply?**

A. Repartition the fact table to match the dimension table partitions  
B. Use `broadcast()` hint on the dimension table  
C. Increase `spark.sql.shuffle.partitions` to 1000  
D. Convert the dimension table to Parquet format

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — `broadcast()` hint gửi toàn bộ dimension table (nhỏ) tới tất cả executors, loại bỏ shuffle hoàn toàn. 5,000 rows rất nhỏ → broadcast lý tưởng → join có thể giảm từ 15 phút xuống vài giây.
> - **(A) Sai** — Repartition fact table tốn thêm shuffle, không cải thiện.
> - **(C) Sai** — Tăng shuffle partitions THÊM overhead, không giải quyết root cause (shuffle itself).
> - **(D) Sai** — Delta/Lakehouse tables đã là Parquet format.

---

### Question 4

**A KQL query against an Eventhouse table that stores 90 days of IoT data suddenly becomes slow. The table's hot cache policy is set to `30d`. The query filters for the last 60 days.**

**What is the cause and how should you fix it?**

A. The data is corrupted — rebuild the table  
B. The query needs to read 30 days from cold storage — extend the hot cache policy to 60d or more  
C. The Eventstream is backlogged — clear the queue  
D. KQL does not support queries longer than 30 days

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — Hot cache = 30 ngày, nhưng query đọc 60 ngày. 30 ngày gần nhất từ SSD (nhanh), 30 ngày cũ hơn từ cold blob storage (chậm). Fix: `.alter table IoTData policy caching hot = 60d`
> - **(A) Sai** — Data không corrupted, chỉ ở tầng storage chậm hơn.
> - **(C) Sai** — Eventstream backlog ảnh hưởng ingestion, không ảnh hưởng query trên data đã ingested.
> - **(D) Sai** — KQL hỗ trợ queries trên bất kỳ time range nào.

---

### Question 5

**You are optimizing a Fabric Data Factory pipeline that copies data from 20 source tables. Currently, it copies tables sequentially and takes 2 hours to complete.**

**What is the most effective way to reduce the total pipeline duration?**

A. Increase the timeout for each Copy Activity  
B. Use a ForEach activity with Sequential mode set to FALSE to copy tables in parallel  
C. Combine all 20 tables into a single Copy Activity  
D. Schedule the pipeline to run more frequently

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — ForEach với Sequential = FALSE chạy multiple Copy Activities **đồng thời** (parallel). Nếu mỗi table mất ~6 phút tuần tự (20 × 6 = 120 phút), parallel có thể giảm xuống ~6–12 phút.
> - **(A) Sai** — Tăng timeout không tăng tốc, chỉ cho phép chạy lâu hơn trước khi fail.
> - **(C) Sai** — Không thể combine 20 tables khác nhau vào 1 Copy Activity.
> - **(D) Sai** — Chạy thường xuyên hơn không giảm duration mỗi lần chạy.

---

### Question 6

**You notice that the same complex T-SQL query in a Fabric Warehouse takes 5 seconds on the first run but only 50 milliseconds on subsequent runs. After an INSERT into the queried table, the next run takes 5 seconds again.**

**What feature is responsible for this behavior?**

A. Liquid Clustering  
B. Result-set caching  
C. Materialized Lake Views  
D. V-Order optimization

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — Result-set caching tự động cache kết quả query. Run đầu = compute full (5s). Run sau = trả kết quả từ cache (50ms). Khi data thay đổi (INSERT) → cache invalidated → run tiếp = compute full lại.
> - **(A) Sai** — Liquid Clustering cải thiện query thông qua data layout, không tạo 100x performance difference.
> - **(C) Sai** — MLVs phải được tạo thủ công, không tự động.
> - **(D) Sai** — V-Order cải thiện read nhưng không tạo hiệu ứng "first run slow, subsequent fast".

---

### Question 7

**You have enabled Spark Autotune in your Fabric workspace. A data engineer asks which Spark settings Autotune automatically adjusts.**

**Which TWO settings does Autotune optimize? (Select TWO)**

A. `spark.sql.shuffle.partitions`  
B. `spark.executor.instances`  
C. `spark.sql.autoBroadcastJoinThreshold`  
D. `spark.driver.memory`  
E. `spark.default.parallelism`

**✅ Correct Answers: A and C**

> **Giải thích**:
> - **(A) Đúng** — Autotune tự động điều chỉnh shuffle partitions dựa trên workload.
> - **(C) Đúng** — Autotune tự động điều chỉnh broadcast join threshold dựa trên table sizes.
> - **(B) Sai** — Executor instances thuộc cluster-level config, không phải Autotune.
> - **(D) Sai** — Driver memory là resource config, không phải query optimization.
> - **(E) Sai** — Default parallelism không phải target của Autotune.

---

### Question 8

**After running VACUUM on a Delta table with `RETAIN 0 HOURS`, a user tries to use time travel to query data from 2 days ago and receives an error.**

**What is the cause?**

A. Time travel is not supported in Fabric  
B. VACUUM deleted the old Parquet files needed for time travel  
C. The Delta transaction log was corrupted  
D. The user does not have permission for time travel queries

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — `VACUUM RETAIN 0 HOURS` xóa TẤT CẢ old files ngay lập tức. Time travel cần old Parquet files để reconstruct historical versions. Sau VACUUM → files bị xóa → time travel fails.
> - **(A) Sai** — Time travel IS supported trong Fabric Delta tables.
> - **(C) Sai** — VACUUM không corrupt transaction log (log vẫn intact, chỉ files bị xóa).
> - **(D) Sai** — Permission không liên quan tới file availability.

---

### Question 9

**You are creating a maintenance pipeline for your Fabric Lakehouse that should run OPTIMIZE and VACUUM on 15 Gold-layer tables every night.**

**What is the recommended approach?**

A. Write a notebook with OPTIMIZE and VACUUM commands for each table and run it manually  
B. Use the Lakehouse Maintenance Activity in a Data Factory pipeline  
C. Create 15 separate pipelines, one for each table  
D. Use Dataflow Gen2 to run maintenance operations

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — Lakehouse Maintenance Activity là feature built-in trong Fabric pipelines, thiết kế chuyên cho scheduled maintenance (OPTIMIZE, VACUUM, V-Order). No-code, schedule dễ dàng, và manage nhiều tables.
> - **(A) Sai** — Manual execution không scalable và dễ bị quên.
> - **(C) Sai** — 15 pipelines riêng lẻ phức tạp không cần thiết.
> - **(D) Sai** — Dataflow Gen2 dùng cho data transformation, không phải table maintenance.

---

### Question 10

**A Spark notebook has a stage where most of the 200 tasks complete in under 3 seconds, but 5 tasks take over 8 minutes each due to data skew on the `CustomerID` join key.**

**Which technique should you use to resolve this?**

A. Increase `spark.sql.shuffle.partitions` to 2000  
B. Use key salting to distribute the hot keys across more partitions  
C. Convert the join to a cross join  
D. Disable Adaptive Query Execution (AQE)

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — **Key salting** thêm random suffix vào hot keys (ví dụ: CustomerID "VIP001" → "VIP001_1", "VIP001_2", ... "VIP001_10"), phân tán data của 1 key sang nhiều partitions → xử lý parallel thay vì 1 task handle toàn bộ.
> - **(A) Sai** — Tăng partitions không giải quyết skew vì hot key vẫn nằm trong 1 partition duy nhất.
> - **(C) Sai** — Cross join tạo cartesian product → exponentially larger, chậm hơn.
> - **(D) Sai** — AQE **GIÚP** xử lý skew (skew join optimization), disable nó sẽ tệ hơn.

---

## 📋 TỔNG KẾT: Monitor and Optimize (30–35%)

### Quick Reference cho thi

| Keyword | Answer |
|---------|--------|
| "job status, failed/succeeded" | **Monitoring Hub** |
| "CU utilization, throttling" | **Capacity Metrics App** |
| "small files after MERGE" | **OPTIMIZE** |
| "remove old files, storage cost" | **VACUUM** |
| "new table, high cardinality columns" | **Liquid Clustering** |
| "slow read performance on Parquet" | **V-Order** |
| "pipeline scheduled failure email" | **Built-in Failure Notifications** |
| "Teams alert, complex conditions" | **Data Activator (Reflex)** |
| "join small table with large table" | **broadcast()** |
| "few tasks much slower (skew)" | **Key salting** |
| "Dataflow creator left org" | **Take Ownership** |
| "notebook library in pipeline" | **Fabric Environment** |
| "KQL query slow, old data" | **Extend hot cache policy** |
| "Eventhouse paused after scaling" | **Enable Always-On** |
| "query same SQL, second run fast" | **Result-set caching** |
| "scheduled OPTIMIZE/VACUUM" | **Lakehouse Maintenance Activity** |
| "Direct Lake stale data" | **Check "Keep data up to date" setting** |
| "T-SQL CREATE INDEX not supported" | **Use Liquid Clustering / V-Order** |
| "Spark OOM, disk spill" | **Increase executor memory, repartition** |
| "VACUUM then time travel fails" | **VACUUM deleted old files** |

---

*Tài liệu được tổng hợp dựa trên Skills Measured as of July 21, 2026*  
*Nguồn tham khảo: Microsoft Learn, Microsoft Fabric Documentation*


---

## 8. BỔ SUNG ÔN THI 2026: OPTIMIZATION DECISION TREE

### 8.1 Lakehouse

- Nhiều small files → `OPTIMIZE`/compaction.
- Query filter thường xuyên → file skipping bằng liquid clustering hoặc Z-Order tùy runtime/interoperability.
- Read-heavy cross-engine → cân nhắc V-Order.
- Storage phình do obsolete files → `VACUUM`, sau khi xác định time-travel retention.

### 8.2 Spark

```python
# Broadcast chỉ khi dimension thực sự nhỏ và phù hợp driver/executor memory
from pyspark.sql.functions import broadcast
result = fact.join(broadcast(dim), "ProductId", "left")

# Kiểm tra physical plan thay vì đoán
result.explain("formatted")
```

### 8.3 KQL

```kusto
Telemetry
| where Timestamp > ago(24h)           // filter sớm để tận dụng hot cache/extents
| project Timestamp, DeviceId, Temperature
| summarize P95 = percentile(Temperature, 95) by DeviceId, bin(Timestamp, 15m)
```

### 8.4 Câu hỏi tình huống bổ sung

**Một Delta table được nhiều engine Fabric cùng đọc. Runtime 2.0 liquid clustering feature bạn muốn dùng được ghi là experimental và không interoperable với mọi workload. Hành động tốt nhất?**

A. Bật ngay trên production table  
B. Benchmark trên bản sao, kiểm tra protocol/interoperability rồi mới áp dụng  
C. VACUUM RETAIN 0 HOURS  
D. Chuyển mọi user thành Admin

<details><summary>Đáp án</summary>

**B.** Tối ưu phải giữ compatibility và khả năng rollback; không chỉ chạy theo tính năng mới nhất.
</details>


---

## NGUỒN CHÍNH THỨC ĐÃ ĐỐI CHIẾU

- [DP-700 study guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)
- [V-Order](https://learn.microsoft.com/en-us/fabric/data-engineering/delta-optimization-and-v-order)
- [Liquid clustering](https://learn.microsoft.com/en-us/fabric/data-engineering/native-execution-engine-z-order-liquid-clustering)
- [VACUUM](https://learn.microsoft.com/en-us/fabric/data-engineering/delta-lake-vacuum)
- [Z-Order](https://learn.microsoft.com/en-us/fabric/data-engineering/delta-lake-z-order)

---

## 9. CÂU HỎI SCENARIO — OPTIMIZE PERFORMANCE

> Các câu hỏi dưới đây là câu tự soạn theo blueprint DP-700, **không phải câu hỏi thật hoặc exam dump**.

### Câu D3-OPT1 — OPTIMIZE vs VACUUM

**Bảng Delta có hàng nghìn small files (< 1 MB mỗi file) do nhiều micro-batch writes. Thao tác nào gộp các files lại để cải thiện read performance?**

A. `VACUUM` với retention 7 ngày  
B. `OPTIMIZE` (compaction)  
C. `DROP TABLE`  
D. `DESCRIBE HISTORY`  

<details><summary>Đáp án và giải thích</summary>

**B.** [`OPTIMIZE`](https://learn.microsoft.com/en-us/fabric/data-engineering/delta-optimization-and-v-order) thực hiện **compaction** — gộp nhiều small files thành ít files lớn hơn (target khoảng 1 GB) để tăng tốc độ đọc. `VACUUM` xoá các old Delta version files (dữ liệu đã bị xoá/cập nhật) để giải phóng storage, không gộp files active.

</details>

---

### Câu D3-OPT2 — Liquid Clustering vs Z-Order

**Table thường xuyên được query theo nhiều filter column kết hợp khác nhau, và data đến liên tục (incremental writes). Pattern nào phù hợp hơn?**

A. Partition theo một cột cố định (ví dụ: năm)  
B. Liquid clustering (linh hoạt, không yêu cầu full rewrite khi thay đổi)  
C. Z-Order với cột cố định  
D. VACUUM ngay sau mỗi write  

<details><summary>Đáp án và giải thích</summary>

**B.** [Liquid clustering](https://learn.microsoft.com/en-us/fabric/data-engineering/native-execution-engine-z-order-liquid-clustering) phù hợp hơn khi:
- Query patterns thay đổi hoặc có nhiều cột filter khác nhau
- Data incremental — không yêu cầu full rewrite khi thay đổi clustering keys
- Muốn tránh maintenance phức tạp của partition scheme

Z-Order tốt cho cột filter cố định và ít thay đổi. Liquid clustering là evolution của Z-Order.

</details>

---

### Câu D3-OPT3 — Spark AQE Và Skew

**Spark job bị skew nghiêm trọng do hot keys trong join operation. AQE (Adaptive Query Execution) được bật. AQE giúp ích gì ở đây?**

A. AQE tự động áp dụng **skew join optimization** — chia nhỏ hot partitions  
B. AQE không giúp gì với skew  
C. AQE tắt toàn bộ shuffles  
D. AQE thay thế tất cả joins bằng MERGE  

<details><summary>Đáp án và giải thích</summary>

**A.** [AQE skew join optimization](https://learn.microsoft.com/en-us/fabric/data-engineering/spark-job-definition) tự động phát hiện và chia nhỏ **skewed partitions** thành nhiều sub-tasks nhỏ hơn, giảm straggler task time. Tuy nhiên với skew **cực đoan**, có thể cần bổ sung **key salting** thủ công bên cạnh AQE.

</details>

---

### Câu D3-OPT4 — KQL Hot Cache

**KQL query thường filter trên dữ liệu 7 ngày gần nhất, nhưng table lưu 2 năm historical data. Tối ưu nào phù hợp?**

A. Tăng hot cache period thành 7 ngày cho table đó  
B. Xoá toàn bộ data cũ hơn 7 ngày  
C. Chuyển sang VACUUM  
D. Tắt V-Order trên table  

<details><summary>Đáp án và giải thích</summary>

**A.** [Hot cache](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/management/cache-policy) trong Eventhouse/KQL database cho phép giữ data gần đây trong SSD/RAM — query trên hot data nhanh hơn đáng kể so với cold data. Đặt `hot cache = 7d` phù hợp với access pattern mà không cần xoá historical data.

</details>

---

### Câu D3-OPT5 — Pipeline Copy Activity Throughput

**Copy activity cần sao chép 5 TB từ ADLS Gen2 vào Lakehouse nhưng chạy quá chậm (đơn luồng). Cách tăng throughput mà **ít thay đổi nhất**?**

A. Bật **Parallel copy** với Data Integration Units (DIU) tăng cao  
B. Giảm số DIU xuống tối thiểu  
C. Tắt staging  
D. Dùng deployment pipeline để copy  

<details><summary>Đáp án và giải thích</summary>

**A.** [Parallel copy](https://learn.microsoft.com/en-us/fabric/data-factory/copy-data-activity#performance-optimization-tips) trong Copy activity cho phép đọc và ghi song song với nhiều luồng. Tăng DIU (Data Integration Units) phân bổ thêm compute cho copy — đây là cách nhanh nhất và ít xâm phạm nhất để tăng throughput. Cũng cần xem xét bật **partition option** để chia source thành phần nhỏ copy song song.

</details>

---

## 10. LAB THỰC HÀNH — OPTIMIZE DELTA TABLES

### Lab 7 — OPTIMIZE, V-Order Và VACUUM

**Mục tiêu:** Thực hành các lệnh tối ưu hoá Delta table — compaction, V-Order encoding, và cleanup.

**Bước 1: Tạo bảng lab với nhiều small files**

```python
from pyspark.sql import functions as F

# Tắt tạm auto-compact để mô phỏng small files
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "false")

# Ghi dữ liệu giả lập — nhiều lần append tạo nhiều small files
for day in range(1, 8):
    df = spark.createDataFrame(
        [(i, f"Device_{i % 10}", round(20 + i * 0.01, 2), f"2026-08-{day:02d}")
         for i in range(1, 1001)],
        ["RecordId", "DeviceId", "Value", "Date"]
    )
    df.write.format("delta").mode("append").saveAsTable("lab.telemetry_raw")

# Kiểm tra số files hiện tại
spark.sql("DESCRIBE DETAIL lab.telemetry_raw").select("numFiles", "sizeInBytes").show()
```

**Bước 2: Chạy OPTIMIZE + V-Order**

```python
# OPTIMIZE: compaction small files (gộp thành target size ~1GB)
# V-Order: sắp xếp dữ liệu để tăng tốc đọc bởi Parquet reader
spark.sql("""
    OPTIMIZE lab.telemetry_raw
    VORDER                        -- Bật V-Order encoding
    ZORDER BY (DeviceId, Date)    -- Sắp xếp co-locate theo 2 cột filter thường dùng
""")

# So sánh sau OPTIMIZE
spark.sql("DESCRIBE DETAIL lab.telemetry_raw").select("numFiles", "sizeInBytes").show()
```

**Bước 3: Xem lịch sử và chạy VACUUM**

```python
# Xem lịch sử thay đổi (Delta transaction log)
spark.sql("DESCRIBE HISTORY lab.telemetry_raw").select(
    "version", "timestamp", "operation", "operationParameters"
).show(10, truncate=False)

# VACUUM: xoá files của versions cũ để giải phóng storage
# Mặc định: retain 7 ngày (168 giờ) — KHÔNG nên set dưới 7 ngày trên production
spark.sql("VACUUM lab.telemetry_raw RETAIN 168 HOURS")
# Sau VACUUM: time travel về version trước 7 ngày sẽ không còn hoạt động
```

**Tóm tắt kết quả mong đợi:**

| Thao tác | Mục đích | Ảnh hưởng |
|---------|---------|----------|
| `OPTIMIZE` | Gộp small files | Giảm file count, tăng read speed |
| `VORDER` | Encoding tối ưu | Tăng compression, tăng tốc đọc Power BI/Warehouse |
| `ZORDER BY` | Co-locate data | Giảm files cần đọc khi filter theo cột đó |
| `VACUUM 168H` | Xoá files cũ | Giải phóng storage, mất time travel cũ |

**Bảng so sánh các kỹ thuật optimization:**

| Kỹ thuật | Use case | Trade-off |
|---------|---------|----------|
| Partitioning | Low-cardinality, range filter | Quá nhiều partition → small files mới |
| Z-Order | High-cardinality, cột filter cố định | Cần full rewrite khi đổi key |
| Liquid clustering | Multi-dimensional filter, incremental | Cần Native Execution Engine |
| V-Order | Đọc bởi Fabric engines (Power BI, Warehouse) | Thêm thời gian write |

> [!TIP]
> Tham khảo: [Delta Lake optimization guide](https://learn.microsoft.com/en-us/fabric/data-engineering/delta-optimization-and-v-order)

