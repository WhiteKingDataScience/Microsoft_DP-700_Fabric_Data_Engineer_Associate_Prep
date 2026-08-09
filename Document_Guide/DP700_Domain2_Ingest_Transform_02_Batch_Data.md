# DP-700: Ingest and Transform Batch Data
## Ingest and Transform Data (30–35%) — Phần 2/3


> [!IMPORTANT]
> **Bản hiệu chỉnh ngày 09/08/2026.** Nội dung được đối chiếu với blueprint DP-700 có hiệu lực từ **21/07/2026**. Microsoft cho biết phần lớn câu hỏi tập trung vào tính năng GA, nhưng tính năng Preview phổ biến vẫn có thể xuất hiện. Với các bảng “supported items”, runtime, limits và trạng thái Preview/GA, hãy kiểm tra Microsoft Learn lần cuối trước ngày thi.

> **Skills measured as of July 21, 2026**  
> Tài liệu chuẩn bị thi chứng chỉ **Microsoft Fabric Data Engineer Associate (DP-700)**  
> Tham chiếu: [Microsoft Learn — Study Guide DP-700](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)

---

## MỤC LỤC

1. [Choose an Appropriate Data Store](#1-choose-an-appropriate-data-store)
2. [Choose Between Dataflows Gen2, Notebooks, KQL, and T-SQL](#2-choose-between-dataflows-gen2-notebooks-kql-and-t-sql)
3. [Create and Manage OneLake Shortcuts](#3-create-and-manage-onelake-shortcuts)
4. [Implement Mirroring](#4-implement-mirroring)
5. [Ingest Data by Using Pipelines](#5-ingest-data-by-using-pipelines)
6. [Transform Data by Using PySpark, SQL, and KQL](#6-transform-data-by-using-pyspark-sql-and-kql)
7. [Denormalize Data](#7-denormalize-data)
8. [Group and Aggregate Data](#8-group-and-aggregate-data)
9. [Handle Duplicate, Missing, and Late-Arriving Data](#9-handle-duplicate-missing-and-late-arriving-data)
10. [Câu hỏi mô phỏng bài thi (Practice Questions)](#10-câu-hỏi-mô-phỏng-bài-thi-practice-questions)

---

## 1. Choose an Appropriate Data Store

### 1.1. Bảng so sánh các Data Store trong Fabric

| Tiêu chí | **Lakehouse** | **Warehouse** | **Eventhouse** | **SQL Database** |
|----------|:-------------|:-------------|:--------------|:----------------|
| **Ngôn ngữ chính** | Spark (PySpark/Scala) + SQL | T-SQL | KQL | T-SQL |
| **Kiểu dữ liệu** | Structured, semi-structured, unstructured | Structured (relational) | Time-series, logs, telemetry | Structured (transactional) |
| **Định dạng lưu trữ** | Delta Lake (Parquet) | Delta Lake | Column store (Kusto) | SQL Database format |
| **Latency** | Phút (batch/micro-batch) | Phút (batch) | Giây (real-time) | Giây (transactional) |
| **Concurrency** | Trung bình | Cao | Cao | Cao |
| **Use Case chính** | Data Engineering, ML, Data Lake | Enterprise BI, Star Schema, Reporting | IoT, Streaming, Log Analytics | OLTP + Analytics hybrid |
| **Medallion Architecture** | ✅ Lý tưởng | ⚠️ Có thể nhưng không lý tưởng | ❌ Không phù hợp | ❌ Không phù hợp |
| **Direct Lake (Power BI)** | ✅ Hỗ trợ | ✅ Hỗ trợ | ❌ Không trực tiếp | ❌ Không trực tiếp |

### 1.2. Quy tắc chọn Data Store (Decision Framework)

```
Bạn cần xử lý loại dữ liệu nào?
│
├── Streaming / Real-time (< 1 giây latency)?
│   └── ✅ EVENTHOUSE (KQL Database)
│
├── Transactional (CRUD operations)?
│   └── ✅ SQL DATABASE
│
├── Structured data cho BI/Reporting, đội ngũ biết SQL?
│   └── ✅ WAREHOUSE
│
├── Multi-format data (CSV, JSON, Parquet), Data Engineering, ML?
│   └── ✅ LAKEHOUSE
│
└── Kết hợp nhiều use case?
    └── ✅ Dùng LAKEHOUSE cho ingestion + WAREHOUSE cho serving
```

### 1.3. Khi nào dùng Lakehouse vs Warehouse?

| Câu hỏi | Lakehouse | Warehouse |
|----------|-----------|-----------|
| Đội ngũ dùng PySpark/Python? | ✅ | ❌ |
| Đội ngũ dùng T-SQL thuần? | ⚠️ (SQL endpoint có giới hạn) | ✅ |
| Cần xử lý JSON, semi-structured? | ✅ | ❌ |
| Cần multi-table transactions? | ❌ | ✅ |
| Cần stored procedures? | ❌ | ✅ |
| Cần Machine Learning? | ✅ | ❌ |
| Cần Medallion Architecture? | ✅ | ⚠️ |

> 📌 **Điểm thi**: Kiến trúc phổ biến nhất là **Lakehouse cho ingestion/transformation** (Bronze → Silver) và **Warehouse hoặc Lakehouse Gold layer cho serving** (BI/Reporting).

---

## 2. Choose Between Dataflows Gen2, Notebooks, KQL, and T-SQL

### 2.1. Ma trận quyết định

| Tiêu chí | **Dataflows Gen2** | **Notebooks (Spark)** | **T-SQL** | **KQL** |
|----------|:-------------------|:---------------------|:----------|:--------|
| **Phong cách** | Low-code (Power Query) | Pro-code (Python/Scala) | SQL thuần | Kusto Query |
| **Người dùng** | Citizen Developer, Analyst | Data Engineer, Data Scientist | DBA, SQL Developer | Analytics Engineer |
| **Quy mô dữ liệu** | Nhỏ → Trung bình | Trung bình → Rất lớn | Trung bình → Lớn | Lớn (time-series) |
| **Data connectors** | Nhiều connector qua Power Query | JDBC/files/APIs và OneLake | Relational sources | Eventhouse/external tables/streaming sources |
| **Cost efficiency** | Trung bình | Cao (1 session xử lý nhiều) | Cao | Cao |
| **Real-time** | ❌ | ⚠️ (Structured Streaming) | ❌ | ✅ |
| **Destination** | Lakehouse, Warehouse | Lakehouse | Warehouse | Eventhouse |

### 2.2. Khi nào chọn Dataflows Gen2?

✅ **Chọn Dataflows Gen2 khi**:
- Logic transformation đơn giản (filter, rename, merge columns)
- Kết nối tới nguồn dữ liệu external (SharePoint, Excel, Web API)
- Người dùng không biết code (citizen developer)
- Transformation phù hợp với Power Query, query folding và capacity/SLA; không quyết định chỉ bằng số row
- Cần giao diện visual drag-and-drop

❌ **KHÔNG chọn Dataflows Gen2 khi**:
- Không dùng ngưỡng row cố định; chọn Notebook khi cần distributed compute, custom code, stateful/complex transformation hoặc Spark ecosystem.
- Logic phức tạp (window functions, ML) → chọn Notebook
- Real-time processing → chọn KQL hoặc Spark Streaming

### 2.3. Khi nào chọn Notebook (Spark)?

✅ **Chọn Notebook khi**:
- Data lớn, cần distributed processing
- Logic transformation phức tạp (custom functions, ML)
- Xây dựng Medallion Architecture (Bronze → Silver → Gold)
- Cần `MERGE INTO` cho incremental loads
- Streaming data (Spark Structured Streaming)
- Cần tái sử dụng code, version control

### 2.4. Khi nào chọn T-SQL?

✅ **Chọn T-SQL khi**:
- Làm việc trong Fabric Warehouse
- Đội ngũ có expertise SQL
- Cần stored procedures, views, functions
- Multi-table transactions
- Transformation trên dữ liệu đã structured sẵn

### 2.5. Khi nào chọn KQL?

✅ **Chọn KQL khi**:
- Dữ liệu streaming real-time (IoT, logs, telemetry)
- Làm việc trong Eventhouse / KQL Database
- Cần time-series analytics
- Query trên semi-structured data (JSON) nhanh

> 📌 **Tip thi**: Câu hỏi thường cho scenario → bạn phải chọn công cụ phù hợp. Nhớ: **Dataflows Gen2 = low-code/Power Query** | **Notebook = pro-code/distributed Spark** | **T-SQL = relational SQL** | **KQL = Eventhouse/time-series**

---

## 3. Create and Manage OneLake Shortcuts

### 3.1. Khái niệm
Shortcut là object trong OneLake trỏ đến target path khác, giúp tránh edge copy/staging không cần thiết.

**Exam keyword:** `do not copy`, `minimize data movement`, `single source of truth` → nghĩ Shortcut.

### 3.2. Nguồn hiện hành cần biết
Tính đến 08/2026, Microsoft tài liệu hóa internal OneLake, ADLS Gen2, Azure Blob, Amazon S3/S3-compatible, Google Cloud Storage, Dataverse, OneDrive for Business/enterprise SharePoint và các nguồn khác trong support matrix.

### 3.3. Lakehouse Tables vs Files

| Target | Shortcut location | Cách dùng |
|---|---|---|
| Delta table | `Tables` | discover/query như table |
| Folder/files | `Files` | Spark đọc bằng path |

```python
df=spark.read.format('parquet').load('Files/external_sales/2026/')
df.write.format('delta').mode('overwrite').saveAsTable('silver_sales')
```

### 3.4. KQL database/Eventhouse
Shortcut được query như external table:

```kusto
external_table('SalesShortcut')
| where OrderTime > ago(1d)
| summarize Revenue=sum(Amount) by bin(OrderTime,1h)
```

Nếu query external Delta shortcut thường xuyên → cân nhắc Query acceleration.

### 3.5. Authentication
Không học rule “mọi shortcut đều pass-through user identity”. Auth phụ thuộc source. Ví dụ Azure Blob hỗ trợ Organizational account, Service principal, Workspace identity và SAS; SharePoint/OneDrive có requirement riêng.

### 3.6. Decision

```text
Storage/OneLake, không muốn copy     → Shortcut
Operational DB, managed replication  → Mirroring
Guided full/incremental/CDC movement → Copy Job
Multi-step orchestration             → Pipeline
```

## 4. Implement Mirroring

### 4.1. Hai loại cần phân biệt
1. **Database mirroring**: replicate data/change vào OneLake Delta.
2. **Metadata mirroring**: đồng bộ metadata và reference/shortcut underlying data, ví dụ Azure Databricks catalog.

### 4.2. Nguồn Mirroring hiện hành (08/2026)
Microsoft hiện tài liệu hóa Azure SQL Database, Azure SQL MI, Azure Cosmos DB, Azure Database for PostgreSQL, Azure Database for MySQL (Preview), SQL Server, Snowflake, Google BigQuery (Preview), Oracle, SAP, Azure Databricks metadata mirroring, Dremio metadata mirroring (Preview), Fabric SQL database và Open Mirroring.

> [!IMPORTANT]
> GA/Preview thay đổi nhanh; kiểm tra support matrix gần ngày thi.

### 4.3. Azure SQL Database limitation dễ ra đề
Azure SQL Database không thể mirrored nếu đã bật **CDC**, **Azure Synapse Link for SQL**, hoặc đã mirror vào Fabric workspace khác.

```text
Azure SQL + near-real-time + no CDC/Synapse Link → Mirroring
Azure SQL đã bật CDC + need change replication    → Copy Job CDC có thể phù hợp
```

### 4.4. Mirroring vs Shortcut vs Pipeline/Copy Job

| Requirement | Tool |
|---|---|
| managed near-real-time DB replication | Mirroring |
| zero-copy storage access | Shortcut |
| simple guided data movement | Copy Job |
| orchestration + conditions + transforms | Pipeline |

### 4.5. Extended capabilities
Delta Change Data Feed và Mirroring views là extended capabilities có billing/behavior riêng.

### 4.6. Bẫy
- Database mirroring **có data replication**, không phải zero-copy.
- Metadata mirroring mới dùng reference/shortcut underlying data.
- Source RLS/DDM/object permissions không mặc định được propagated sang replicated OneLake data.

## 5. Ingest Data by Using Pipelines

### 5.1. Pipeline là orchestrator
Dùng khi cần Copy/Notebook/Dataflow/Script cùng dependency, parameter, loop, error path, schedule/event trigger.

### 5.2. Copy Activity — write behavior phụ thuộc destination

**Lakehouse Table:** `Append`, `Overwrite`, `Upsert` + key columns.  
**Warehouse:** `Insert`, `Upsert` + key columns.

> [!IMPORTANT]
> Không học một danh sách write mode chung cho mọi connector. “Delete + Insert” không phải universal Copy Activity mode.

### 5.3. Copy Job — khác Copy Activity
Copy Job là standalone guided data movement item:
- Full copy
- Incremental watermark
- CDC nếu connector hỗ trợ
- Fabric quản lý last-successful-run state
- Append/Overwrite/Merge; SCD Type 2 trên supported CDC paths
- Audit columns
- Auto-partitioning (Preview) ở supported connectors

Copy Job còn có thể chạy **bên trong Pipeline qua Copy Job activity**.

### 5.4. Robust watermark pipeline

```text
Lookup OldWatermark
→ Lookup NewWatermark at source
→ Copy (Old, New]
→ Notebook/Script MERGE
→ SUCCESS: update control table = NewWatermark
```

```text
@concat(
  'SELECT * FROM dbo.Orders WHERE ModifiedDate > ''',
  activity('GetOldWatermark').output.firstRow.LastWatermark,
  ''' AND ModifiedDate <= ''',
  activity('GetNewWatermark').output.firstRow.NewWatermark,
  ''''
)
```

> Hai dấu nháy đơn liên tiếp (`''`) là cách escape dấu `'` bên trong chuỗi SQL của Fabric expression. Trong production nên truyền parameterized query khi connector hỗ trợ để giảm lỗi quoting/type.

### 5.5. Metadata-driven multi-table pattern

```sql
CREATE TABLE dbo.IngestionConfig(
 TableName varchar(128) NOT NULL,
 WatermarkColumn varchar(128) NULL,
 TargetTable varchar(128) NOT NULL,
 IsActive bit NOT NULL
);
```

`Lookup → ForEach → parameterized Copy/Notebook` giúp tránh tạo hàng chục activity thủ công.

### 5.6. Retry rule
Retry transient error; credential/schema/permission error phải sửa nguyên nhân. Mọi activity có thể retry nên target write cần idempotent hoặc source range cố định.

## 6. Transform Data by Using PySpark, SQL, and KQL

### 6.1. PySpark Transformations

#### Các phép biến đổi phổ biến

```python
from pyspark.sql.functions import *

# === FILTER ===
df_filtered = df.filter(col("Status") == "Active")

# === SELECT & RENAME ===
df_selected = df.select(
    col("CustomerID"),
    col("customer_name").alias("CustomerName"),
    upper(col("city")).alias("City")
)

# === JOIN ===
df_joined = orders.join(customers, "CustomerID", "left")

# === WINDOW FUNCTION ===
from pyspark.sql.window import Window

window_spec = Window.partitionBy("CustomerID").orderBy(desc("OrderDate"))
df_ranked = df.withColumn("RowNum", row_number().over(window_spec))
df_latest = df_ranked.filter(col("RowNum") == 1)

# === GROUP BY & AGGREGATE ===
df_agg = df.groupBy("ProductCategory") \
    .agg(
        sum("Revenue").alias("TotalRevenue"),
        count("OrderID").alias("OrderCount"),
        avg("UnitPrice").alias("AvgPrice")
    )

# === TYPE CASTING ===
df_cast = df.withColumn("Amount", col("Amount").cast("decimal(18,2)")) \
            .withColumn("OrderDate", to_date(col("OrderDate"), "yyyy-MM-dd"))

# === HANDLE NULLS ===
df_clean = df.fillna({"City": "Unknown", "Revenue": 0}) \
             .dropna(subset=["CustomerID"])

# === ADD COLUMNS ===
df_enriched = df.withColumn("Year", year(col("OrderDate"))) \
               .withColumn("IsHighValue", when(col("Revenue") > 1000, True).otherwise(False))
```

### 6.2. SQL Transformations (T-SQL trong Warehouse)

```sql
-- === CREATE VIEW cho transformation ===
CREATE VIEW silver.v_orders_cleaned AS
SELECT 
    OrderID,
    CustomerID,
    CAST(OrderDate AS DATE) AS OrderDate,
    ISNULL(Quantity, 0) AS Quantity,
    ISNULL(UnitPrice, 0) AS UnitPrice,
    Quantity * UnitPrice AS TotalAmount,
    YEAR(OrderDate) AS OrderYear,
    MONTH(OrderDate) AS OrderMonth
FROM bronze.orders
WHERE OrderID IS NOT NULL;

-- === CTAS (CREATE TABLE AS SELECT) ===
CREATE TABLE silver.orders_cleaned AS
SELECT 
    OrderID,
    CustomerID,
    CAST(OrderDate AS DATE) AS OrderDate,
    COALESCE(Quantity, 0) AS Quantity,
    COALESCE(UnitPrice, 0.00) AS UnitPrice
FROM bronze.orders
WHERE Status <> 'Cancelled';

-- === WINDOW FUNCTION trong T-SQL ===
SELECT 
    CustomerID,
    OrderDate,
    Revenue,
    ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY OrderDate DESC) AS RowNum,
    SUM(Revenue) OVER (PARTITION BY CustomerID) AS CustomerTotalRevenue,
    LAG(Revenue) OVER (PARTITION BY CustomerID ORDER BY OrderDate) AS PreviousRevenue
FROM silver.orders;
```

### 6.3. KQL Transformations (Eventhouse)

```kql
// === FILTER & PROJECT ===
SensorData
| where EventTime > ago(1h)
| where Temperature > 30
| project DeviceId, Temperature, Humidity, EventTime

// === AGGREGATE ===
SensorData
| summarize 
    AvgTemp = avg(Temperature),
    MaxTemp = max(Temperature),
    EventCount = count()
    by DeviceId, bin(EventTime, 5m)

// === JOIN ===
SensorData
| join kind=inner (DeviceMetadata) on DeviceId
| project DeviceId, DeviceName, Location, Temperature

// === PARSE JSON ===
RawEvents
| extend parsed = parse_json(Body)
| project 
    DeviceId = tostring(parsed.deviceId),
    Temperature = todouble(parsed.temperature),
    EventTime = todatetime(parsed.timestamp)

// === TIME SERIES ===
SensorData
| make-series AvgTemp = avg(Temperature) on EventTime from ago(24h) to now() step 1h by DeviceId
| render timechart
```

---

## 7. Denormalize Data

### 7.1. Tại sao Denormalize?

Trong bối cảnh Fabric, denormalization thường xảy ra ở tầng **Silver → Gold**:

| Tầng | State | Mô tả |
|------|-------|--------|
| Bronze | Raw | Giữ nguyên cấu trúc nguồn |
| Silver | Normalized | Cleaned, validated, vẫn normalized |
| **Gold** | **Denormalized** | **Flatten, star schema, sẵn sàng cho BI** |

### 7.2. Kỹ thuật Denormalization

#### A. Flatten Hierarchy (JOIN nhiều bảng)
```python
# Normalized: 3 bảng riêng
# employees -> departments -> locations

dim_employee = spark.table("silver.employees") \
    .join(spark.table("silver.departments"), "DeptID") \
    .join(spark.table("silver.locations"), "LocationID") \
    .select(
        "EmployeeID", "EmployeeName", "Title",
        "DepartmentName",                    # Từ bảng departments
        "City", "Country"                    # Từ bảng locations
    )
```

#### B. Pre-aggregate (Tính toán trước)
```python
# Thay vì query: SUM(Revenue) GROUP BY Product mỗi lần
# → Tạo bảng denormalized sẵn

product_summary = spark.table("silver.sales") \
    .groupBy("ProductID", "ProductName", "Category") \
    .agg(
        sum("Revenue").alias("TotalRevenue"),
        count("OrderID").alias("TotalOrders"),
        avg("Quantity").alias("AvgQuantity")
    )

product_summary.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.product_summary")
```

#### C. Embed Related Data (Nhúng dữ liệu liên quan)
```sql
-- Thay vì JOIN mỗi khi query
-- → Thêm trực tiếp cột từ bảng liên quan vào fact table

CREATE TABLE gold.fact_sales_enriched AS
SELECT 
    s.OrderID,
    s.OrderDate,
    s.Quantity,
    s.Revenue,
    c.CustomerName,    -- Nhúng từ DimCustomer
    c.City,
    p.ProductName,     -- Nhúng từ DimProduct
    p.Category,
    d.Year,            -- Nhúng từ DimDate
    d.Quarter
FROM silver.sales s
JOIN gold.dim_customer c ON s.CustomerID = c.CustomerID
JOIN gold.dim_product p ON s.ProductID = p.ProductID
JOIN gold.dim_date d ON s.OrderDate = d.FullDate;
```

---

## 8. Group and Aggregate Data

### 8.1. PySpark Aggregations

```python
from pyspark.sql.functions import *

# === Basic Aggregation ===
df.groupBy("Region", "ProductCategory") \
    .agg(
        sum("Revenue").alias("TotalRevenue"),
        count("OrderID").alias("OrderCount"),
        avg("UnitPrice").alias("AvgUnitPrice"),
        min("OrderDate").alias("FirstOrder"),
        max("OrderDate").alias("LastOrder"),
        countDistinct("CustomerID").alias("UniqueCustomers")
    )

# === Rollup (Subtotals) ===
df.rollup("Region", "City") \
    .agg(sum("Revenue").alias("TotalRevenue")) \
    .orderBy("Region", "City")
# Kết quả: chi tiết theo City, subtotal theo Region, grand total

# === Cube (All Combinations) ===
df.cube("Region", "ProductCategory") \
    .agg(sum("Revenue").alias("TotalRevenue"))
# Kết quả: mọi tổ hợp subtotals

# === Pivot ===
df.groupBy("Region") \
    .pivot("ProductCategory", ["Electronics", "Clothing", "Food"]) \
    .agg(sum("Revenue"))
```

### 8.2. SQL Aggregations (Warehouse)

```sql
-- === GROUP BY with HAVING ===
SELECT 
    ProductCategory,
    COUNT(*) AS OrderCount,
    SUM(Revenue) AS TotalRevenue,
    AVG(Revenue) AS AvgRevenue
FROM silver.orders
GROUP BY ProductCategory
HAVING SUM(Revenue) > 100000;

-- === GROUPING SETS ===
SELECT 
    Region, 
    ProductCategory,
    SUM(Revenue) AS TotalRevenue
FROM silver.orders
GROUP BY GROUPING SETS (
    (Region, ProductCategory),  -- Detail
    (Region),                    -- Subtotal by Region
    ()                           -- Grand Total
);

-- === Window Aggregation (Running Total) ===
SELECT 
    OrderDate,
    Revenue,
    SUM(Revenue) OVER (ORDER BY OrderDate ROWS UNBOUNDED PRECEDING) AS RunningTotal,
    AVG(Revenue) OVER (ORDER BY OrderDate ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS MovingAvg7Day
FROM silver.orders;
```

### 8.3. KQL Aggregations (Eventhouse)

```kql
// === Basic Summarize ===
SensorData
| summarize 
    AvgTemp = avg(Temperature),
    MaxTemp = max(Temperature),
    MinTemp = min(Temperature),
    ReadingCount = count()
    by DeviceId

// === Time-binned Aggregation ===
SensorData
| summarize AvgTemp = avg(Temperature) by bin(EventTime, 1h), DeviceId
| order by EventTime desc

// === Percentiles ===
ResponseTimes
| summarize 
    P50 = percentile(Duration, 50),
    P90 = percentile(Duration, 90),
    P99 = percentile(Duration, 99)
    by ServiceName

// === Dynamic Aggregation (Array/Set) ===
Orders
| summarize 
    OrderList = make_list(OrderID),
    UniqueProducts = make_set(ProductID)
    by CustomerID
```

---

## 9. Handle Duplicate, Missing, and Late-Arriving Data

### 9.1. Xử lý Duplicate Data (Dữ liệu trùng lặp)

#### Phát hiện và loại bỏ duplicates bằng PySpark

```python
from pyspark.sql.functions import row_number, col
from pyspark.sql.window import Window

# Phương pháp 1: dropDuplicates (đơn giản)
df_dedup = df.dropDuplicates(["OrderID"])

# Phương pháp 2: Window function (giữ bản ghi mới nhất)
window_spec = Window.partitionBy("OrderID").orderBy(col("ModifiedDate").desc())

df_dedup = df.withColumn("rn", row_number().over(window_spec)) \
             .filter(col("rn") == 1) \
             .drop("rn")

# Phương pháp 3: Dùng MERGE INTO để tránh insert duplicate
from delta.tables import DeltaTable

target = DeltaTable.forName(spark, "silver.orders")
target.alias("t").merge(
    source_df.alias("s"),
    "t.OrderID = s.OrderID"
).whenNotMatchedInsertAll().execute()
```

#### Phát hiện duplicates bằng SQL
```sql
-- Tìm duplicates
SELECT OrderID, COUNT(*) as cnt
FROM bronze.orders
GROUP BY OrderID
HAVING COUNT(*) > 1;

-- Loại bỏ duplicates (giữ mới nhất)
WITH ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY OrderID ORDER BY ModifiedDate DESC) AS rn
    FROM bronze.orders
)
SELECT * FROM ranked WHERE rn = 1;
```

### 9.2. Xử lý Missing Data (Dữ liệu thiếu)

```python
from pyspark.sql.functions import col, when, coalesce, lit, avg

# === Phương pháp 1: Thay thế bằng giá trị mặc định ===
df_filled = df.fillna({
    "City": "Unknown",
    "Revenue": 0,
    "Category": "Uncategorized"
})

# === Phương pháp 2: Thay thế NULL bằng giá trị trung bình ===
avg_price = df.agg(avg("UnitPrice")).collect()[0][0]
df_filled = df.fillna({"UnitPrice": avg_price})

# === Phương pháp 3: COALESCE — lấy giá trị NOT NULL đầu tiên ===
df_clean = df.withColumn(
    "Address", 
    coalesce(col("ShippingAddress"), col("BillingAddress"), lit("No Address"))
)

# === Phương pháp 4: Loại bỏ rows có NULL trong cột quan trọng ===
df_clean = df.dropna(subset=["CustomerID", "OrderDate"])

# === Phương pháp 5: Flag missing data ===
df_flagged = df.withColumn(
    "IsMissingRevenue", 
    when(col("Revenue").isNull(), True).otherwise(False)
)
```

**SQL xử lý Missing Data**:
```sql
-- ISNULL / COALESCE
SELECT 
    OrderID,
    ISNULL(Quantity, 0) AS Quantity,
    COALESCE(ShippingAddress, BillingAddress, 'No Address') AS Address,
    CASE WHEN Revenue IS NULL THEN 'Missing' ELSE 'Present' END AS RevenueStatus
FROM bronze.orders;
```

### 9.3. Xử lý Late-Arriving Data (Dữ liệu đến muộn)

#### Vấn đề
Late-arriving data xảy ra khi:
- Fact record đến trước dimension record tương ứng
- Event đến muộn so với processing window
- System delay gây ra data out-of-order

#### Chiến lược xử lý

**Strategy 1: Early-Arriving Fact Pattern**
```python
# Khi fact đến trước dimension → dùng "Unknown" surrogate key
fact_with_lookup = fact_df.join(
    dim_customer,
    "CustomerID",
    "left"  # LEFT JOIN để giữ fact row dù dimension chưa có
).withColumn(
    "CustomerSK",
    coalesce(col("CustomerSK"), lit(-1))  # -1 = Unknown member
)
```

**Strategy 2: Reprocessing (Xử lý lại)**
```python
# Khi dimension đến muộn → UPDATE fact table
# Bước 1: Load dimension mới
# Bước 2: Update fact records có SK = -1

spark.sql("""
    MERGE INTO gold.fact_sales AS f
    USING gold.dim_customer AS d
        ON f.CustomerID = d.CustomerID AND f.CustomerSK = -1
    WHEN MATCHED THEN
        UPDATE SET f.CustomerSK = d.CustomerSK
""")
```

**Strategy 3: Late-Arriving Dimension Members**
```sql
-- Tạo "placeholder" dimension record khi fact đến trước
INSERT INTO gold.dim_customer (CustomerSK, CustomerID, CustomerName, IsCurrent)
VALUES (-1, NULL, 'Unknown', 1);

-- Sau khi dimension data đến → UPDATE placeholder
MERGE INTO gold.dim_customer AS target
USING source_customers AS source
    ON target.CustomerID = source.CustomerID
WHEN MATCHED AND target.CustomerName = 'Unknown' THEN
    UPDATE SET 
        target.CustomerName = source.CustomerName,
        target.City = source.City;
```

**Strategy 4: Event-time Windowing (cho Streaming)**
```python
from pyspark.sql.functions import avg, window

# Watermark giới hạn state và chính sách late data cho stateful aggregation
aggregated = (
    streaming_df
    .withWatermark("EventTime", "10 minutes")
    .groupBy(window("EventTime", "5 minutes"), "DeviceId")
    .agg(avg("Temperature").alias("AvgTemp"))
)
```

> 📌 **Điểm thi quan trọng về Late-Arriving Data**:
> - Luôn tạo **"Unknown" dimension member** (SK = -1) để handle early-arriving facts
> - Trong streaming, dùng **watermark** để xác định deadline cho late data
> - Dữ liệu đến sau watermark sẽ bị **DROP** (loại bỏ)

---

## 10. Câu hỏi mô phỏng bài thi (Practice Questions)

### Question 1

**You are designing a data platform in Microsoft Fabric. The requirements are:**
- **Data Engineering team** uses PySpark and Python for complex transformations
- **Data Analysts** need to query the final data using T-SQL
- **Raw data** includes JSON files, CSVs, and Parquet files from multiple sources
- **Final output** must support Power BI Direct Lake mode

**Which combination of data stores should you use?**

A. Eventhouse for ingestion, Warehouse for serving  
B. Lakehouse for ingestion and transformation, Warehouse for serving  
C. Lakehouse for everything  
D. Warehouse for everything

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — Lakehouse là lý tưởng cho Data Engineering team (PySpark, multi-format data). Warehouse tối ưu cho T-SQL queries từ Data Analysts. Cả hai đều hỗ trợ Direct Lake cho Power BI. Đây là kiến trúc phổ biến nhất.
> - **(A) Sai** — Eventhouse dành cho streaming/real-time, không phải batch ETL.
> - **(C) Sai** — Lakehouse SQL endpoint có giới hạn so với Warehouse cho T-SQL phức tạp (stored procedures, multi-table transactions).
> - **(D) Sai** — Warehouse không hỗ trợ PySpark và không lý tưởng cho xử lý JSON/semi-structured data.

---

### Question 2

**You need to provide analysts access to data stored in Azure Data Lake Storage Gen2 from a Fabric Lakehouse WITHOUT copying the data.**

**What should you use?**

A. Database mirroring  
B. Data pipeline with Copy Activity  
C. OneLake shortcut  
D. Dataflow Gen2

**✅ Correct Answer: C**

> **Giải thích**:
> - **(C) Đúng** — OneLake shortcuts tạo liên kết ảo tới ADLS Gen2, cho phép truy cập dữ liệu mà KHÔNG sao chép. Data xuất hiện như thư mục/bảng bình thường trong Lakehouse.
> - **(A) Sai** — Mirroring chỉ hỗ trợ databases (Azure SQL, Cosmos DB, etc.), KHÔNG hỗ trợ ADLS Gen2.
> - **(B) Sai** — Copy Activity sẽ sao chép dữ liệu, vi phạm yêu cầu "without copying".
> - **(D) Sai** — Dataflow Gen2 cũng di chuyển/transform dữ liệu.

---

### Question 3

**You are ingesting data from an operational Azure SQL Database with 100 tables into Microsoft Fabric. You need near real-time data freshness with minimal maintenance.**

**The Azure SQL Database does NOT have Change Data Capture (CDC) currently enabled.**

**Which approach should you use?**

A. Create a pipeline with 100 Copy Activities  
B. Enable CDC on Azure SQL and use Copy Job CDC mode  
C. Configure database mirroring  
D. Create OneLake shortcuts to the database

**✅ Correct Answer: C**

> **Giải thích**:
> - **(C) Đúng** — Mirroring là giải pháp zero-ETL tốt nhất cho scenario này. Nó tự động replicate toàn bộ tables, near real-time, không cần enable CDC trên nguồn, và không cần maintain pipeline. Mirroring tự xử lý change detection internally.
> - **(A) Sai** — 100 Copy Activities rất phức tạp để build và maintain.
> - **(B) Sai** — Đề bài nói CDC chưa được enable, và việc enable CDC có thể impact performance production database.
> - **(D) Sai** — OneLake shortcuts KHÔNG hỗ trợ kết nối trực tiếp tới Azure SQL Database. Shortcuts chỉ hỗ trợ storage (ADLS, S3, GCS).

---

### Question 4

**A citizen developer with no coding experience needs to clean and transform data from a SharePoint list and load it into a Fabric Lakehouse. The data has about 50,000 rows.**

**Which transformation tool should you recommend?**

A. Spark Notebook with PySpark  
B. Dataflow Gen2  
C. T-SQL stored procedure in Warehouse  
D. KQL in Eventhouse

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — Dataflow Gen2 có giao diện visual low-code (Power Query), hỗ trợ kết nối SharePoint natively, và phù hợp cho người dùng không biết code với data size nhỏ (50K rows).
> - **(A) Sai** — Notebook yêu cầu coding skills (PySpark), quá mức cần thiết cho task đơn giản này.
> - **(C) Sai** — T-SQL không phù hợp cho kết nối SharePoint và yêu cầu SQL knowledge.
> - **(D) Sai** — KQL dành cho real-time/streaming, không phù hợp cho SharePoint data.

---

### Question 5

**You are processing IoT sensor data in a Fabric Lakehouse using PySpark. Some sensor readings arrive multiple times with the same SensorID and Timestamp due to network retries. You need to keep only the most recent reading for each combination of SensorID and Timestamp.**

**Which PySpark approach should you use?**

A. `df.distinct()`  
B. `df.dropDuplicates(["SensorID", "Timestamp"])`  
C. Use a window function with `ROW_NUMBER()` partitioned by SensorID and Timestamp, ordered by ingestion time descending, then filter `rn == 1`  
D. `df.filter(col("duplicate") == False)`

**✅ Correct Answer: C**

> **Giải thích**:
> - **(C) Đúng** — Window function với `ROW_NUMBER()` cho phép xác định chính xác row "mới nhất" dựa trên cột ingestion time hoặc arrival time. Đây là pattern chuẩn khi cần chọn 1 row cụ thể từ nhiều duplicates dựa trên tiêu chí ordering.
> - **(A) Sai** — `distinct()` loại bỏ rows giống hoàn toàn, nhưng duplicate readings có thể khác nhau ở cột khác (ví dụ: _ingestion_timestamp).
> - **(B) Sai** — `dropDuplicates` giữ row đầu tiên gặp được, KHÔNG đảm bảo giữ row "mới nhất".
> - **(D) Sai** — Không có cột "duplicate" mặc định trong DataFrame.

---

### Question 6

**You are building a data pipeline where customer orders (fact data) sometimes arrive before the corresponding customer records (dimension data) are available. The customer dimension uses surrogate keys.**

**How should you handle this late-arriving dimension scenario?**

A. Drop the fact records that don't have matching dimension records  
B. Use an INNER JOIN so only matched records are loaded  
C. Use a LEFT JOIN and assign a default surrogate key (-1) for unmatched dimension references, then update later when dimension data arrives  
D. Delay the entire pipeline until all dimension data is confirmed loaded

**✅ Correct Answer: C**

> **Giải thích**:
> - **(C) Đúng** — Đây là pattern chuẩn "Early-Arriving Fact": sử dụng LEFT JOIN và gán SK = -1 (Unknown member) cho facts chưa có dimension match. Khi dimension data đến muộn, chạy UPDATE để sửa SK từ -1 thành giá trị đúng.
> - **(A) Sai** — Drop fact records gây mất dữ liệu kinh doanh.
> - **(B) Sai** — INNER JOIN cũng gây mất fact records không match.
> - **(D) Sai** — Delay pipeline gây trễ toàn bộ hệ thống, không thực tế.

---

### Question 7

**You have an Azure SQL Database that already has Azure Synapse Link enabled. You want to continuously replicate the database tables into a Fabric Lakehouse.**

**Which approach should you use?**

A. Configure database mirroring  
B. Use a Copy Job with an incremental method supported by the source (CDC if CDC can be enabled/supported; otherwise a reliable watermark)  
C. Create OneLake shortcuts directly to the Azure SQL Database  
D. Use Dataflow Gen2 and full-refresh every minute

**✅ Correct Answer: B**

> **Giải thích**:
> - **(A) Sai** — Azure SQL Database đã bật Synapse Link là một documented blocker cho Fabric Mirroring.
> - **(B) Đúng** — Khi Mirroring bị chặn, dùng Data Factory incremental movement. Chọn **CDC** chỉ khi CDC được bật và connector hỗ trợ; nếu không, dùng **watermark** đáng tin cậy. Đừng suy luận “Synapse Link enabled ⇒ CDC cũng enabled”.
> - **(C) Sai** — OneLake shortcut không trỏ trực tiếp vào relational Azure SQL Database như một storage shortcut.
> - **(D) Sai** — full-refresh mỗi phút tạo data movement/load không cần thiết.

---

### Question 8

**You are transforming data in a Fabric Eventhouse. You need to parse JSON data from raw event logs and extract specific fields.**

**Which query language should you use?**

A. PySpark  
B. T-SQL  
C. KQL  
D. DAX

**✅ Correct Answer: C**

> **Giải thích**:
> - **(C) Đúng** — KQL (Kusto Query Language) là ngôn ngữ native của Eventhouse. Nó có hàm `parse_json()` và `extend` để extract fields từ JSON rất hiệu quả.
> - **(A) Sai** — PySpark chạy trong Lakehouse Notebook, không phải Eventhouse.
> - **(B) Sai** — T-SQL dùng trong Warehouse, không phải Eventhouse.
> - **(D) Sai** — DAX dùng trong Power BI semantic model, không phải data transformation.

---

### Question 9

**You need to create a denormalized product dimension table for a Gold layer. The source data is in three normalized Silver tables: Products, SubCategories, and Categories.**

**Which approach correctly creates the denormalized dimension?**

A. Create shortcuts from all three tables into the Gold folder  
B. Create a view that JOINs the three tables and materialize it as a Delta table  
C. Copy each table individually into the Gold layer  
D. Use a Dataflow Gen2 to merge the three tables without any transformations

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — Denormalization yêu cầu JOIN các bảng normalized lại thành 1 bảng phẳng. Cách chuẩn là viết query JOIN 3 bảng rồi save kết quả dưới dạng Delta table trong Gold layer.
> - **(A) Sai** — Shortcuts chỉ tạo liên kết ảo, không thực hiện denormalization (JOIN).
> - **(C) Sai** — Copy từng bảng riêng lẻ không tạo ra bảng denormalized.
> - **(D) Sai** — "Merge without transformations" không có ý nghĩa — cần JOIN để denormalize.

---

### Question 10

**In Microsoft Fabric, compute costs for querying data through an OneLake shortcut are charged to which capacity?**

A. The capacity of the source where the original data resides  
B. The capacity of the workspace where the shortcut was created (destination)  
C. Costs are split evenly between source and destination  
D. No compute costs are incurred for shortcut queries

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — Chi phí compute khi query qua shortcut được tính trên **destination capacity** — tức capacity của workspace nơi shortcut nằm, KHÔNG phải capacity nơi data gốc lưu trữ.
> - **(A) Sai** — Source capacity không bị charge cho queries từ shortcut.
> - **(C) Sai** — Costs không chia đôi.
> - **(D) Sai** — Vẫn có compute cost khi query.

---

*Tài liệu được tổng hợp dựa trên Skills Measured as of July 21, 2026*  
*Nguồn tham khảo: Microsoft Learn, Microsoft Fabric Documentation*


---

## 11. BỔ SUNG CODE: PYSPARK, T-SQL VÀ KQL

### 11.1 PySpark làm sạch và aggregate

```python
from pyspark.sql.functions import col, coalesce, lit, sum as spark_sum

clean = (
    spark.table("bronze.sales")
    .dropDuplicates(["OrderId"])
    .withColumn("Quantity", coalesce(col("Quantity"), lit(0)))
    .filter(col("OrderDate").isNotNull())
)

gold = clean.groupBy("OrderDate", "Region").agg(
    spark_sum(col("Quantity") * col("UnitPrice")).alias("Revenue")
)
gold.write.format("delta").mode("overwrite").saveAsTable("gold.daily_sales")
```

### 11.2 T-SQL aggregate trong Warehouse

```sql
SELECT
    CAST(OrderDate AS date) AS OrderDate,
    Region,
    SUM(Quantity * UnitPrice) AS Revenue
FROM dbo.Sales
WHERE OrderDate IS NOT NULL
GROUP BY CAST(OrderDate AS date), Region;
```

### 11.3 KQL transform trong Eventhouse

```kusto
Telemetry
| where Timestamp >= ago(1h)
| summarize AvgTemperature = avg(Temperature), Events = count()
    by DeviceId, bin(Timestamp, 5m)
| order by Timestamp desc
```

### 11.4 Câu hỏi tình huống bổ sung

**Dữ liệu JSON lớn cần flatten, dedup, custom UDF và ghi Delta Silver. Team dùng Python. Chọn công cụ nào?**

A. Notebook PySpark  
B. Deployment pipeline  
C. Sensitivity label  
D. KQL Queryset trên Warehouse

<details><summary>Đáp án</summary>

**A.** Notebook/Spark phù hợp distributed transformation và logic code-first.
</details>


---

## 12. CÂU HỎI SCENARIO — BATCH DATA

> Các câu hỏi dưới đây là câu tự soạn theo blueprint DP-700, **không phải câu hỏi thật hoặc exam dump**.

### Câu D2-B1 — Chọn Data Store: Lakehouse

**Dữ liệu đa định dạng (JSON, CSV, Parquet), cần xử lý bằng PySpark, hỗ trợ ML workflow, và tổ chức theo kiến trúc Medallion (Bronze/Silver/Gold). Store nào phù hợp nhất?**

A. Lakehouse (Delta Lake trên OneLake)  
B. Warehouse (T-SQL only)  
C. Eventhouse (KQL only)  
D. SQL Database (OLTP)  

<details><summary>Đáp án và giải thích</summary>

**A.** [Lakehouse](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-overview) là store duy nhất hỗ trợ đồng thời: multi-format files, Spark/PySpark processing, ML workflows, và kiến trúc Medallion. Delta Lake làm nền tảng cho cả Tables (có schema) lẫn Files (raw data).

</details>

---

### Câu D2-B2 — Chọn Data Store: Warehouse

**Cần lưu trữ relational star schema, hỗ trợ T-SQL đầy đủ (DDL, DML, stored procedures), và phục vụ BI reports với high concurrency. Store nào tối ưu?**

A. Warehouse (Fabric)  
B. Eventhouse  
C. Notebook  
D. Airflow pool  

<details><summary>Đáp án và giải thích</summary>

**A.** [Fabric Warehouse](https://learn.microsoft.com/en-us/fabric/data-warehouse/data-warehousing) tối ưu cho relational data, T-SQL analytics với full DDL/DML support, và phục vụ BI reports thông qua SQL analytics endpoint với high concurrency. Eventhouse dùng KQL, không T-SQL; Notebook là compute tool, không phải storage.

</details>

---

### Câu D2-B3 — Zero-Copy Access

**Dữ liệu đang lưu trong ADLS Gen2 và Amazon S3. Team cần truy cập để query bằng Spark, nhưng không được copy vào OneLake (chính sách cost và data residency). Giải pháp nào phù hợp?**

A. OneLake shortcut (ADLS Gen2 và S3)  
B. Copy data bằng pipeline mỗi giờ  
C. Import model trong Power BI  
D. Deployment pipeline  

<details><summary>Đáp án và giải thích</summary>

**A.** [OneLake shortcuts](https://learn.microsoft.com/en-us/fabric/onelake/onelake-shortcuts) tạo virtual pointer tới external storage — **không cần ingest/copy bản dữ liệu nguồn vào Lakehouse**. Cơ chế authentication **phụ thuộc loại shortcut/source** (ví dụ Organizational account, Service principal, Workspace identity, SAS ở các connector phù hợp), không có một rule “mọi shortcut đều passthrough”.

</details>

---

### Câu D2-B4 — Transformation Phức Tạp

**Dữ liệu JSON lớn cần: flatten nested arrays, dedup theo key, viết custom UDF, và schema evolution phức tạp. Transform tool nào phù hợp?**

A. Notebook PySpark  
B. Dataflow Gen2 vì low-code  
C. Deployment pipeline  
D. Sensitivity label  

<details><summary>Đáp án và giải thích</summary>

**A.** [Notebook PySpark](https://learn.microsoft.com/en-us/fabric/data-engineering/how-to-use-notebook) phù hợp cho distributed custom transformation: custom UDF, schema evolution phức tạp, nested data processing. Dataflow Gen2 có giới hạn với complex UDF và xử lý nested JSON quy mô lớn; deployment pipeline là orchestration tool, không phải transformation.

</details>

---

### Câu D2-B5 — KQL Aggregation Trên Eventhouse

**Dữ liệu telemetry time-series đang lưu trong Eventhouse. Cần tính average temperature theo 5 phút cho mỗi device. Language nào dùng?**

A. KQL với `summarize` + `bin`  
B. T-SQL MERGE vào Git  
C. DAX only  
D. M language trong deployment pipeline  

<details><summary>Đáp án và giải thích</summary>

**A.** [KQL (Kusto Query Language)](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/kusto-query-language) là ngôn ngữ chính của Eventhouse. `summarize avg(Temperature) by bin(Timestamp, 5m), DeviceId` là cú pháp chuẩn cho time-series aggregation.

</details>

---

## 13. LAB THỰC HÀNH — BATCH DATA

### Lab 1 — Full Load Và Incremental MERGE

**Mục tiêu:** Hiểu sự khác biệt giữa full load (overwrite) và incremental load (MERGE), đồng thời biết điều kiện để một MERGE có thể rerun an toàn.

```python
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# ═══════════════════════════════════════════════════
# PHẦN 1: Full Load — Tạo bảng ban đầu
# ═══════════════════════════════════════════════════
initial = spark.createDataFrame([
    (1, "Pending",   10.0, "2026-08-01T10:00:00"),
    (2, "Confirmed", 20.0, "2026-08-01T10:00:00"),
], ["OrderId", "Status", "Amount", "ModifiedDate"])

(initial
 .withColumn("ModifiedDate", F.to_timestamp("ModifiedDate"))
 .write.format("delta")
 .mode("overwrite")        # Full load: xoá dữ liệu cũ, tạo Delta version mới
 .saveAsTable("lab.orders"))

# ═══════════════════════════════════════════════════
# PHẦN 2: Incremental MERGE — Upsert thay đổi
# ═══════════════════════════════════════════════════
changes = spark.createDataFrame([
    (2, "Shipped", 25.0, "2026-08-02T10:00:00"),   # OrderId=2 thay đổi Status và Amount
    (3, "Pending", 30.0, "2026-08-02T10:00:00"),    # OrderId=3 mới
], ["OrderId", "Status", "Amount", "ModifiedDate"]).withColumn(
    "ModifiedDate", F.to_timestamp("ModifiedDate")
)

target = DeltaTable.forName(spark, "lab.orders")
(target.alias("t")
 .merge(changes.alias("s"), "t.OrderId = s.OrderId")
 # Chỉ update nếu source mới hơn target (tránh revert bởi dữ liệu cũ)
 .whenMatchedUpdateAll(condition="s.ModifiedDate >= t.ModifiedDate")
 .whenNotMatchedInsertAll()
 .execute())

# Kiểm tra: rerun MERGE không tạo duplicate
# Xem lịch sử: spark.sql("DESCRIBE HISTORY lab.orders").show()
```

**Điểm quan trọng:**
- Trong ví dụ này, MERGE có thể rerun an toàn vì source có **một row duy nhất cho mỗi `OrderId`**, match key xác định, và update condition ổn định
- Ngoài thực tế phải dedup source trước MERGE; nhiều source row cùng match một target row có thể làm MERGE lỗi hoặc có kết quả không xác định
- Condition `s.ModifiedDate >= t.ModifiedDate` ngăn dữ liệu cũ ghi đè dữ liệu mới hơn

---

### Lab 6 — Warehouse T-SQL: Tạo Bảng Và Query

**Mục tiêu:** Thực hành T-SQL trong Fabric Warehouse — tạo bảng fact, query với aggregation.

```sql
-- Bước 1: Tạo bảng fact trong Fabric Warehouse
CREATE TABLE dbo.FactSales (
    SalesId    BIGINT       NOT NULL,
    OrderDate  DATE         NOT NULL,
    CustomerId BIGINT       NOT NULL,
    ProductId  BIGINT       NOT NULL,
    Quantity   INT          NOT NULL,
    Amount     DECIMAL(18,2) NOT NULL
);

-- Bước 2: Insert dữ liệu mẫu
INSERT INTO dbo.FactSales VALUES
  (1, '2026-08-01', 101, 201, 3, 150.00),
  (2, '2026-08-01', 102, 202, 1,  50.00),
  (3, '2026-08-02', 101, 201, 2, 100.00);

-- Bước 3: Aggregation query — Revenue theo ngày và customer
SELECT
    OrderDate,
    CustomerId,
    SUM(Amount)   AS Revenue,
    SUM(Quantity) AS TotalQty,
    COUNT(*)      AS OrderCount
FROM dbo.FactSales
GROUP BY OrderDate, CustomerId
ORDER BY OrderDate, Revenue DESC;

-- Bước 4: Xem execution plan (không hỗ trợ EXPLAIN trong Warehouse, dùng Query Insights)
```

> [!NOTE]
> Fabric Warehouse sử dụng distributed SQL engine. Tham khảo: [Fabric Warehouse overview](https://learn.microsoft.com/en-us/fabric/data-warehouse/data-warehousing)

---

## 14. CODE CHEAT SHEET — BATCH DATA

**PySpark:**
```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

clean=(df.filter(F.col('OrderId').isNotNull())
       .withColumn('Amount',F.col('Amount').cast('decimal(18,2)'))
       .fillna({'Region':'Unknown'}))
w=Window.partitionBy('OrderId').orderBy(F.col('ModifiedDate').desc())
latest=clean.withColumn('rn',F.row_number().over(w)).filter('rn=1').drop('rn')
```

**T-SQL window + aggregate:**
```sql
WITH Latest AS (
 SELECT *, ROW_NUMBER() OVER(PARTITION BY OrderId ORDER BY ModifiedDate DESC) rn
 FROM dbo.OrderStage
)
SELECT Region, SUM(Amount) Revenue, COUNT(*) Orders
FROM Latest
WHERE rn=1
GROUP BY Region
HAVING SUM(Amount)>10000;
```

**KQL core:**
```kusto
Telemetry
| where Timestamp > ago(24h)
| extend Temp=todouble(Temperature)
| project Timestamp,DeviceId,Site,Temp
| summarize AvgTemp=avg(Temp), P95=percentile(Temp,95), Events=count()
  by Site,DeviceId,bin(Timestamp,5m)
| order by Timestamp desc
```

Operators cần nhận ra: `where`, `project`, `extend`, `summarize`, `bin`, `join`, `distinct`, `arg_max`, `top`, `take`, `order by`, `mv-expand`, `parse_json`, `make-series`.

```text
Power Query/low-code               → Dataflow Gen2
Distributed/custom Spark           → Notebook
Relational SQL transform           → T-SQL
Time-series/log/telemetry          → KQL
Move data only                     → Copy Job
Orchestrate many steps             → Pipeline
Zero-copy storage                  → Shortcut
Managed operational DB replication→ Mirroring
```

---

## NGUỒN CHÍNH THỨC ĐÃ ĐỐI CHIẾU

- [DP-700 Study Guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)
- [Lakehouse Copy Activity](https://learn.microsoft.com/en-us/fabric/data-factory/connector-lakehouse-copy-activity)
- [What is Copy Job](https://learn.microsoft.com/en-us/fabric/data-factory/what-is-copy-job)
- [CDC in Copy Job](https://learn.microsoft.com/en-us/fabric/data-factory/cdc-copy-job)
- [Mirroring overview](https://learn.microsoft.com/en-us/fabric/mirroring/overview)
- [Azure SQL mirroring limitations](https://learn.microsoft.com/en-us/fabric/mirroring/azure-sql-database-limitations)
- [OneLake Shortcuts](https://learn.microsoft.com/en-us/fabric/onelake/onelake-shortcuts)


- [DP-700 study guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)
- [OneLake shortcuts](https://learn.microsoft.com/en-us/fabric/onelake/onelake-shortcuts)
- [Copy activity](https://learn.microsoft.com/en-us/fabric/data-factory/copy-data-activity)

---
