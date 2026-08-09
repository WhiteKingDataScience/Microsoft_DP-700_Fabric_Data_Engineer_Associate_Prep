# DP-700: Design and Implement Loading Patterns
## Ingest and Transform Data (30–35%) — Phần 1/3


> [!IMPORTANT]
> **Bản hiệu chỉnh ngày 04/08/2026.** Nội dung được đối chiếu với blueprint DP-700 có hiệu lực từ **21/07/2026**. Microsoft cho biết phần lớn câu hỏi tập trung vào tính năng GA, nhưng tính năng Preview phổ biến vẫn có thể xuất hiện. Với các bảng “supported items”, runtime, limits và trạng thái Preview/GA, hãy kiểm tra Microsoft Learn lần cuối trước ngày thi.

> **Skills measured as of July 21, 2026**  
> Tài liệu chuẩn bị thi chứng chỉ **Microsoft Fabric Data Engineer Associate (DP-700)**  
> Tham chiếu: [Microsoft Learn — Study Guide DP-700](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)

---

## MỤC LỤC

1. [Tổng quan về Loading Patterns trong Fabric](#1-tổng-quan-về-loading-patterns-trong-fabric)
2. [Design and Implement Full and Incremental Data Loads](#2-design-and-implement-full-and-incremental-data-loads)
3. [Prepare Data for Loading into a Dimensional Model](#3-prepare-data-for-loading-into-a-dimensional-model)
4. [Design and Implement a Loading Pattern for Streaming Data](#4-design-and-implement-a-loading-pattern-for-streaming-data)
5. [Câu hỏi mô phỏng bài thi (Practice Questions)](#5-câu-hỏi-mô-phỏng-bài-thi-practice-questions)

---

## 1. Tổng quan về Loading Patterns trong Fabric

### 1.1. Kiến trúc Medallion (Bronze – Silver – Gold)

Microsoft Fabric khuyến nghị sử dụng kiến trúc **Medallion** gồm 3 tầng:

| Tầng | Mục đích | Đặc điểm |
|------|----------|-----------|
| **Bronze** | Raw data — dữ liệu thô từ nguồn | Append-only, giữ nguyên format gốc, có audit columns |
| **Silver** | Cleaned & Conformed — dữ liệu đã làm sạch | Deduplicated, validated, chuẩn hóa kiểu dữ liệu |
| **Gold** | Business-ready — dữ liệu sẵn sàng phân tích | Star schema, denormalized, tối ưu cho Power BI Direct Lake |

### 1.2. Nguyên tắc chung

- **Tách biệt Ingestion và Transformation**: Không transform dữ liệu trong bước ingestion
- **Idempotency**: Pipeline có thể chạy lại nhiều lần mà không tạo dữ liệu trùng lặp
- **Audit Trail**: Luôn thêm cột `_ingestion_timestamp`, `_source_system` để theo dõi lineage
- **Delta Lake Format**: Fabric dùng Delta cho các analytical tables phổ biến, nhưng thư mục `Files` trong OneLake có thể chứa CSV, JSON, Parquet, ảnh và các định dạng khác. Không đồng nhất “mọi file trong OneLake” với “Delta table”.

---

## 2. Design and Implement Full and Incremental Data Loads

### 2.1. Full Load (Tải toàn bộ)

#### Khái niệm
Full load là quá trình **tải lại toàn bộ dữ liệu** từ nguồn vào đích mỗi lần pipeline chạy. Dữ liệu cũ bị ghi đè hoặc thay thế hoàn toàn.

#### Khi nào sử dụng Full Load?
- Dữ liệu nguồn nhỏ (< vài triệu rows)
- Không có cột watermark (Modified Date, RowVersion) trong nguồn
- Nguồn dữ liệu là file tĩnh (CSV, Excel, flat file)
- Bảng dimension nhỏ thay đổi toàn bộ
- Lần khởi tạo (initial load) đầu tiên

#### Cách triển khai trong Fabric

**Cách 1: Pipeline Copy Activity với mode Overwrite**
```
Pipeline → Copy Activity → Destination: Lakehouse Table
→ Write Mode: "Overwrite"
```

**Cách 2: Notebook PySpark với mode overwrite**
```python
# Full load từ nguồn vào Bronze
df = spark.read.format("csv") \
    .option("header", "true") \
    .load("abfss://source-container@storage.dfs.core.windows.net/data/")

# Thêm audit columns
from pyspark.sql.functions import current_timestamp, lit

df_with_audit = df.withColumn("_ingestion_timestamp", current_timestamp()) \
                  .withColumn("_source_system", lit("ERP_System"))

# Ghi đè toàn bộ
df_with_audit.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze.customers")
```

> ⚠️ **Hiệu chỉnh quan trọng:** `overwrite`, `INSERT OVERWRITE` hoặc `CREATE OR REPLACE` tạo **một version mới** trong Delta log; chúng không xóa toàn bộ transaction log. Dữ liệu cũ còn có thể time travel/restore cho đến khi bị `VACUUM`. Tuy nhiên, overwrite vẫn không phải row-level incremental upsert; dùng `MERGE`/CDC/watermark khi cần chỉ áp dụng thay đổi.

---

### 2.2. Incremental Load (Tải gia tăng)

#### Khái niệm
Incremental load chỉ **tải dữ liệu mới hoặc đã thay đổi** kể từ lần chạy cuối cùng, giúp giảm thời gian xử lý và chi phí compute.

#### Ba phương pháp Incremental Load chính

##### A. Watermark-based (High-Water Mark)

**Nguyên lý**: Sử dụng một cột "watermark" (thường là `ModifiedDate` hoặc `RowVersion`) để xác định dữ liệu mới.

**Quy trình**:
1. **Lookup**: Đọc giá trị watermark cao nhất từ bảng control
2. **Filter**: Truy vấn nguồn chỉ lấy dữ liệu có watermark > giá trị đã lưu
3. **Load**: Ghi dữ liệu mới vào đích
4. **Update**: Cập nhật giá trị watermark mới vào bảng control

**Ví dụ triển khai bằng Pipeline**:
```
Pipeline:
  ├── Lookup Activity: Đọc LastWatermark từ control_table
  ├── Copy Activity: 
  │     Source Query: SELECT * FROM Orders WHERE ModifiedDate > @{activity('Lookup').output.firstRow.LastWatermark}
  │     Destination: Lakehouse bronze.orders (Append mode)
  └── Stored Procedure / Script Activity: 
        UPDATE control_table SET LastWatermark = @{activity('Copy').output.MaxValue}
```

**Ví dụ triển khai bằng Notebook**:
```python
from pyspark.sql.functions import max, col, current_timestamp, lit

# 1. Đọc watermark từ control table
watermark_df = spark.sql("SELECT MAX(LastWatermark) as wm FROM control.watermarks WHERE TableName = 'Orders'")
last_watermark = watermark_df.collect()[0]['wm']

# 2. Đọc dữ liệu mới từ nguồn
new_data = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("query", f"SELECT * FROM Orders WHERE ModifiedDate > '{last_watermark}'") \
    .load()

# 3. Ghi vào Bronze (append)
new_data.withColumn("_ingestion_timestamp", current_timestamp()) \
    .write.format("delta") \
    .mode("append") \
    .saveAsTable("bronze.orders")

# 4. Cập nhật watermark
new_max = new_data.agg(max("ModifiedDate")).collect()[0][0]
spark.sql(f"UPDATE control.watermarks SET LastWatermark = '{new_max}' WHERE TableName = 'Orders'")
```

> 📌 **Điểm thi quan trọng**: Watermark pattern **KHÔNG** bắt được hàng bị DELETE từ nguồn. Nếu cần tracking deletes → dùng CDC hoặc Mirroring.

##### B. Change Data Capture (CDC)

**Nguyên lý**: Nguồn dữ liệu tự động phát hiện và ghi lại mọi thay đổi (INSERT, UPDATE, DELETE) vào một change log.

**Triển khai trong Fabric**:

- **Copy Job với CDC mode**: Fabric Data Factory hỗ trợ CDC natively cho:
  - Azure SQL Database
  - SQL Server (2016–2025)
  - Snowflake
  - Google BigQuery
  - SAP (qua Datasphere)

```
Copy Job Configuration:
  ├── Source: Azure SQL Database (CDC enabled)
  ├── Change Tracking: CDC
  ├── Destination: Lakehouse Table
  └── Write Mode: Merge (auto upsert/delete)
```

**Ưu điểm CDC**:
- Bắt được cả DELETE operations
- Không cần maintain watermark table
- Tự động merge vào bảng đích (upsert)

##### C. MERGE INTO Pattern

**Nguyên lý**: Sử dụng lệnh SQL `MERGE INTO` để thực hiện "upsert" — INSERT nếu row mới, UPDATE nếu đã tồn tại.

**Ví dụ SQL trong Fabric Warehouse**:
```sql
MERGE INTO silver.customers AS target
USING bronze.customers_staging AS source
    ON target.CustomerID = source.CustomerID
WHEN MATCHED AND source.ModifiedDate > target.ModifiedDate THEN
    UPDATE SET 
        target.CustomerName = source.CustomerName,
        target.Email = source.Email,
        target.ModifiedDate = source.ModifiedDate,
        target._updated_at = GETDATE()
WHEN NOT MATCHED THEN
    INSERT (CustomerID, CustomerName, Email, ModifiedDate, _created_at)
    VALUES (source.CustomerID, source.CustomerName, source.Email, source.ModifiedDate, GETDATE());
```

**Ví dụ PySpark trong Lakehouse Notebook**:
```python
from delta.tables import DeltaTable

# Đọc dữ liệu mới (staging)
staging_df = spark.table("bronze.customers_staging")

# Merge vào Silver
silver_table = DeltaTable.forName(spark, "silver.customers")

silver_table.alias("target").merge(
    staging_df.alias("source"),
    "target.CustomerID = source.CustomerID"
).whenMatchedUpdate(
    condition="source.ModifiedDate > target.ModifiedDate",
    set={
        "CustomerName": "source.CustomerName",
        "Email": "source.Email",
        "ModifiedDate": "source.ModifiedDate",
        "_updated_at": "current_timestamp()"
    }
).whenNotMatchedInsertAll() \
 .execute()

# Tối ưu sau MERGE
spark.sql("OPTIMIZE silver.customers")
```

> 📌 **Best Practice**: Luôn chạy `OPTIMIZE` sau khi MERGE nhiều dữ liệu để hợp nhất các small files.

### 2.3. So sánh Full Load vs Incremental Load

| Tiêu chí | Full Load | Incremental Load |
|----------|-----------|-----------------|
| **Dữ liệu di chuyển** | Toàn bộ | Chỉ thay đổi |
| **Thời gian xử lý** | Lâu | Nhanh |
| **Chi phí CU** | Cao | Thấp |
| **Độ phức tạp** | Đơn giản | Trung bình → Cao |
| **Bắt DELETE** | Có (ghi đè) | Chỉ khi dùng CDC |
| **Khi nào dùng** | Dữ liệu nhỏ, khởi tạo | Dữ liệu lớn, chạy định kỳ |

---

## 3. Prepare Data for Loading into a Dimensional Model

### 3.1. Khái niệm Star Schema

Star schema là mô hình dữ liệu tối ưu cho phân tích (analytics) gồm:

- **Fact Table** (Bảng sự kiện): Chứa các metrics/measures (số lượng, doanh thu, chi phí)
  - Khóa ngoại (FK) liên kết tới các dimension
  - Granularity: mỗi row = một sự kiện kinh doanh
  
- **Dimension Table** (Bảng chiều): Chứa thuộc tính mô tả (tên sản phẩm, tên khách hàng, ngày tháng)
  - Khóa chính: Surrogate Key (số nguyên tăng dần)
  - Denormalized: flatten tất cả hierarchy vào một bảng

#### Ví dụ Star Schema cho Sales

```
                    ┌──────────────┐
                    │  DimDate     │
                    │──────────────│
                    │ DateKey (PK) │
                    │ FullDate     │
                    │ Year         │
                    │ Quarter      │
                    │ Month        │
                    └──────┬───────┘
                           │
┌──────────────┐    ┌──────┴───────┐    ┌──────────────┐
│ DimCustomer  │    │  FactSales   │    │ DimProduct   │
│──────────────│    │──────────────│    │──────────────│
│ CustKey (PK) ├────┤ DateKey (FK) ├────┤ ProdKey (PK) │
│ CustName     │    │ CustKey (FK) │    │ ProductName  │
│ City         │    │ ProdKey (FK) │    │ Category     │
│ Country      │    │ Quantity     │    │ SubCategory  │
│ Segment      │    │ Revenue      │    │ Brand        │
└──────────────┘    │ Cost         │    └──────────────┘
                    └──────────────┘
```

### 3.2. Surrogate Keys vs Natural Keys

| Đặc điểm | Surrogate Key | Natural Key |
|-----------|--------------|-------------|
| **Kiểu dữ liệu** | Integer (INT/BIGINT) | Chuỗi (VARCHAR), tùy nguồn |
| **Nguồn gốc** | Tự tạo (auto-increment) | Từ hệ thống nguồn |
| **Kích thước** | Nhỏ (4–8 bytes) | Lớn (tùy thuộc) |
| **JOIN Performance** | Cao | Thấp hơn |
| **Xử lý SCD** | Dễ | Khó |

> 📌 **Quy tắc thi**: Luôn sử dụng **Surrogate Key** (integer-based) trong dimensional model. Giữ Natural Key làm business key riêng.

**Ví dụ tạo Surrogate Key bằng PySpark**:
```python
from pyspark.sql.functions import monotonically_increasing_id

dim_customer = spark.table("silver.customers") \
    .select("CustomerID", "CustomerName", "City", "Country", "Segment") \
    .withColumn("CustomerSK", monotonically_increasing_id())

dim_customer.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.dim_customer")
```

### 3.3. Slowly Changing Dimensions (SCD)

#### SCD Type 1 — Overwrite (Ghi đè)

- **Hành vi**: Cập nhật trực tiếp bản ghi hiện tại, mất lịch sử
- **Khi nào dùng**: Sửa lỗi dữ liệu, thông tin không cần lịch sử (số điện thoại)
- **Triển khai**: Dùng `MERGE INTO` với `WHEN MATCHED THEN UPDATE`

```sql
-- SCD Type 1: Ghi đè trực tiếp
MERGE INTO gold.dim_customer AS target
USING silver.customers AS source
    ON target.CustomerID = source.CustomerID
WHEN MATCHED THEN
    UPDATE SET 
        target.CustomerName = source.CustomerName,
        target.Phone = source.Phone    -- Chỉ cập nhật, không lưu lịch sử
WHEN NOT MATCHED THEN
    INSERT (CustomerID, CustomerName, Phone)
    VALUES (source.CustomerID, source.CustomerName, source.Phone);
```

#### SCD Type 2 — Add New Row (Thêm hàng mới)

- **Hành vi**: Giữ nguyên bản ghi cũ, thêm bản ghi mới với flag `IsCurrent`
- **Khi nào dùng**: Cần phân tích lịch sử (khách hàng chuyển địa chỉ, thay đổi segment)
- **Cột bắt buộc**: `ValidFrom`, `ValidTo`, `IsCurrent`

**Ví dụ SCD Type 2 bằng PySpark**:
```python
from pyspark.sql.functions import col, lit, current_timestamp
from delta.tables import DeltaTable

# Bước 1: Đánh dấu records cũ hết hiệu lực
dim_table = DeltaTable.forName(spark, "gold.dim_customer")

dim_table.alias("target").merge(
    source_df.alias("source"),
    "target.CustomerID = source.CustomerID AND target.IsCurrent = true"
).whenMatchedUpdate(
    condition="target.City <> source.City OR target.Segment <> source.Segment",
    set={
        "IsCurrent": lit(False),
        "ValidTo": current_timestamp()
    }
).execute()

# Bước 2: Insert records mới (cả mới hoàn toàn và changed)
new_records = source_df.alias("s").join(
    spark.table("gold.dim_customer").filter("IsCurrent = true").alias("t"),
    (col("s.CustomerID") == col("t.CustomerID")) & 
    ((col("s.City") != col("t.City")) | (col("s.Segment") != col("t.Segment"))),
    "inner"  # Changed records
).select("s.*") \
.union(
    source_df.alias("s").join(
        spark.table("gold.dim_customer").alias("t"),
        col("s.CustomerID") == col("t.CustomerID"),
        "left_anti"  # New records
    )
).withColumn("IsCurrent", lit(True)) \
 .withColumn("ValidFrom", current_timestamp()) \
 .withColumn("ValidTo", lit(None).cast("timestamp"))

new_records.write.format("delta").mode("append").saveAsTable("gold.dim_customer")
```

**Fabric Copy Job — SCD Type 2 tự động (2026)**:
- Từ 2026, **Copy Job** trong Fabric Data Factory hỗ trợ SCD Type 2 **no-code**
- Tự động xử lý `ValidFrom`, `ValidTo`, soft deletes
- Không cần viết SQL MERGE phức tạp

### 3.4. Denormalization (Phi chuẩn hóa)

#### Khái niệm
Denormalization là quá trình **gộp nhiều bảng normalized thành ít bảng hơn** bằng cách flatten hierarchy, loại bỏ bảng trung gian.

#### Tại sao cần Denormalize cho Dimensional Model?
- Giảm số lượng JOIN → tăng performance truy vấn
- Đơn giản hóa model cho người dùng cuối
- Tối ưu cho Power BI VertiPaq engine (nén tốt hơn)
- Hỗ trợ Direct Lake mode tốt hơn

#### Ví dụ Denormalize
```python
# Nguồn: 3 bảng normalized
# products (ProductID, ProductName, SubCategoryID)
# sub_categories (SubCategoryID, SubCategoryName, CategoryID)  
# categories (CategoryID, CategoryName)

# Denormalize thành 1 dimension table
dim_product = spark.table("silver.products") \
    .join(spark.table("silver.sub_categories"), "SubCategoryID") \
    .join(spark.table("silver.categories"), "CategoryID") \
    .select(
        "ProductID",
        "ProductName",
        "SubCategoryName",
        "CategoryName"
    )

dim_product.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.dim_product")
```

### 3.5. Fact Table Loading Best Practices

| Bước | Hành động | Giải thích |
|------|----------|------------|
| 1 | Load Dimensions trước | Cần có Surrogate Keys sẵn |
| 2 | Lookup Surrogate Keys | Map natural keys → surrogate keys |
| 3 | Aggregate nếu cần | Tổng hợp theo granularity yêu cầu |
| 4 | Insert vào Fact table | Append mode cho new rows |
| 5 | OPTIMIZE table | Hợp nhất small files |

```python
# Load Fact Table với Surrogate Key lookup
fact_sales = spark.table("silver.sales_clean")

# Lookup dimension keys
dim_customer = spark.table("gold.dim_customer") \
    .filter("IsCurrent = true") \
    .select("CustomerSK", "CustomerID")

dim_product = spark.table("gold.dim_product") \
    .select("ProductSK", "ProductID")

dim_date = spark.table("gold.dim_date") \
    .select("DateKey", "FullDate")

# Join để lấy surrogate keys
fact_enriched = fact_sales \
    .join(dim_customer, "CustomerID") \
    .join(dim_product, "ProductID") \
    .join(dim_date, fact_sales.OrderDate == dim_date.FullDate) \
    .select("DateKey", "CustomerSK", "ProductSK", "Quantity", "Revenue", "Cost")

fact_enriched.write.format("delta") \
    .mode("append") \
    .saveAsTable("gold.fact_sales")
```

---

## 4. Design and Implement a Loading Pattern for Streaming Data

### 4.1. Tổng quan Streaming Data trong Fabric

Microsoft Fabric hỗ trợ hai mô hình xử lý streaming:

| Mô hình | Công cụ | Latency | Use Case |
|---------|---------|---------|----------|
| **Real-time streaming** | Eventstream + Eventhouse | Giây | IoT, alerting, dashboards real-time |
| **Near-real-time (micro-batch)** | Spark Structured Streaming | Phút | Log processing, incremental ETL |

### 4.2. Kiến trúc Streaming Data Loading

```
                                    ┌─────────────────┐
                                    │  Real-Time       │
                                ┌───►│  Dashboard (KQL) │
                                │    └─────────────────┘
┌──────────┐    ┌───────────┐  │    ┌─────────────────┐
│ Event Hub │───►│Eventstream│──┼───►│  Eventhouse      │
│ / Kafka   │    │           │  │    │  (KQL Database)  │
└──────────┘    └───────────┘  │    └─────────────────┘
                                │    ┌─────────────────┐
                                └───►│  Lakehouse       │
                                     │  (Delta Table)   │
                                     └────────┬────────┘
                                              │
                                     ┌────────▼────────┐
                                     │  Spark Notebook  │
                                     │  (Structured     │
                                     │   Streaming)     │
                                     └─────────────────┘
```

### 4.3. Loading Pattern cho Eventstream → Eventhouse

**Bước 1**: Tạo Eventstream kết nối nguồn (Event Hub, Kafka, Custom App)
**Bước 2**: Cấu hình destination là KQL Database trong Eventhouse
**Bước 3**: Map schema và chọn data format (JSON, Avro)
**Bước 4**: Tùy chỉnh ingestion mapping

> 📌 **Điểm thi**: Eventstream cho phép **split stream** (1 nguồn → nhiều đích) và **filter** trước khi ghi vào destination.

### 4.4. Loading Pattern cho Spark Structured Streaming

```python
# Đọc streaming data từ Event Hub
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# Định nghĩa schema
schema = StructType() \
    .add("DeviceId", StringType()) \
    .add("Temperature", DoubleType()) \
    .add("Humidity", DoubleType()) \
    .add("EventTime", TimestampType())

# Đọc từ Event Hub (streaming)
raw_stream = spark.readStream \
    .format("eventhubs") \
    .options(**eventhub_config) \
    .load()

# Parse JSON body
parsed_stream = raw_stream \
    .select(from_json(col("body").cast("string"), schema).alias("data")) \
    .select("data.*")

# Ghi vào Lakehouse Delta Table
query = parsed_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "abfss://<path>/_checkpoint/iot_events") \
    .trigger(processingTime="1 minute") \
    .toTable("bronze.iot_events")
```

### 4.5. Trigger Options cho Streaming

| Trigger | Mô tả | Khi nào dùng |
|---------|--------|-------------|
| `processingTime="1 minute"` | Micro-batch mỗi 1 phút | Streaming liên tục, cân bằng latency/cost |
| `availableNow=True` | Xử lý tất cả dữ liệu có sẵn rồi dừng | Scheduled incremental load (tiết kiệm cost) |
| `once=True` | Chạy 1 micro-batch duy nhất rồi dừng | Testing, one-time catch-up |
| Continuous (experimental) | True millisecond latency | Hiếm khi dùng, experimental |

> 📌 **Best Practice 2026**: Ưu tiên `trigger(availableNow=True)` cho pipeline chạy theo schedule. Nó xử lý tất cả data tồn đọng thành micro-batches rồi tự shutdown → tiết kiệm compute cost.

### 4.6. Checkpointing — Đảm bảo Exactly-Once

- **Bắt buộc** phải chỉ định `checkpointLocation` cho mọi writeStream
- Mỗi streaming query phải có **checkpoint riêng biệt** — KHÔNG chia sẻ
- Checkpoint lưu trữ:
  - Offset đã đọc từ nguồn
  - State của windowed aggregations
  - Metadata xử lý

```python
# Mỗi streaming query phải có checkpoint riêng
query1 = (
    stream_a.writeStream
    .format("delta")
    .option("checkpointLocation", "Files/checkpoints/query1")
    .toTable("bronze.stream_a")
)

query2 = (
    stream_b.writeStream
    .format("delta")
    .option("checkpointLocation", "Files/checkpoints/query2")
    .toTable("bronze.stream_b")
)
```

---

## 5. Câu hỏi mô phỏng bài thi (Practice Questions)

### Question 1

**You are a data engineer building an incremental data load pipeline in Microsoft Fabric. The source system is an Azure SQL Database that tracks customer orders. The database does NOT have Change Data Capture (CDC) enabled. Each order record has a `LastModifiedDate` column that is updated whenever a record is inserted or updated.**

**You need to design a pipeline that loads only new and changed records into a Fabric Lakehouse table while minimizing cost and compute usage.**

**Which approach should you use?**

A. Configure mirroring between Azure SQL Database and the Lakehouse  
B. Use a Copy Activity with a full table copy and overwrite mode on each pipeline run  
C. Use a Copy Activity with a watermark-based incremental pattern that stores the last `LastModifiedDate` in a control table  
D. Use a Dataflow Gen2 with a scheduled full refresh

**✅ Correct Answer: C**

> **Giải thích**: 
> - **(C) Đúng** — Watermark-based incremental load sử dụng cột `LastModifiedDate` là pattern phù hợp nhất khi nguồn không có CDC. Pipeline lưu giá trị watermark cao nhất từ lần chạy trước vào control table, và lần chạy tiếp theo chỉ lấy records có `LastModifiedDate` > watermark đã lưu.
> - **(A) Sai** — Mirroring yêu cầu cấu hình riêng và không phải lúc nào cũng là giải pháp tối ưu khi chỉ cần sync 1 bảng đơn giản. Mirroring phù hợp hơn khi cần replicate nhiều bảng liên tục.
> - **(B) Sai** — Full copy mỗi lần chạy tốn nhiều compute và thời gian, đặc biệt khi bảng lớn.
> - **(D) Sai** — Dataflow Gen2 full refresh cũng tải toàn bộ dữ liệu mỗi lần, không tiết kiệm cost.

---

### Question 2

**You are building a dimensional model in a Fabric Lakehouse for a retail analytics solution. The customer dimension requires tracking historical changes to customer addresses for reporting purposes.**

**Which Slowly Changing Dimension (SCD) type should you implement?**

A. SCD Type 0 — Fixed  
B. SCD Type 1 — Overwrite  
C. SCD Type 2 — Add new row with effective dating  
D. SCD Type 3 — Add new column

**✅ Correct Answer: C**

> **Giải thích**:
> - **(C) Đúng** — SCD Type 2 tạo một bản ghi mới mỗi khi có thay đổi, giữ nguyên bản ghi cũ với `ValidTo` date và `IsCurrent = false`. Đây là phương pháp duy nhất cho phép phân tích **lịch sử thay đổi** (historical tracking) — ví dụ: "Doanh thu theo địa chỉ khách hàng tại thời điểm mua hàng".
> - **(A) Sai** — SCD Type 0 không bao giờ thay đổi, không phù hợp khi dữ liệu nguồn thay đổi.
> - **(B) Sai** — SCD Type 1 ghi đè giá trị cũ, mất hoàn toàn lịch sử.
> - **(D) Sai** — SCD Type 3 chỉ lưu giá trị "trước" và "sau" (2 cột), không lưu toàn bộ lịch sử thay đổi.

---

### Question 3

**You have a Spark Structured Streaming notebook in Microsoft Fabric that processes IoT sensor data from Azure Event Hubs and writes to a Delta table. The pipeline runs on a schedule every 6 hours.**

**You need to ensure that the streaming job processes all available data since the last run and then automatically stops to minimize compute costs.**

**Which trigger configuration should you use?**

A. `trigger(processingTime="10 seconds")`  
B. `trigger(once=True)`  
C. `trigger(availableNow=True)`  
D. `trigger(continuous="1 second")`

**✅ Correct Answer: C**

> **Giải thích**:
> - **(C) Đúng** — `availableNow=True` xử lý **tất cả dữ liệu hiện có** thành nhiều micro-batches rồi tự động dừng. Đây là lựa chọn tối ưu cho pipeline chạy theo schedule vì nó vừa đảm bảo xử lý hết data, vừa tự shutdown để tiết kiệm compute.
> - **(A) Sai** — `processingTime` giữ Spark session chạy liên tục, tốn compute khi không có data mới.
> - **(B) Sai** — `once=True` chỉ chạy **1 micro-batch duy nhất**, có thể không xử lý hết tất cả data tồn đọng nếu lượng data lớn.
> - **(D) Sai** — Continuous trigger là experimental và giữ compute chạy liên tục.

---

### Question 4

**You are designing a data pipeline in Microsoft Fabric. The source is an operational Azure SQL Database with 50 tables. You need near real-time data freshness in Fabric without building and maintaining complex ETL pipelines.**

**Which approach should you recommend?**

A. Create 50 individual Copy Activities in a pipeline  
B. Use Dataflows Gen2 to connect to each table  
C. Configure database mirroring between Azure SQL Database and Fabric  
D. Create OneLake shortcuts to the Azure SQL Database

**✅ Correct Answer: C**

> **Giải thích**:
> - **(C) Đúng** — Database Mirroring tự động replicate toàn bộ (hoặc selected) tables từ Azure SQL vào OneLake dưới dạng Delta format. Đây là giải pháp "zero-ETL" — không cần build pipeline, tự động near-real-time, và bắt được INSERT/UPDATE/DELETE.
> - **(A) Sai** — 50 Copy Activities phức tạp, khó maintain, và tốn effort.
> - **(B) Sai** — Dataflow Gen2 cho 50 bảng cũng phức tạp và không near-real-time.
> - **(D) Sai** — OneLake shortcuts KHÔNG hỗ trợ kết nối trực tiếp tới Azure SQL Database. Shortcuts chỉ hỗ trợ ADLS Gen2, S3, GCS, Dataverse, OneLake internal.

---

### Question 5

**You need to load data into a fact table in a Fabric Lakehouse gold layer. The fact table references three dimension tables: DimDate, DimCustomer, and DimProduct. Each dimension uses surrogate keys.**

**In which order should you load the tables?**

A. Load the fact table first, then the dimension tables  
B. Load the dimension tables first, then the fact table  
C. Load all tables simultaneously in parallel  
D. Load the fact table first, then run a stored procedure to generate surrogate keys

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — Dimension tables phải được load **trước** fact table vì fact table cần thực hiện lookup các Surrogate Key từ dimension tables. Nếu dimension chưa có data, fact table sẽ có NULL foreign keys.
> - **(A) Sai** — Không thể lookup surrogate keys nếu dimensions chưa tồn tại.
> - **(C) Sai** — Load song song có thể gây ra race condition khi fact table cần dimension keys.
> - **(D) Sai** — Surrogate keys thuộc về dimension tables, không phải fact tables.

---

### Question 6

**You are configuring a Spark Structured Streaming job that reads from Event Hubs and writes to a Lakehouse Delta table. You have two separate streaming queries writing to two different tables.**

**What is the correct approach for configuring checkpoint locations?**

A. Use the same checkpoint location for both queries to ensure consistency  
B. Use a unique checkpoint location for each streaming query  
C. Checkpoint locations are optional and can be omitted for better performance  
D. Store checkpoints in a temporary directory that is cleared after each run

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — Mỗi streaming query **BẮT BUỘC** phải có checkpoint location riêng biệt. Checkpoint lưu trữ offset và state duy nhất cho từng query. Chia sẻ checkpoint giữa 2 queries sẽ gây lỗi hoặc data loss.
> - **(A) Sai** — Chia sẻ checkpoint gây conflict metadata giữa hai queries.
> - **(C) Sai** — Checkpoint là **BẮT BUỘC** (mandatory) cho fault tolerance.
> - **(D) Sai** — Xóa checkpoint sau mỗi lần chạy sẽ khiến query xử lý lại toàn bộ dữ liệu từ đầu, mất khả năng resume.

---

### Question 7

**A company wants to implement an incremental load pattern using the MERGE INTO statement in a Fabric Lakehouse notebook. After running a large MERGE operation that updates millions of rows, they notice query performance has degraded significantly.**

**What should they do to resolve the performance issue?**

A. Increase the Spark cluster size  
B. Run the `OPTIMIZE` command on the Delta table  
C. Convert the table from Delta to Parquet format  
D. Recreate the table with a different file format

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — MERGE operations tạo nhiều small files trong Delta table. Lệnh `OPTIMIZE` hợp nhất (compaction) các small files thành files lớn hơn, cải thiện query performance đáng kể. Đây là best practice sau mọi MERGE lớn.
> - **(A) Sai** — Tăng cluster không giải quyết vấn đề small files.
> - **(C) Sai** — Chuyển sang Parquet mất tất cả lợi ích của Delta (ACID, time-travel, MERGE).
> - **(D) Sai** — Tạo lại bảng không cần thiết khi OPTIMIZE giải quyết được vấn đề.

---

### Question 8

**You are designing a streaming data loading pattern for a Fabric solution. The requirement is to ingest real-time telemetry data with sub-second latency for a live operational dashboard.**

**Which loading pattern should you implement?**

A. Spark Structured Streaming with `trigger(processingTime="1 minute")`  
B. Eventstream → Eventhouse (KQL Database) with native table ingestion  
C. Data Factory pipeline with a 5-minute schedule trigger  
D. Dataflow Gen2 with incremental refresh

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — Eventstream → Eventhouse cung cấp **sub-second latency** cho real-time ingestion. Dữ liệu được index và sẵn sàng query bằng KQL ngay lập tức. Kết hợp với Real-Time Dashboard cho live monitoring.
> - **(A) Sai** — Spark Structured Streaming có micro-batch latency tối thiểu hàng giây đến phút, không đạt sub-second.
> - **(C) Sai** — Pipeline schedule 5 phút là batch, không phải real-time.
> - **(D) Sai** — Dataflow Gen2 là batch processing, không hỗ trợ streaming.

---

*Tài liệu được tổng hợp dựa trên Skills Measured as of July 21, 2026*  
*Nguồn tham khảo: Microsoft Learn, Microsoft Fabric Documentation*


---

## 6. BỔ SUNG ÔN THI 2026: IDEMPOTENT INCREMENTAL LOAD

### 6.1 PySpark MERGE có dedup trước khi upsert

```python
from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number

incoming = spark.table("bronze.orders_changes")
w = Window.partitionBy("OrderId").orderBy(col("ModifiedDate").desc())
latest = incoming.withColumn("rn", row_number().over(w)).filter("rn = 1").drop("rn")

target = DeltaTable.forName(spark, "silver.orders")
(
    target.alias("t")
    .merge(latest.alias("s"), "t.OrderId = s.OrderId")
    .whenMatchedUpdateAll(condition="s.ModifiedDate >= t.ModifiedDate")
    .whenNotMatchedInsertAll()
    .execute()
)
```

### 6.2 Bẫy thi

- Watermark control table chỉ được cập nhật sau khi load/merge thành công.
- Dùng khoảng `(last_watermark, current_high_watermark]` để rerun có kiểm soát.
- `overwrite` hợp lệ cho full snapshot, nhưng không phải row-level incremental pattern.
- Late-arriving dimensions thường cần unknown member và bước sửa foreign key sau khi dimension đến.

### 6.3 Câu hỏi tình huống bổ sung

**Pipeline đọc watermark cuối, copy dữ liệu mới rồi lỗi trước khi cập nhật control table. Lần chạy lại phải không tạo duplicate. Thiết kế nào tốt nhất?**

A. Append trực tiếp không khóa  
B. Ghi staging theo run id, dedup/MERGE vào target, chỉ cập nhật watermark sau commit thành công  
C. Xóa toàn bộ Delta log  
D. VACUUM RETAIN 0 HOURS trước mỗi run

<details><summary>Đáp án</summary>

**B.** Thiết kế idempotent tách ingestion/staging khỏi merge và commit watermark.
</details>


---

## NGUỒN CHÍNH THỨC ĐÃ ĐỐI CHIẾU

- [DP-700 study guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)
- [Replace Delta tables](https://learn.microsoft.com/en-us/fabric/data-engineering/delta-lake-replace)
- [Structured Streaming overview](https://learn.microsoft.com/en-us/fabric/data-engineering/structured-streaming-overview)
