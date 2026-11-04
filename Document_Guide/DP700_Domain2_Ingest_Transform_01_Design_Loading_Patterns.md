# DP-700: Design and Implement Loading Patterns
## Ingest and Transform Data (30–35%) — Phần 1/3


> [!IMPORTANT]
> **Bản cập nhật ngày 05/11/2026.** Đối chiếu với [Study Guide DP-700](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700) (Microsoft cập nhật trang ngày **30/10/2026**) và tài liệu Microsoft Learn tháng 10–11/2026. Bản trước (09/08/2026) dựa trên blueprint 21/07/2026; những gì đã đổi nằm ở chương **⚡ CẬP NHẬT 05/11/2026** cuối file và ở đầu file [MASTER](DP700_00_MASTER_Trong_Tam_On_Thi.md). Microsoft cho biết phần lớn câu hỏi tập trung vào tính năng GA, nhưng tính năng Preview phổ biến vẫn có thể xuất hiện. Với các bảng “supported items”, runtime, limits và trạng thái Preview/GA, hãy kiểm tra Microsoft Learn lần cuối trước ngày thi.

> **Đối chiếu Study Guide DP-700 — trang cập nhật 30/10/2026**  
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
- **Initial load / bootstrap** lần đầu.
- Dataset đủ nhỏ để tải lại toàn bộ trong SLA và chi phí chấp nhận được; **không dùng một ngưỡng row cố định**.
- Nguồn cung cấp full snapshot mỗi lần và không có reliable change signal.
- Bảng lookup/dimension nhỏ, full refresh đơn giản hơn incremental.
- Cần phản ánh cả DELETE nhưng nguồn không có CDC/change tracking; full snapshot + replace có thể phù hợp hơn.

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

Incremental load chỉ xử lý dữ liệu **mới hoặc thay đổi** kể từ mốc thành công trước. Một design tốt phải đồng thời giải quyết **state, retry, duplicate, stale update và delete**.

#### 2.2.1. Mental model bắt buộc

| Khái niệm | Trả lời câu hỏi nào? | DELETE? |
|---|---|---|
| **Watermark** | Đọc record nào kể từ lần trước? | Không tự nhiên |
| **CDC** | Operation nào đã xảy ra: insert/update/delete? | Có |
| **MERGE/Upsert** | Ghi change set vào target thế nào? | Chỉ khi có delete signal + logic delete |
| **SCD Type 2** | Có cần giữ lịch sử version không? | Có thể soft-delete/expire |

> [!IMPORTANT]
> **Watermark/CDC là read pattern; MERGE/SCD là write pattern.** Đừng coi chúng là các đáp án thay thế ngang hàng trong mọi scenario.

#### A. Watermark pattern — OldWatermark + NewWatermark

Pattern an toàn:

```text
1. Lookup OldWatermark từ control table
2. Lookup NewWatermark = MAX(ModifiedDate) tại SOURCE
3. Copy WHERE ModifiedDate > OldWatermark
          AND ModifiedDate <= NewWatermark
4. Dedup + MERGE/Upsert vào target
5. Chỉ sau SUCCESS mới commit control table = NewWatermark
```

Cận trên `<= NewWatermark` tạo một “cut” nhất quán; source thay đổi trong lúc run sẽ được xử lý ở lần sau.

**T-SQL control table:**

```sql
CREATE TABLE dbo.LoadWatermark (
    TableName varchar(128) NOT NULL,
    LastWatermark datetime2(6) NULL
);

SELECT LastWatermark
FROM dbo.LoadWatermark
WHERE TableName = 'Orders';

SELECT MAX(ModifiedDate) AS NewWatermark
FROM dbo.Orders;

SELECT *
FROM dbo.Orders
WHERE ModifiedDate > @OldWatermark
  AND ModifiedDate <= @NewWatermark;
```

**PySpark dedup + idempotent MERGE:**

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

incoming = spark.table('bronze.orders_increment')
w = Window.partitionBy('OrderId').orderBy(F.col('ModifiedDate').desc())
latest = (incoming
          .withColumn('_rn', F.row_number().over(w))
          .filter('_rn = 1')
          .drop('_rn'))

target = DeltaTable.forName(spark, 'silver.orders')
(target.alias('t')
 .merge(latest.alias('s'), 't.OrderId = s.OrderId')
 .whenMatchedUpdateAll(condition='s.ModifiedDate >= t.ModifiedDate')
 .whenNotMatchedInsertAll()
 .execute())
```

> [!WARNING]
> Watermark không tự phát hiện **hard DELETE**. Nếu DELETE quan trọng, ưu tiên CDC/Mirroring hoặc soft-delete/snapshot reconciliation.

#### B. Copy Job incremental — Fabric quản lý state

Copy Job có **Full copy** và **Incremental copy**. Với incremental mode, Fabric quản lý mốc của **last successful run**; failed run không advance state. Copy Job hỗ trợ watermark-based incremental và CDC-based incremental khi connector hỗ trợ.

Update methods hiện gồm **Append, Overwrite, Merge**; với supported CDC paths có **SCD Type 2 (Preview)**. Copy Job còn có audit columns và auto-partitioning (Preview) cho một số connector.

#### C. CDC

CDC phù hợp khi cần INSERT + UPDATE + DELETE và source/connector hỗ trợ. Trong Copy Job:

- **Merge / SCD Type 1**: target giữ current state; source delete có thể xóa target row.
- **SCD Type 2 (Preview)**: update tạo version mới; delete expire/soft-delete current version.

Support matrix thay đổi nhanh (xem chương **⚡ CẬP NHẬT 05/11/2026** cuối file cho bảng đối chiếu 09/2026). Tính đến 08/2026 Microsoft tài liệu hóa CDC cho các nguồn như Azure SQL DB, Azure SQL MI, Fabric Warehouse, Fabric Lakehouse table (Preview), Google BigQuery (Preview), SQL Server on-prem, Oracle (Preview), SAP Datasphere outbound; Snowflake có tutorial CDC riêng.

#### D. MERGE / Upsert

**Warehouse / T-SQL:**

```sql
MERGE INTO dbo.Customer AS T
USING dbo.CustomerStage AS S
ON T.CustomerId = S.CustomerId
WHEN MATCHED AND S.ModifiedDate >= T.ModifiedDate THEN
  UPDATE SET
    CustomerName = S.CustomerName,
    Email = S.Email,
    ModifiedDate = S.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
  INSERT (CustomerId, CustomerName, Email, ModifiedDate)
  VALUES (S.CustomerId, S.CustomerName, S.Email, S.ModifiedDate);
```

**Lakehouse / Delta PySpark:**

```python
from delta.tables import DeltaTable

target = DeltaTable.forName(spark, 'silver.customer')
source = spark.table('bronze.customer_stage')

(target.alias('t')
 .merge(source.alias('s'), 't.CustomerId = s.CustomerId')
 .whenMatchedUpdateAll(condition='s.ModifiedDate >= t.ModifiedDate')
 .whenNotMatchedInsertAll()
 .execute())
```

Delete cần tín hiệu + explicit logic, ví dụ:

```python
.whenMatchedDelete(condition="s.Operation = 'DELETE'")
```

#### E. `OPTIMIZE` sau MERGE?

**Không chạy theo thói quen sau mọi MERGE.** Chỉ cân nhắc khi small files/query scan trở thành bottleneck hoặc có maintenance window phù hợp. Fabric còn có automatic/managed optimization ở một số workload.

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

- **Natural/business key** đến từ source (`CustomerId`, `ProductCode`).
- **Surrogate key** do dimensional model quản lý, độc lập với source và đặc biệt hữu ích cho SCD Type 2.

> [!IMPORTANT]
> Integer/BIGINT là lựa chọn phổ biến nhưng trọng tâm đề thi là **độc lập với source + stable relationship + version history**, không phải “surrogate key bắt buộc luôn là integer”.

#### Không dùng `monotonically_increasing_id()` như identity generator mặc định

Hàm này unique trong DataFrame nhưng không liên tiếp, phụ thuộc partition và không phải sequence bền vững giữa nhiều run.

Pattern lab **single-writer** dễ hiểu:

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

source = spark.table('silver.customer')
target = spark.table('gold.dim_customer')

existing = target.select('CustomerId').distinct()
new_rows = (source.dropDuplicates(['CustomerId'])
            .join(existing, 'CustomerId', 'left_anti'))

max_sk = target.agg(F.coalesce(F.max('CustomerSK'), F.lit(0))).first()[0]
w = Window.orderBy('CustomerId')

new_rows = new_rows.withColumn(
    'CustomerSK', F.row_number().over(w) + F.lit(max_sk)
)

new_rows.write.format('delta').mode('append').saveAsTable('gold.dim_customer')
```

Với production concurrency, cần key-allocation/serialization đáng tin cậy hơn; exam tập trung vào concept và loading order.

### 3.3. Slowly Changing Dimensions (SCD)

#### SCD Type 1 — overwrite current value

Không giữ lịch sử:

```sql
MERGE INTO dbo.DimCustomer AS T
USING dbo.CustomerStage AS S
ON T.CustomerId = S.CustomerId
WHEN MATCHED THEN
  UPDATE SET CustomerName=S.CustomerName, Phone=S.Phone
WHEN NOT MATCHED THEN
  INSERT (CustomerId, CustomerName, Phone)
  VALUES (S.CustomerId, S.CustomerName, S.Phone);
```

#### SCD Type 2 — preserve version history

Cột điển hình: `CustomerSK`, `CustomerId`, `ValidFrom`, `ValidTo`, `IsCurrent`.

**Bẫy:** xác định changed rows **trước khi expire** current rows.

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

src = spark.table('silver.customer')
cur = (spark.table('gold.dim_customer')
       .filter('IsCurrent = true')
       .select('CustomerSK','CustomerId',
               F.col('City').alias('OldCity'),
               F.col('Segment').alias('OldSegment')))

cmp = src.alias('s').join(
    cur.alias('t'),
    F.col('s.CustomerId') == F.col('t.CustomerId'),
    'left'
)
changed = (cmp.filter(
              F.col('t.CustomerId').isNotNull() &
              ((F.coalesce(F.col('s.City'),F.lit('')) != F.coalesce(F.col('t.OldCity'),F.lit(''))) |
               (F.coalesce(F.col('s.Segment'),F.lit('')) != F.coalesce(F.col('t.OldSegment'),F.lit(''))))
           ).select('s.*'))
new_only = cmp.filter(F.col('t.CustomerId').isNull()).select('s.*')

# 1) expire changed current versions
target = DeltaTable.forName(spark, 'gold.dim_customer')
(target.alias('t')
 .merge(changed.select('CustomerId').distinct().alias('s'),
        't.CustomerId=s.CustomerId AND t.IsCurrent=true')
 .whenMatchedUpdate(set={'IsCurrent':'false','ValidTo':'current_timestamp()'})
 .execute())

# 2) insert new + changed versions
ins = new_only.unionByName(changed)
max_sk = spark.table('gold.dim_customer').agg(
    F.coalesce(F.max('CustomerSK'),F.lit(0)).alias('m')).first()['m']
w = Window.orderBy('CustomerId')

ins = (ins
       .withColumn('CustomerSK',F.row_number().over(w)+F.lit(max_sk))
       .withColumn('ValidFrom',F.current_timestamp())
       .withColumn('ValidTo',F.lit(None).cast('timestamp'))
       .withColumn('IsCurrent',F.lit(True)))

ins.write.format('delta').mode('append').saveAsTable('gold.dim_customer')
```

#### Copy Job SCD Type 2

CDC Copy Job hỗ trợ **SCD Type 2 (Preview)**: Fabric tạo/duy trì `Valid_From`, `Valid_To`, `Is_Current`; source delete được xử lý như expire/soft-delete version. Khi đề nói **no-code + CDC + historical versions**, đây là lựa chọn cần nhớ nếu connector hỗ trợ.

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
| 5 | Cân nhắc `OPTIMIZE` | Chỉ khi small files/query scan là bottleneck; không chạy máy móc sau mọi load |

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

### 4.1. Hai hướng chính

| Hướng | Công cụ | Phù hợp |
|---|---|---|
| Real-Time Intelligence | Eventstream + Eventhouse + KQL | low-code routing, dashboard, alert |
| Data Engineering | Spark Structured Streaming | code-first ETL, stateful transform, Delta |

> [!IMPORTANT]
> Fabric Runtime **2.0 (Spark 4.1)** đã có **Real-time Mode** cho ultra-low-latency trên topology được hỗ trợ. Không còn đúng nếu học “Spark streaming luôn micro-batch/phút”.

### 4.2. Eventstream pattern

```text
Sources → Eventstream operators → Eventhouse / Lakehouse / Activator / Stream / Custom endpoint
                              └→ Spark Notebook destination (Preview)
```

Các operators quan trọng: Filter, Manage Fields, Aggregate/Group By, Join, Union, Expand, SQL operator (Preview).

### 4.3. Spark Structured Streaming — built-in file source

Fabric tài liệu hóa file sources `text/csv/json/parquet/orc`, Delta, Kafka, Event Hubs/Fabric Eventstream qua compatible connectors/endpoints và `rate` cho test.

```python
from pyspark.sql.types import *
from pyspark.sql import functions as F

schema=StructType([
    StructField('DeviceId',StringType()),
    StructField('Temperature',DoubleType()),
    StructField('EventTime',TimestampType())
])

stream=(spark.readStream
        .schema(schema)
        .format('json')
        .option('maxFilesPerTrigger',100)
        .load('Files/incoming/iot'))

q=(stream.writeStream
   .format('delta')
   .outputMode('append')
   .option('checkpointLocation','Files/checkpoints/iot')
   .trigger(availableNow=True)
   .toTable('bronze_iot'))
q.awaitTermination()
```

> [!WARNING]
> `cloudFiles` / Databricks Auto Loader không phải Fabric Spark API được Microsoft Learn tài liệu hóa cho DP-700. Học built-in file source ở Fabric.

### 4.4. Execution/trigger model

| Mode | Dùng khi |
|---|---|
| Micro-batch default/fixed interval | always-on general streaming |
| `availableNow=True` | scheduled incremental: process backlog rồi stop |
| Real-time Mode | Runtime 2.0+, supported ultra-low-latency Spark topology |

Real-time Mode hiện yêu cầu output mode `update`; PySpark preview chưa expose `Trigger.RealTime` trực tiếp trong `.trigger()`.

### 4.5. Checkpoint, fault tolerance, exactly-once

- Mỗi query dùng checkpoint riêng trên durable OneLake path.
- Checkpoint giữ offsets/progress/state.
- Supported sink như Delta + checkpoint hỗ trợ fault-tolerant exactly-once processing semantics.
- `foreachBatch`/custom sink có thể bị retry → write logic phải **idempotent**.

```python
def upsert_batch(batch_df,batch_id):
    from delta.tables import DeltaTable
    tgt=DeltaTable.forName(spark,'silver_orders')
    (tgt.alias('t')
     .merge(batch_df.alias('s'),'t.OrderId=s.OrderId')
     .whenMatchedUpdateAll(condition='s.ModifiedDate >= t.ModifiedDate')
     .whenNotMatchedInsertAll()
     .execute())

q=(stream_df.writeStream
   .foreachBatch(upsert_batch)
   .option('checkpointLocation','Files/checkpoints/orders-upsert')
   .trigger(availableNow=True)
   .start())
q.awaitTermination()
```

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
D. Bật Real-time Mode dù workload chỉ chạy theo schedule

**✅ Correct Answer: C**

> **Giải thích**:
> - **(C) Đúng** — `availableNow=True` xử lý **tất cả dữ liệu hiện có** thành nhiều micro-batches rồi tự động dừng. Đây là lựa chọn tối ưu cho pipeline chạy theo schedule vì nó vừa đảm bảo xử lý hết data, vừa tự shutdown để tiết kiệm compute.
> - **(A) Sai** — `processingTime` giữ Spark session chạy liên tục, tốn compute khi không có data mới.
> - **(B) Sai** — `once=True` chỉ chạy **1 micro-batch duy nhất**, có thể không xử lý hết tất cả data tồn đọng nếu lượng data lớn.
> - **(D) Sai** — Real-time Mode dành cho always-on low-latency supported topology; không phù hợp yêu cầu scheduled catch-up rồi stop.

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
> - **(C) Sai trong production** — Muốn recover progress/state sau lỗi thì phải dùng durable checkpoint; mỗi query phải có checkpoint riêng. Một số sink thử nghiệm như console/memory có thể chạy không khai báo checkpoint, nhưng không cung cấp recovery bền vững và không phải thiết kế production.
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
> - **(B) Đúng trong scenario này** — Sau large MERGE, triệu chứng query chậm cần được xác nhận là small-file/scan problem; khi đúng, `OPTIMIZE` compact các file nhỏ. Không chạy `OPTIMIZE` máy móc sau mọi MERGE: hãy đo số/kích thước file và query scan trước.
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
> - **(B) Đúng** — Eventstream → Eventhouse phù hợp low-code, low-latency real-time analytics; không coi “sub-second” là SLA mặc định cho mọi topology. Dữ liệu được index và sẵn sàng query bằng KQL ngay lập tức. Kết hợp với Real-Time Dashboard cho live monitoring.
> - **(A) Sai** — Spark mặc định dùng micro-batch, nhưng Runtime 2.0 còn có Real-time Mode. Trong scenario low-code dashboard này Eventstream/Eventhouse vẫn là lựa chọn phù hợp hơn.
> - **(C) Sai** — Pipeline schedule 5 phút là batch, không phải real-time.
> - **(D) Sai** — Dataflow Gen2 là batch processing, không hỗ trợ streaming.

---

*Tài liệu được tổng hợp dựa trên Study Guide DP-700 (trang cập nhật 30/10/2026); bản trước dựa trên Skills Measured 21/07/2026*  
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

## 7. CÂU HỎI SCENARIO — LOADING PATTERNS

> Các câu hỏi dưới đây là câu tự soạn theo blueprint DP-700, **không phải câu hỏi thật hoặc exam dump**.

### Câu D2-LP1 — Idempotent Incremental Load

**Pipeline incremental bị lỗi sau khi thực hiện MERGE nhưng trước khi cập nhật watermark trong control table. Khi chạy lại, pipeline phải không tạo dữ liệu trùng. Thiết kế nào đảm bảo điều này?**

A. Append trực tiếp vào target, không có dedup  
B. Ghi dữ liệu mới vào staging table → dedup/MERGE vào target → **chỉ** cập nhật watermark sau khi MERGE thành công  
C. Xoá toàn bộ Delta transaction log trước khi rerun  
D. Chạy `VACUUM RETAIN 0 HOURS` trước mỗi lần chạy  

<details><summary>Đáp án và giải thích</summary>

**B.** Thiết kế **idempotent** (có thể rerun an toàn) yêu cầu:
1. **Staging** — dữ liệu mới vào bảng staging riêng (tách khỏi target)
2. **MERGE có điều kiện xác định** — upsert từ staging đã dedup vào target theo unique business key; cùng input hợp lệ sẽ update đúng row thay vì insert thêm
3. **Watermark commit cuối cùng** — chỉ update control table sau khi MERGE thành công

> `MERGE` **không tự thân luôn idempotent**. Nếu source có nhiều row cùng match một target key, operation có thể lỗi/không xác định; nếu dùng nondeterministic values hoặc business rule khác nhau giữa các lần chạy, kết quả cũng có thể đổi. Công thức thi an toàn là: **dedup source + deterministic unique key + điều kiện update ổn định + commit watermark sau success**. Xem [Delta MERGE semantics](https://learn.microsoft.com/en-us/azure/databricks/delta/merge#merge-operation-semantics).

Tham khảo: [Delta Lake MERGE](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-streaming-write)

</details>

---

### Câu D2-LP2 — Full Snapshot Thay Thế

**Nhận được một bản snapshot đầy đủ mới cần thay thế toàn bộ dữ liệu cũ, nhưng vẫn phải giữ table identity (tên bảng, schema) và lịch sử Delta để time travel vẫn hoạt động. Cách làm nào phù hợp?**

A. DROP TABLE rồi CREATE TABLE mới  
B. Dùng `CREATE OR REPLACE TABLE` hoặc PySpark `.mode("overwrite")` với Delta format  
C. Chạy `VACUUM 0 HOURS` để reset  
D. Xoá trực tiếp thư mục `_delta_log`  

<details><summary>Đáp án và giải thích</summary>

**B.** [`CREATE OR REPLACE TABLE`](https://learn.microsoft.com/en-us/fabric/data-engineering/delta-lake-replace) tạo **Delta version mới** — dữ liệu cũ bị thay thế nhưng **transaction log được giữ nguyên**, cho phép time travel đến các version trước (trong giới hạn retention). DROP TABLE mất hoàn toàn identity và history. Xoá `_delta_log` là thao tác nguy hiểm, không được hỗ trợ.

</details>

---

### Câu D2-LP3 — Late-Arriving Dimension

**Một fact record đến trước khi dimension record tương ứng có mặt trong bảng `dim_customer`. Foreign key `CustomerId` chưa tồn tại. Pattern dimensional loading nào phù hợp?**

A. Bỏ fact record — chờ dimension đến mới load  
B. Dùng **unknown member** (ví dụ: `CustomerId = -1` đại diện "Unknown") → sau khi dimension đến, thực hiện key correction  
C. Cấp quyền Admin cho pipeline để tự tạo dimension  
D. Tắt relationship giữa fact và dimension  

<details><summary>Đáp án và giải thích</summary>

**B.** Pattern **unknown member + late correction** là giải pháp chuẩn cho dimensional modeling:
1. Load fact với surrogate key "Unknown" (-1 hoặc 0) thay vì bỏ record
2. Khi dimension record đến, chạy **key correction** để cập nhật fact records liên quan

Điều này đảm bảo fact records không bị mất và tính toàn vẹn referential được duy trì. Tham khảo: [Dimensional modeling best practices](https://learn.microsoft.com/en-us/fabric/data-warehouse/dimensional-modeling-overview)

</details>

---

## 8. LAB THỰC HÀNH — SCD TYPE 2

> Lab mô phỏng Slowly Changing Dimension Type 2 — pattern thường gặp trong đề thi DP-700.

### Lab 2 — Xây Dựng SCD Type 2 Với Delta Lake

**Mục tiêu:** Theo dõi lịch sử thay đổi của customer segment, mỗi thay đổi tạo ra một version mới trong bảng dimension.

**Bước 1: Khởi tạo bảng dimension ban đầu**

```python
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Dữ liệu khởi tạo
initial_data = spark.createDataFrame([
    (101, "Alice", "Gold"),
    (102, "Bob",   "Silver"),
], ["CustomerId", "Name", "Segment"])

# Thêm các cột SCD Type 2
dim_initial = (
    initial_data
    .withColumn("EffectiveFrom", F.current_timestamp())
    .withColumn("EffectiveTo",   F.lit(None).cast("timestamp"))
    .withColumn("IsCurrent",     F.lit(True))
)

dim_initial.write.format("delta").mode("overwrite").saveAsTable("gold.dim_customer")

# Kiểm tra
spark.table("gold.dim_customer").show()
```

**Bước 2: Xử lý thay đổi (Alice đổi từ Gold → Platinum)**

```python
from pyspark.sql import functions as F
from delta.tables import DeltaTable
import datetime

# Dữ liệu thay đổi mới
changes = spark.createDataFrame([
    (101, "Alice", "Platinum"),   # CustomerId 101 đổi Segment
], ["CustomerId", "Name", "Segment"])

now = F.current_timestamp()
target = DeltaTable.forName(spark, "gold.dim_customer")

# Bước 2a: Expire row cũ (set IsCurrent=False, EffectiveTo=now) cho các CustomerId thay đổi
(target.alias("t")
 .merge(
     changes.alias("s"),
     "t.CustomerId = s.CustomerId AND t.IsCurrent = true AND t.Segment != s.Segment"
 )
 .whenMatchedUpdate(set={
     "IsCurrent":   F.lit(False),
     "EffectiveTo": now,
 })
 .execute())

# Bước 2b: Insert version mới
new_rows = (
    changes
    .withColumn("EffectiveFrom", now)
    .withColumn("EffectiveTo",   F.lit(None).cast("timestamp"))
    .withColumn("IsCurrent",     F.lit(True))
)
new_rows.write.format("delta").mode("append").saveAsTable("gold.dim_customer")
```

**Bước 3: Kiểm tra kết quả**

```python
spark.table("gold.dim_customer").orderBy("CustomerId", "EffectiveFrom").show(truncate=False)
# Kết quả mong đợi cho Alice (CustomerId=101):
#   Row 1: Gold,     EffectiveFrom=T0, EffectiveTo=T1, IsCurrent=False
#   Row 2: Platinum, EffectiveFrom=T1, EffectiveTo=NULL, IsCurrent=True
# Bob (CustomerId=102) không thay đổi: IsCurrent=True, EffectiveTo=NULL
```

**Điểm quan trọng cần ghi nhớ:**

| Khái niệm | Giải thích |
|-----------|-----------|
| **IsCurrent = True** | Row hiện tại đang có hiệu lực |
| **EffectiveTo = NULL** | Row chưa bị expire |
| **Expire trước, insert sau** | Thứ tự quan trọng — tránh duplicate IsCurrent=True |
| **SCD Type 1** | Chỉ update trực tiếp, không giữ lịch sử |
| **SCD Type 2** | Giữ toàn bộ lịch sử, dùng EffectiveFrom/To + IsCurrent |

> [!TIP]
> Tham khảo: [Delta Lake MERGE documentation](https://learn.microsoft.com/en-us/azure/databricks/delta/merge) và [Lakehouse dimensional modeling](https://learn.microsoft.com/en-us/fabric/data-warehouse/dimensional-modeling-overview)

---

## 9. CODE CHEAT SHEET — LOADING PATTERNS

```text
Full snapshot?             → Full copy / Overwrite
Reliable ModifiedDate?     → Watermark
Need insert+update+delete? → CDC / Mirroring
Current state only?        → Merge / SCD1
Need history?              → SCD2
Retry-safe?                → Dedup + idempotent MERGE + watermark commit last
Scheduled stream catch-up? → availableNow + checkpoint
Low-code realtime route?   → Eventstream
```

**SQL latest-per-key:**

```sql
WITH R AS (
  SELECT *, ROW_NUMBER() OVER(PARTITION BY CustomerId ORDER BY ModifiedDate DESC) rn
  FROM dbo.CustomerStage
)
SELECT * FROM R WHERE rn=1;
```

**PySpark latest-per-key:**

```python
w=Window.partitionBy('CustomerId').orderBy(F.col('ModifiedDate').desc())
latest=df.withColumn('rn',F.row_number().over(w)).filter('rn=1').drop('rn')
```

---

## NGUỒN CHÍNH THỨC ĐÃ ĐỐI CHIẾU

- [DP-700 Study Guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)
- [What is Copy Job](https://learn.microsoft.com/en-us/fabric/data-factory/what-is-copy-job)
- [CDC in Copy Job](https://learn.microsoft.com/en-us/fabric/data-factory/cdc-copy-job)
- [Structured Streaming overview](https://learn.microsoft.com/en-us/fabric/data-engineering/structured-streaming-overview)
- [Structured Streaming best practices](https://learn.microsoft.com/en-us/fabric/data-engineering/structured-streaming-best-practices)
- [Real-time Mode](https://learn.microsoft.com/en-us/fabric/data-engineering/structured-streaming-real-time-mode)


- [DP-700 study guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)
- [Replace Delta tables](https://learn.microsoft.com/en-us/fabric/data-engineering/delta-lake-replace)
- [Structured Streaming overview](https://learn.microsoft.com/en-us/fabric/data-engineering/structured-streaming-overview)

---

---

# 🎯 CHƯƠNG BỔ SUNG 06/09/2026 — HỌC LẠI TỪ GỐC

> **Vì sao có chương này:** *Design and implement loading patterns* là nhóm kỹ năng bị hỏi ở mức **chi tiết triển khai**, không phải mức khái niệm. Đề hiếm khi hỏi "watermark là gì"; đề đưa một ràng buộc rồi hỏi "logic nào đúng", "vì sao lần chạy lại sinh dòng trùng", "cột nào phải có". Chương này viết lại phần cốt lõi theo hướng **dễ hiểu + code gõ được từ trí nhớ + tự kiểm tra**.
> Xem thêm: [File tổng chỉ huy](DP700_00_MASTER_Trong_Tam_On_Thi.md)

## A. HIỂU BẢN CHẤT TRONG 60 GIÂY

Mọi bài toán loading pattern chỉ là câu trả lời cho **bốn câu hỏi**:

```text
1. LẤY GÌ?      Toàn bộ (full)  hay  chỉ phần mới/đổi (incremental)?
                       │                        │
                       │                        ├── Nguồn có cột thời gian/ID tăng dần? → WATERMARK
                       │                        ├── Nguồn có CDC/change feed?           → CDC / CDF
                       │                        └── Nguồn chỉ cho snapshot?             → so sánh snapshot

2. GHI THẾ NÀO?  Overwrite  |  Append  |  Merge (upsert)  |  SCD Type 2

3. CHẠY LẠI CÓ AN TOÀN KHÔNG?   ← câu hỏi bị bỏ quên nhiều nhất, và là chỗ đề hay gài bẫy
                Dedup nguồn → match key duy nhất → điều kiện xác định → commit state SAU khi thành công

4. DỮ LIỆU BỊ XOÁ Ở NGUỒN THÌ SAO?   Soft delete flag | CDC delete | so sánh snapshot
```

Nếu bạn trả lời được bốn câu này cho một scenario bất kỳ, bạn đã giải xong câu hỏi.

**Ví von:** full load là **chép lại cả cuốn sổ mỗi ngày**; incremental là **chỉ chép những dòng mới viết thêm**. Watermark là **cái kẹp sách đánh dấu đã chép tới đâu** — và cái kẹp chỉ được dời **sau khi** chép xong an toàn, chứ không phải trước.

## B. FULL LOAD vs INCREMENTAL — CHỌN THẾ NÀO

| Tiêu chí | Full load | Incremental load |
|---|---|---|
| Khối lượng dữ liệu | Nhỏ / vừa | Lớn |
| Nguồn có cột thay đổi đáng tin? | Không cần | **Bắt buộc** (timestamp/ID) hoặc phải có CDC |
| Xử lý delete ở nguồn | Tự nhiên đúng (nạp lại hết) | Phải xử lý riêng |
| Chi phí CU | Cao và tăng theo thời gian | Thấp, ổn định |
| Độ phức tạp vận hành | Thấp | Cao (state, retry, dedup) |
| Rủi ro | Cửa sổ ghi dài, tốn tài nguyên | Sai watermark → **mất dữ liệu** hoặc **trùng dữ liệu** |

> 📌 **Phản xạ:** đề nói "bảng dimension nhỏ, thay đổi ít, cần đơn giản" → full load/overwrite. Đề nói "bảng fact hàng trăm triệu dòng, chỉ có dữ liệu mới mỗi ngày" → incremental.

## C. WATERMARK — VIẾT LẠI ĐƯỢC TỪ TRÍ NHỚ

Đây là pattern phải gõ lại được không nhìn tài liệu.

```text
┌──────────────┐   1. Lookup: đọc watermark cũ
│ control table│──────────────────────────────────┐
│ (watermark)  │                                  ▼
└──────▲───────┘                    2. Copy/Query: lấy rows có
       │                               ModifiedDate > @old AND <= @new
       │                                          │
       │ 4. Update watermark = @new               ▼
       │    (CHỈ khi bước 3 thành công)  3. MERGE vào bảng đích
       └──────────────────────────────────────────┘
```

**Ba lỗi kinh điển của pattern này — đề hay hỏi đúng ba lỗi này:**

1. **Cập nhật watermark trước khi load xong.** Nếu bước load fail, watermark đã dời → **mất dữ liệu vĩnh viễn**. Luôn commit watermark ở bước cuối, sau khi mọi thứ thành công.
2. **Dùng `>=` cả hai đầu.** Gây trùng dòng biên. Chuẩn: `> old AND <= new`, với `new` chốt một lần ở đầu lần chạy (không dùng `utcnow()` lặp lại nhiều nơi).
3. **Lấy `new` bằng thời gian hệ thống thay vì `MAX()` ở nguồn.** Lệch múi giờ/độ trễ ghi làm sót dòng. An toàn hơn: `new = MAX(ModifiedDate)` đọc từ nguồn.

**Pipeline expression cần thuộc:**

```text
@activity('LookupWatermark').output.firstRow.WatermarkValue
@concat('SELECT * FROM dbo.Orders WHERE ModifiedDate > ''',
        activity('LookupWatermark').output.firstRow.WatermarkValue,
        ''' AND ModifiedDate <= ''',
        activity('LookupNewWatermark').output.firstRow.NewWatermark, '''')
```

## D. MERGE IDEMPOTENT — BỐN ĐIỀU KIỆN

`MERGE` **không tự thân idempotent**. Muốn chạy lại an toàn phải đủ bốn điều kiện:

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ĐIỀU KIỆN 1 — DEDUP NGUỒN theo business key, giữ bản mới nhất
w = Window.partitionBy("CustomerId").orderBy(F.col("ModifiedDate").desc())
src = (spark.read.format("delta").load(bronze_path)
       .withColumn("rn", F.row_number().over(w))
       .filter("rn = 1")
       .drop("rn"))

# ĐIỀU KIỆN 2 — MATCH KEY DUY NHẤT trên bảng đích
# ĐIỀU KIỆN 3 — ĐIỀU KIỆN XÁC ĐỊNH (chỉ update khi thực sự mới hơn)
tgt = DeltaTable.forName(spark, "silver.dim_customer")
(tgt.alias("t")
    .merge(src.alias("s"), "t.CustomerId = s.CustomerId")
    .whenMatchedUpdateAll(condition="s.ModifiedDate > t.ModifiedDate")
    .whenNotMatchedInsertAll()
    .execute())

# ĐIỀU KIỆN 4 — COMMIT WATERMARK/STATE SAU KHI THÀNH CÔNG (ở bước sau, không trong cùng cell)
```

> ⚠️ **Bẫy được hỏi rất nhiều:** nếu nguồn có **nhiều dòng cùng match một dòng đích**, Delta `MERGE` sẽ **báo lỗi** (`multiple source rows matched`). Đó chính là lý do bước dedup ở điều kiện 1 là **bắt buộc**, không phải tuỳ chọn.

## E. SCD TYPE 1 vs TYPE 2 — SƠ ĐỒ VÀ CODE

```text
Nguồn: Customer C001 đổi City từ "Hanoi" sang "Danang"

SCD TYPE 1 (ghi đè — chỉ giữ hiện tại)
┌────────┬─────────┐          ┌────────┬─────────┐
│ C001   │ Hanoi   │   ──►    │ C001   │ Danang  │      1 dòng, lịch sử biến mất
└────────┴─────────┘          └────────┴─────────┘

SCD TYPE 2 (giữ lịch sử)
┌────────┬─────────┬────────────┬────────────┬───────────┐
│ C001   │ Hanoi   │ 2026-01-01 │ 2026-09-06 │ IsCurrent=false │  ← expire dòng cũ
│ C001   │ Danang  │ 2026-09-06 │ NULL       │ IsCurrent=true  │  ← insert dòng mới
└────────┴─────────┴────────────┴────────────┴───────────┘
                     ValidFrom    ValidTo
```

**SCD Type 2 luôn là hai bước** — đây là điểm phải nhớ:

```python
from pyspark.sql import functions as F
from delta.tables import DeltaTable

tgt = DeltaTable.forName(spark, "silver.dim_customer")
now = F.current_timestamp()

# BƯỚC 1 — Expire các dòng current có thuộc tính thay đổi
(tgt.alias("t")
    .merge(src.alias("s"), "t.CustomerId = s.CustomerId AND t.IsCurrent = true")
    .whenMatchedUpdate(
        condition="t.City <> s.City OR t.Segment <> s.Segment",
        set={"IsCurrent": F.lit(False), "ValidTo": now})
    .execute())

# BƯỚC 2 — Insert version mới cho những key vừa bị expire + key hoàn toàn mới
new_versions = (src.alias("s")
    .join(tgt.toDF().alias("t"),
          (F.col("s.CustomerId") == F.col("t.CustomerId")) & (F.col("t.IsCurrent") == True),
          "left_anti")          # chỉ giữ những dòng chưa có bản current tương ứng
    .withColumn("ValidFrom", F.current_timestamp())
    .withColumn("ValidTo", F.lit(None).cast("timestamp"))
    .withColumn("IsCurrent", F.lit(True)))

new_versions.write.format("delta").mode("append").saveAsTable("silver.dim_customer")
```

**T-SQL trong Warehouse** cũng theo đúng hai bước: `UPDATE ... SET IsCurrent = 0, ValidTo = SYSDATETIME()` rồi `INSERT` bản mới.

> 📌 Cột bắt buộc của SCD2: **ValidFrom / ValidTo / IsCurrent** (một số thiết kế thêm `IsDeleted`, `HashDiff` để phát hiện thay đổi nhanh).

## F. LATE-ARRIVING DIMENSION — MẪU CHUẨN

Tình huống: fact có `CustomerId = C999` nhưng `dim_customer` chưa có C999.

```text
SAI:  để join fail hoặc để fact bị loại → mất doanh thu trong báo cáo
SAI:  dừng cả pipeline chờ dimension    → hệ thống mong manh

ĐÚNG: gán surrogate key UNKNOWN (thường = -1) rồi ghi log
      ┌──────────────────────────────────────────────┐
      │ dim_customer luôn có sẵn dòng đặc biệt:      │
      │   CustomerKey = -1, CustomerId = 'UNKNOWN'   │
      └──────────────────────────────────────────────┘
      Sau đó khi dimension thật xuất hiện, một job cập nhật lại fact
      (hoặc dùng inferred member: tạo dòng dim tạm rồi hoàn thiện sau)
```

```python
fact = (fact_raw.alias("f")
        .join(dim.alias("d"), F.col("f.CustomerId") == F.col("d.CustomerId"), "left")
        .withColumn("CustomerKey", F.coalesce(F.col("d.CustomerKey"), F.lit(-1))))
```

## G. XỬ LÝ DELETE Ở NGUỒN — BA CÁCH

| Cách | Khi nào dùng | Ghi chú |
|---|---|---|
| **CDC / change feed** | Nguồn phát tín hiệu delete | Chuẩn nhất; Copy Job với CDC replicate được cả delete |
| **Soft delete flag** | Nguồn có cột `IsDeleted`/`Status` | Chỉ cần lọc/đánh dấu ở đích |
| **So sánh snapshot** | Nguồn chỉ cho snapshot đầy đủ | So key đích với key nguồn, đánh dấu key thiếu là đã xoá — tốn kém |

> 📌 Câu hỏi hay gặp: "incremental load bằng watermark, vì sao bảng đích vẫn còn dòng đã xoá ở nguồn?" → vì **watermark chỉ thấy dòng được sửa/thêm**, nó **không thấy dòng bị xoá**. Muốn xử lý delete phải dùng CDC hoặc so sánh snapshot.

## H. COPY JOB — CÁC LỰA CHỌN PHẢI THUỘC

| Nhóm | Lựa chọn | Ghi chú thi |
|---|---|---|
| Chế độ | **Full copy** ↔ **Incremental copy** | Lần đầu chép hết, các lần sau chỉ chép phần mới/đổi |
| Phát hiện thay đổi | **CDC** ↔ **Watermark-based** | Trộn bảng có CDC và không CDC trong một job → **tất cả** bị coi là watermark-based |
| Cách ghi | **Append** (mặc định) · **Merge** (≈ SCD1) · **Overwrite** · **SCD Type 2** | SCD2 built-in chỉ khả dụng với CDC, tự thêm cột hiệu lực |

> 📌 Copy Job là đáp án đúng khi đề nói "cần incremental/CDC/SCD2 nhưng **giảm thiểu công sức xây dựng**". Nếu đề đòi rẽ nhánh, ForEach, gọi notebook, alert → đó là **Pipeline**.

## I. MEDALLION — MỖI LỚP LÀM GÌ, DÙNG PATTERN NÀO

```text
BRONZE  raw, giữ nguyên, có audit columns      →  APPEND (hoặc overwrite theo partition ngày)
          _ingested_at, _source_file, _batch_id
   ▼
SILVER  làm sạch, ép kiểu, khử trùng, conform  →  MERGE idempotent (SCD1/SCD2)
   ▼
GOLD    star schema, aggregate, business logic →  MERGE hoặc rebuild theo lịch
```

Ba quy tắc thường bị hỏi:

1. **Bronze không sửa dữ liệu** — chỉ thêm cột audit. Muốn tái xử lý thì luôn còn nguồn gốc.
2. **Silver là nơi đặt business key và khử trùng**, không phải Gold.
3. **Gold phục vụ báo cáo** — hợp với Direct Lake, nên là nơi cần V-Order + `OPTIMIZE`.

## J. ACTIVE RECALL — 14 CÂU (che đáp án, nói thành tiếng)

<details><summary><b>1.</b> Watermark phải được cập nhật ở thời điểm nào và vì sao?</summary>

**Sau khi** toàn bộ bước load/MERGE thành công. Nếu cập nhật trước và load fail, lần chạy sau sẽ bỏ qua khoảng dữ liệu đó → **mất dữ liệu**.
</details>

<details><summary><b>2.</b> Vì sao khoảng lọc nên là `> old AND <= new` chứ không phải `>= old`?</summary>

`>=` làm dòng ở biên bị lấy lại hai lần → trùng dữ liệu. `>` ở cận dưới và `<=` ở cận trên chốt một lần cho mỗi dòng.
</details>

<details><summary><b>3.</b> Bốn điều kiện để `MERGE` idempotent?</summary>

(1) dedup source theo business key; (2) match key duy nhất ở đích; (3) điều kiện update xác định (ví dụ chỉ update khi mới hơn); (4) commit watermark/state sau khi thành công.
</details>

<details><summary><b>4.</b> Điều gì xảy ra khi nhiều source row match một target row trong Delta MERGE?</summary>

`MERGE` **báo lỗi**. Phải dedup nguồn trước bằng `row_number()` theo business key.
</details>

<details><summary><b>5.</b> SCD Type 2 cần những cột nào và làm mấy bước?</summary>

`ValidFrom`, `ValidTo`, `IsCurrent` (có thể thêm `HashDiff`, `IsDeleted`). Làm **hai bước**: expire dòng current đã đổi, rồi insert version mới.
</details>

<details><summary><b>6.</b> Late-arriving dimension xử lý ra sao và vì sao không để pipeline fail?</summary>

Gán surrogate key **unknown member (-1)** và ghi log, sau đó cập nhật lại khi dimension xuất hiện. Để fail sẽ chặn cả luồng fact chỉ vì một key thiếu.
</details>

<details><summary><b>7.</b> Watermark có phát hiện được dòng bị xoá ở nguồn không?</summary>

**Không.** Watermark chỉ thấy dòng mới/được sửa. Delete phải xử lý bằng CDC, soft-delete flag, hoặc so sánh snapshot.
</details>

<details><summary><b>8.</b> Trong Copy Job, trộn bảng có CDC và bảng không CDC thì sao?</summary>

Toàn bộ job bị coi là **watermark-based**, tức mất lợi thế CDC (kể cả khả năng replicate delete).
</details>

<details><summary><b>9.</b> Bốn chế độ ghi của Copy Job?</summary>

**Append** (mặc định), **Merge** (tương đương SCD1), **Overwrite**, **SCD Type 2** (đi kèm CDC, tự thêm cột hiệu lực).
</details>

<details><summary><b>10.</b> Surrogate key khác natural/business key thế nào và vì sao dùng surrogate?</summary>

Natural key đến từ hệ nguồn (mã khách hàng), surrogate key do kho dữ liệu sinh ra (số nguyên tăng dần). Dùng surrogate để độc lập với hệ nguồn, hỗ trợ SCD2 (một business key nhiều version) và join nhanh hơn.
</details>

<details><summary><b>11.</b> Bronze/Silver/Gold — mỗi lớp dùng pattern ghi nào?</summary>

Bronze: append (giữ nguyên raw + audit columns). Silver: MERGE idempotent (SCD1/SCD2). Gold: MERGE hoặc rebuild theo lịch phục vụ báo cáo.
</details>

<details><summary><b>12.</b> Ba cột audit nên có ở Bronze?</summary>

`_ingested_at` (thời điểm nạp), `_source_file`/`_source_system` (nguồn gốc), `_batch_id`/run id (để truy vết và tái xử lý).
</details>

<details><summary><b>13.</b> Denormalize để làm gì khi chuẩn bị cho dimensional model?</summary>

Gộp các bảng chuẩn hoá (product → subcategory → category) thành **một dimension phẳng** để giảm số join lúc truy vấn và phù hợp star schema/Power BI.
</details>

<details><summary><b>14.</b> Loading pattern cho streaming khác batch ở điểm nào?</summary>

Streaming cần **checkpoint** (mỗi query một checkpoint riêng), **watermark** để giới hạn state, và vì delivery là at-least-once nên bước ghi phải **idempotent** — thường dùng `foreachBatch` + Delta `MERGE`.
</details>

## K. MINI-LAB 90 PHÚT — GÕ TỪ ĐẦU, KHÔNG COPY

1. Tạo bảng nguồn giả lập 10 dòng có `CustomerId`, `City`, `ModifiedDate`.
2. **Full load** vào bronze bằng `mode("overwrite")`; thêm ba cột audit.
3. **Incremental watermark:** tạo bảng control lưu watermark; viết pipeline Lookup → Copy → Stored Procedure/notebook cập nhật watermark. Chạy 2 lần, xác nhận lần 2 không nạp lại dòng cũ.
4. **Tạo lỗi có chủ đích:** nhân đôi một `CustomerId` trong nguồn rồi chạy `MERGE` → xem lỗi *multiple source rows matched* → sửa bằng dedup `row_number()`.
5. **SCD Type 1:** đổi `City` của một khách, chạy MERGE, xác nhận chỉ có 1 dòng và giá trị đã đổi.
6. **SCD Type 2:** thêm cột `ValidFrom/ValidTo/IsCurrent`, chạy đủ hai bước, xác nhận có 2 dòng cho khách đó và chỉ 1 dòng `IsCurrent = true`.
7. **Late-arriving dimension:** insert fact với `CustomerId` chưa tồn tại, xác nhận nhận `CustomerKey = -1` thay vì fail.
8. **Test idempotency:** chạy lại toàn bộ notebook lần thứ ba mà không đổi dữ liệu nguồn → số dòng ở đích **phải không đổi**. Nếu đổi, quay lại tìm điều kiện nào trong bốn điều kiện ở mục D bị thiếu.

**Ngưỡng đạt:** gõ lại được đoạn MERGE idempotent và hai bước SCD2 **không nhìn tài liệu**, và trả lời đúng ≥13/14 câu ở mục J.

---

# ⚡ CẬP NHẬT 05/11/2026 — LOADING PATTERNS

## U1. Ba bullet chính thức — không đổi

Full & incremental loads · Prepare data for a dimensional model · Loading pattern for streaming data.

## U2. Copy job CDC & SCD Type 2 — đối chiếu Learn (trang cập nhật 10/09/2026)

| Điểm | Nội dung |
|---|---|
| Cột SCD2 thêm vào đích | **`Valid_From`**, **`Valid_To`** (NULL với bản ghi đang hiệu lực), **`Is_Current`** |
| Trạng thái | **SCD Type 2: Preview**. Merge (SCD Type 1) là **mặc định** của CDC trong Copy job |
| Connector ghi SCD2 | Azure SQL DB, Azure SQL MI, SQL Server on-prem, SQL database in Fabric, Fabric Warehouse, Fabric Lakehouse table, Google BigQuery, Snowflake, Synapse DW. **Không**: SAP Datasphere Outbound. **Oracle: chưa hỗ trợ SCD2** khi CDC từ Oracle |
| Bật SCD2 | Chọn SCD Type 2 làm **write method** — lịch sử version và **soft delete** bật cùng lúc, áp cho mọi bảng đã chọn |
| Phát hiện CDC | Copy job **tự phát hiện** bảng nguồn đã bật CDC; chỉ hỗ trợ **net changes** |
| **Bẫy trộn bảng** | Nếu một Copy job có cả bảng **bật CDC** và **không bật CDC** → **tất cả** bị xử lý như **watermark-based incremental** (mất khả năng bắt DELETE) |
| Lakehouse làm nguồn | Copy job **không tự phát hiện** CDF đã bật trên Lakehouse table |
| Mới GA 09/2026 | **Custom Copy job staging** (chọn database/schema staging cho CDC và SCD2), **Copy job workspace monitoring**, **Snowflake read-only CDC**, cải thiện initial load Oracle CDC. What's new cũng công bố GA **SQL CDC custom capture names**, trong khi trang CDC (10/09/2026) vẫn ghi chỉ default capture instance — đề khó hỏi chi tiết này |

```sql
-- Nguồn SQL Server / Azure SQL: bật CDC ở database và từng bảng (kiến thức SQL chuẩn)
EXEC sys.sp_cdc_enable_db;
EXEC sys.sp_cdc_enable_table
     @source_schema = N'dbo',
     @source_name   = N'Customer',
     @role_name     = NULL;
```

## U3. Runtime 2.0 ảnh hưởng tới MERGE/SCD2 viết tay

- **Deletion vectors bật mặc định** cho table mới trên Runtime 2.0 → `UPDATE`/`DELETE`/`MERGE` (bước expire version cũ của SCD2) ghi nhanh hơn vì chỉ đánh dấu dòng; chạy **OPTIMIZE** định kỳ để gộp lại.
- `spark.databricks.delta.optimizeWrite.enabled` **không còn set mặc định** (Runtime 1.3: true), nhưng adaptive target file size + fast optimize bật → vẫn theo dõi số file sau mỗi lần MERGE.
- Logic idempotent ở mục 6 **không đổi**: dedup source theo business key trước MERGE, điều kiện cập nhật theo thời gian/sequence.

## U4. Streaming loading pattern — sửa code Event Hubs cho Runtime 2.0

`EventHubConnector` đã bị gỡ khỏi Runtime 2.0. Mẫu đúng là đọc Event Hubs qua **Kafka endpoint**:

```python
from pyspark.sql import functions as F

eh_namespace = "contoso-eh"            # <namespace>.servicebus.windows.net
eh_name      = "telemetry"             # Event Hub = Kafka topic
conn_str     = notebookutils.credentials.getSecret("https://contoso-kv.vault.azure.net/", "eh-conn")

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", f"{eh_namespace}.servicebus.windows.net:9093")
       .option("subscribe", eh_name)
       .option("kafka.security.protocol", "SASL_SSL")
       .option("kafka.sasl.mechanism", "PLAIN")
       .option("kafka.sasl.jaas.config",
               'org.apache.kafka.common.security.plain.PlainLoginModule required '
               f'username="$ConnectionString" password="{conn_str}";')
       .option("startingOffsets", "latest")
       .load())

events = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e"))   # payload ở cột value
             .select("e.*"))

(events.writeStream.format("delta").outputMode("append")
       .option("checkpointLocation", "Files/checkpoints/telemetry_kafka")
       .toTable("bronze_telemetry"))
```

> Checkpoint của query `eventhubs` cũ **không dùng lại được** cho source Kafka mới → dùng checkpoint mới; để không mất/không trùng dữ liệu khi chuyển, đặt `startingOffsets` phù hợp và giữ sink **idempotent**.

## U5. Câu hỏi tình huống cập nhật

<details><summary><b>LP-U1.</b> Copy job đang CDC 10 bảng từ Azure SQL. Team thêm 2 bảng chưa bật CDC vào cùng job. Sau đó các lệnh DELETE ở nguồn không còn phản ánh sang đích. Vì sao?</summary>

Job trộn bảng CDC và non-CDC nên **toàn bộ** chuyển sang **watermark-based incremental**, vốn không bắt được hard DELETE. Sửa: bật CDC cho 2 bảng mới (hoặc tách chúng sang Copy job khác).
</details>

<details><summary><b>LP-U2.</b> Yêu cầu: giữ lịch sử thay đổi khách hàng từ Azure SQL DB vào Fabric Warehouse, ít công sức nhất, không viết code. Chọn gì, và bảng đích có cột nào?</summary>

**Copy job + CDC + write method SCD Type 2 (Preview)**. Bảng đích có thêm `Valid_From`, `Valid_To`, `Is_Current`; bản ghi bị xóa ở nguồn được soft delete (đặt `Valid_To`, `Is_Current = false`).
</details>

<details><summary><b>LP-U3.</b> Cùng yêu cầu như LP-U2 nhưng nguồn là Oracle. Thay đổi gì?</summary>

Copy job CDC (Merge) đưa thay đổi vào **staging**, rồi tự hiện thực SCD2 bằng **Delta MERGE trong notebook** (Lakehouse) hoặc **T-SQL** trong Warehouse — vì SCD2 của Copy job chưa hỗ trợ CDC từ Oracle.
</details>

<details><summary><b>LP-U4.</b> Notebook streaming dùng `format("eventhubs")` báo lỗi không tìm thấy data source sau khi workspace chuyển Runtime 2.0. Bước sửa đúng?</summary>

Chuyển sang `format("kafka")` với endpoint `<namespace>.servicebus.windows.net:9093`, `SASL_SSL`/`PLAIN`, parse cột `value`, dùng **checkpoint mới**, đảm bảo sink idempotent. Không phải lỗi quyền, không phải do capacity.
</details>
