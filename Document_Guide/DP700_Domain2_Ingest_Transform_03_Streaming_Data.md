# DP-700: Ingest and Transform Streaming Data
## Ingest and Transform Data (30–35%) — Phần 3/3


> [!IMPORTANT]
> **Bản hiệu chỉnh ngày 09/08/2026.** Nội dung được đối chiếu với blueprint DP-700 có hiệu lực từ **21/07/2026**. Microsoft cho biết phần lớn câu hỏi tập trung vào tính năng GA, nhưng tính năng Preview phổ biến vẫn có thể xuất hiện. Với các bảng “supported items”, runtime, limits và trạng thái Preview/GA, hãy kiểm tra Microsoft Learn lần cuối trước ngày thi.

> **Skills measured as of July 21, 2026**  
> Tài liệu chuẩn bị thi chứng chỉ **Microsoft Fabric Data Engineer Associate (DP-700)**  
> Tham chiếu: [Microsoft Learn — Study Guide DP-700](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)

---

## MỤC LỤC

1. [Choose an Appropriate Streaming Engine](#1-choose-an-appropriate-streaming-engine)
2. [Choose Between Native Tables and OneLake Shortcuts in Real-Time Intelligence](#2-choose-between-native-tables-and-onelake-shortcuts-in-real-time-intelligence)
3. [Choose Between Query Acceleration and Standard OneLake Shortcuts](#3-choose-between-query-acceleration-and-standard-onelake-shortcuts)
4. [Process Data by Using Eventstreams](#4-process-data-by-using-eventstreams)
5. [Process Data by Using Spark Structured Streaming](#5-process-data-by-using-spark-structured-streaming)
6. [Process Data by Using KQL](#6-process-data-by-using-kql)
7. [Create Windowing Functions](#7-create-windowing-functions)
8. [Câu hỏi mô phỏng bài thi (Practice Questions)](#8-câu-hỏi-mô-phỏng-bài-thi-practice-questions)

---

## 1. Choose an Appropriate Streaming Engine

### 1.1. Ba lựa chọn execution cần phân biệt

| Lựa chọn | Điểm mạnh | Scenario |
|---|---|---|
| Eventstream + Eventhouse | low-code routing + KQL analytics | dashboard, alert, IoT/log |
| Spark Structured Streaming micro-batch | broad transforms + Delta | streaming ETL, stateful processing |
| Spark Structured Streaming Real-time Mode | ultra-low-latency Spark | Runtime 2.0+, supported topology |

> [!IMPORTANT]
> **Real-time Mode là cập nhật 2026:** Fabric Runtime 2.0 (Spark 4.1)+ hỗ trợ `Trigger.RealTime`. Không còn đúng nếu học “Spark Structured Streaming luôn micro-batch”.

### 1.2. Decision framework

```text
Low-code routing/dashboard/alert? → Eventstream + Eventhouse (+ Activator)
Complex Spark/stateful/Delta ETL? → Structured Streaming
Scheduled backlog then stop?      → availableNow + checkpoint
Ultra-low-latency but must Spark? → Real-time Mode, verify supported topology
Data already in OneLake for KQL?  → shortcut; frequent query → acceleration
```

### 1.3. Trade-off
- Eventstream tối ưu event ingestion/routing và no-code operators.
- Eventhouse tối ưu KQL/time-series/log analytics.
- Spark tối ưu custom/distributed processing.
- Không coi `sub-second` là SLA mặc định cho mọi topology.
- Real-time Mode support surface hẹp hơn micro-batch trong preview.

## 2. Choose Between Native Tables and OneLake Shortcuts in Real-Time Intelligence

### 2.1. Native Tables trong Eventhouse

**Native Tables** là bảng dữ liệu được tạo **natively** trong Eventhouse (KQL Database). Dữ liệu được physically ingested, indexed, và optimized cho Kusto engine.

**Đặc điểm**:
- Dữ liệu được **sao chép** vào Eventhouse
- **Indexed** theo thời gian và các cột thường truy vấn
- **Hiệu suất cao nhất** — latency thấp nhất có thể
- **Compression** tối ưu cho Kusto engine

**Khi nào dùng Native Tables**:
- Streaming data tốc độ cao; sizing phải dựa trên benchmark, event size, partitioning và capacity
- Dashboard/alerting cần độ trễ thấp và KQL query performance; không coi “sub-second” là SLA mặc định cho mọi topology
- Mission-critical alerting/monitoring
- Data không tồn tại ở nơi khác trong OneLake

### 2.2. OneLake Shortcuts trong Eventhouse

**OneLake Shortcuts** trong Real-Time Intelligence cho phép Eventhouse **tham chiếu** tới Delta tables trong OneLake mà **không cần copy** dữ liệu.

**Đặc điểm**:
- **Không sao chép dữ liệu** — tham chiếu ảo
- Truy cập Delta tables từ Lakehouse/Warehouse
- **Hiệu suất thấp hơn** native tables (network overhead)
- **Tiết kiệm storage** — single copy philosophy

**Khi nào dùng OneLake Shortcuts**:
- Dữ liệu đã tồn tại trong OneLake (Lakehouse)
- Cần **unified analytics** — KQL queries trên data lake
- Không cần ultra-low latency
- Muốn tránh data duplication

### 2.3. Bảng so sánh chi tiết

| Tiêu chí | Native Tables | OneLake Shortcuts |
|----------|:-------------|:-----------------|
| **Lưu trữ vật lý** | Trong Eventhouse | Tham chiếu tới OneLake |
| **Data duplication** | ✅ Có (copy data) | ❌ Không (virtual) |
| **Query Performance** | ⭐⭐⭐⭐⭐ Cao nhất | ⭐⭐⭐ Trung bình |
| **Storage cost** | Cao hơn (duplicate) | Thấp hơn (single copy) |
| **Ingestion cần thiết** | ✅ Có (Eventstream/batching) | ❌ Không |
| **Real-time streaming** | ✅ Lý tưởng | ⚠️ Không phù hợp cho streaming |
| **Cross-workspace data** | ❌ Cần copy | ✅ Truy cập trực tiếp |
| **Query acceleration** | N/A (đã native) | ✅ Có thể enable |

### 2.4. Decision Framework

```
Dữ liệu streaming cần KQL queries?
│
├── Data đang stream real-time vào hệ thống?
│   └── ✅ NATIVE TABLE (ingestion → Eventhouse)
│
├── Data đã nằm trong Lakehouse Delta tables?
│   ├── Cần query performance cao?
│   │   └── ✅ SHORTCUT + QUERY ACCELERATION
│   └── Performance không quá quan trọng?
│       └── ✅ STANDARD SHORTCUT
│
└── Cần cả streaming data VÀ historical data từ Lakehouse?
    └── ✅ Kết hợp: NATIVE TABLE cho streaming
                  + SHORTCUT cho historical
```

---

## 3. Choose Between Query Acceleration and Standard OneLake Shortcuts

### 3.1. Standard OneLake Shortcuts

Shortcuts thông thường hoạt động như **virtual reference** — mọi query đều đọc trực tiếp từ OneLake source.

**Ưu điểm**:
- Không tốn thêm storage
- Data luôn up-to-date
- Setup đơn giản

**Nhược điểm**:
- Network latency khi đọc data
- Không có Kusto indexing → query chậm hơn
- Không phù hợp cho ad-hoc analytics cần tốc độ

### 3.2. Query Acceleration cho OneLake Shortcuts

Query Acceleration là tính năng **indexing và caching** cho shortcuts, giúp nâng performance gần bằng native tables.

**Cách hoạt động**:
1. Enable acceleration policy trên shortcut
2. Eventhouse tự động **cache và index** data từ OneLake
3. Queries chạy trên cached/indexed data → performance cao
4. Data được **sync tự động** khi source thay đổi

**Ưu điểm**:
- Performance **gần bằng native tables**
- Không cần maintain separate ingestion pipeline
- Data vẫn là single copy (source of truth ở OneLake)

**Nhược điểm**:
- Tốn thêm **SSD storage và compute** trong Eventhouse
- Có slight delay trong sync (not real-time)
- Chi phí capacity tăng

### 3.3. So sánh ba lựa chọn

| Tiêu chí | Standard Shortcut | Accelerated Shortcut | Native Table |
|----------|:-----------------|:--------------------|:-------------|
| **Performance** | ⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| **Storage cost** | Thấp nhất | Trung bình | Cao nhất |
| **Data freshness** | Real-time | Near real-time | Real-time (streaming) |
| **Setup effort** | Thấp | Thấp | Trung bình |
| **Maintenance** | Không | Tối thiểu | Quản lý ingestion |
| **Best for** | Occasional queries | Ad-hoc analytics | High-freq dashboards |

### 3.4. Khi nào chọn Query Acceleration?

✅ **Chọn Query Acceleration khi**:
- Data ở OneLake mà bạn cần query **thường xuyên** bằng KQL
- Cần performance tốt hơn standard shortcut
- Muốn **tránh duplicate ingestion** (data đã ở Lakehouse)
- Budget cho SSD storage/compute

❌ **KHÔNG cần Query Acceleration khi**:
- Query shortcut data **hiếm khi** (occasional)
- Budget giới hạn
- Data stream trực tiếp vào Eventhouse → dùng Native Table

> 📌 **Tóm tắt cho thi**: 
> - **Standard Shortcut** = Cheap, slow, occasional queries
> - **Accelerated Shortcut** = Medium cost, fast, frequent queries on Delta data
> - **Native Table** = Highest cost, fastest, real-time streaming

---

## 4. Process Data by Using Eventstreams

### 4.1. Eventstream là gì?

Eventstream là thành phần trong Fabric **Real-Time Intelligence** dùng để **capture, transform, và route** real-time events.

### 4.2. Kiến trúc Eventstream

```
┌─────────────────────────────────────────────────────────┐
│                     EVENTSTREAM                          │
│                                                          │
│  ┌──────────┐    ┌──────────────┐    ┌───────────────┐  │
│  │  SOURCE   │───►│ TRANSFORMATIONS│───►│ DESTINATIONS  │ │
│  │          │    │  (optional)   │    │               │  │
│  │• Event Hub│    │• Filter      │    │• Eventhouse   │  │
│  │• Kafka    │    │• Manage      │    │• Lakehouse    │  │
│  │• Custom   │    │  Fields      │    │• Custom App   │  │
│  │  App      │    │• Group By    │    │• Reflex       │  │
│  │• DB CDC   │    │• Union       │    │  (Activator)  │  │
│  │• Blob     │    │• Expand      │    │• Derived      │  │
│  │• Sample   │    │• SQL Operator│    │  Eventstream  │  │
│  │  Data     │    │              │    │               │  │
│  └──────────┘    └──────────────┘    └───────────────┘  │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

### 4.3. Nguồn dữ liệu (Sources)

| Source | Mô tả |
|--------|--------|
| **Azure Event Hubs** | Event streaming platform, hỗ trợ millions events/sec |
| **Azure IoT Hub** | IoT device management + event ingestion |
| **Apache Kafka** | Kafka clusters (Confluent, HDInsight, self-managed) |
| **Custom App** | SDK-based ingestion từ ứng dụng tùy chỉnh |
| **Azure Blob Storage** | Event-driven ingestion khi file mới xuất hiện |
| **Database CDC** | Change events từ Azure SQL, PostgreSQL |
| **Sample Data** | Dữ liệu mẫu để testing (Bike, Stock, etc.) |

### 4.4. Transformations trong Eventstream

| Transformation | Mô tả | Ví dụ |
|---------------|--------|-------|
| **Filter** | Lọc events theo điều kiện | `Temperature > 30` |
| **Manage Fields** | Thêm, xóa, đổi tên cột | Xóa cột nhạy cảm |
| **Group By** | Aggregate events theo time window | Trung bình Temperature mỗi 5 phút |
| **Union** | Kết hợp nhiều streams | Merge IoT + Log streams |
| **Expand** | Flatten nested arrays | Expand JSON array thành rows |
| **SQL Operator** | Viết SQL tùy chỉnh | Windowed aggregations phức tạp |

### 4.5. Destinations

| Destination | Mô tả | Use Case |
|------------|--------|----------|
| **Eventhouse** | KQL Database table | Real-time analytics, dashboards |
| **Lakehouse** | Delta table | Streaming ETL, data lake |
| **Custom App** | HTTP endpoint | External processing |
| **Activator** | Theo dõi điều kiện và hành động/alert | Real-time action |
| **Derived stream** | Stream logic sau transform | Chia sẻ/fan-out qua Real-Time hub |
| **Spark Notebook (Preview)** | Khởi chạy Structured Streaming trong notebook đích | Custom Spark processing |

### 4.6. Ví dụ End-to-End Eventstream Setup

**Scenario**: IoT sensor data → filter high temperatures → alert + store

```
Eventstream: IoT_Sensor_Stream
│
├── Source: Azure IoT Hub
│
├── Transform: Filter
│   Condition: Temperature > 50
│
├── Transform: Manage Fields
│   Add: AlertLevel = CASE WHEN Temperature > 80 THEN 'Critical' 
│                          WHEN Temperature > 50 THEN 'Warning'
│                     END
│
├── Destination 1: Eventhouse → SensorAlerts table
│   (Cho real-time dashboard)
│
├── Destination 2: Lakehouse → bronze.iot_sensor_data
│   (Cho historical analytics)
│
└── Destination 3: Activator
    Trigger: Temperature > 80 → Send email alert
```

> 📌 **Điểm thi**: Eventstream hỗ trợ **nhiều destinations** cùng lúc (fan-out pattern). Một event có thể đồng thời ghi vào Eventhouse VÀ Lakehouse VÀ trigger alert.

> [!NOTE]
> Eventstream mới dùng **Enhanced capabilities** mặc định. Supported destinations đã mở rộng, gồm cả **Spark Notebook (Preview)**; với câu hỏi thi hãy chọn destination theo requirement thay vì học một danh sách cứng.

---

## 5. Process Data by Using Spark Structured Streaming

### 5.1. Core model
Structured Streaming = **source → transformations → sink**, với always-on hoặc scheduled incremental.

### 5.2. Sources Fabric tài liệu hóa
Files (`text/csv/json/parquet/orc`), Delta, Kafka, Azure Event Hubs, Fabric Eventstream và `rate` cho test.

> [!WARNING]
> `cloudFiles` / Databricks Auto Loader không phải API Fabric Spark được Microsoft Learn tài liệu hóa. Với DP-700, dùng built-in file source.

### 5.3. File source → Delta

```python
from pyspark.sql.types import *
from pyspark.sql import functions as F

schema=StructType([
 StructField('DeviceId',StringType()),
 StructField('Temperature',DoubleType()),
 StructField('Humidity',DoubleType()),
 StructField('EventTime',TimestampType())
])

stream=(spark.readStream.schema(schema).format('json')
        .option('maxFilesPerTrigger',100)
        .load('Files/incoming/sensors'))

q=(stream.writeStream.format('delta').outputMode('append')
   .option('checkpointLocation','Files/checkpoints/sensors')
   .trigger(availableNow=True)
   .toTable('bronze_sensors'))
q.awaitTermination()
```

### 5.4. Event Hubs → Delta

```python
import pyspark.sql.functions as F
from pyspark.sql.types import *

raw=(spark.readStream.format('eventhubs').options(**ehConf).load())
parsed=(raw.withColumn('bodyText',F.col('body').cast('string'))
        .select(F.from_json('bodyText',schema).alias('e'))
        .select('e.*'))

q=(parsed.writeStream.format('delta').outputMode('append')
   .option('checkpointLocation','Files/checkpoints/eventhub')
   .toTable('bronze_iot'))
q.awaitTermination()
```

### 5.5. Output modes
- `append`: new/finalized rows.
- `update`: changed rows; **Real-time Mode hiện yêu cầu update**.
- `complete`: toàn bộ result table ở mỗi trigger với supported aggregation.

Không học “có aggregation = complete”; hợp lệ còn phụ thuộc query plan/watermark/sink.

### 5.6. Watermark — hiểu đúng
Watermark được tính từ **max event time đã thấy - delay threshold**; records older than current watermark có thể bị drop ở stateful operator.

```python
from pyspark.sql.functions import window,avg,count
agg=(parsed.withWatermark('EventTime','10 minutes')
     .groupBy(window('EventTime','5 minutes'),'DeviceId')
     .agg(avg('Temperature').alias('AvgTemp'),count('*').alias('Events')))
```

### 5.7. `foreachBatch` + idempotent MERGE

```python
from delta.tables import DeltaTable

def upsert(batch_df,batch_id):
    tgt=DeltaTable.forName(spark,'silver_device')
    (tgt.alias('t').merge(batch_df.alias('s'),'t.DeviceId=s.DeviceId')
     .whenMatchedUpdateAll(condition='s.EventTime >= t.EventTime')
     .whenNotMatchedInsertAll().execute())

q=(parsed.writeStream.foreachBatch(upsert)
   .option('checkpointLocation','Files/checkpoints/device-upsert')
   .trigger(availableNow=True).start())
q.awaitTermination()
```

### 5.8. Checkpointing
- Một query → một durable checkpoint.
- Checkpoint giữ offsets/progress/state.
- Delta + supported sources/sinks giúp fault-tolerant exactly-once processing semantics.
- `foreachBatch`/custom sink có thể retry → idempotent logic.

### 5.9. RocksDB state store

```python
spark.conf.set('spark.sql.streaming.stateStore.providerClass',
 'org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider')
spark.conf.set('spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled',True)
```

Hữu ích cho window, dedup, stream-stream join và state lớn.

### 5.10. Real-time Mode (2026)
- Runtime 2.0 / Spark 4.1+.
- Long-running tasks, ultra-low-latency.
- Output mode hiện `update`.
- PySpark preview chưa expose trigger trực tiếp qua `.trigger()`; Microsoft dùng JVM API trong example.
- Source/sink/operator support hẹp hơn micro-batch.
- Scheduled lakehouse ingestion thường chọn `availableNow`, không chọn Real-time Mode chỉ vì “nhanh hơn”.

## 6. Process Data by Using KQL

### 6.1. KQL trong Real-Time Intelligence

KQL (Kusto Query Language) là ngôn ngữ native của Eventhouse, tối ưu cho:
- Dữ liệu time-series
- Log analytics
- Semi-structured data (JSON)
- Aggregations trên data streaming

### 6.2. Ingestion bằng KQL

#### Streaming Ingestion

```kql
// Streaming ingestion policy (enable real-time availability)
.alter table SensorData policy streamingingestion
'{"IsEnabled": true}'

// Ingestion mapping
.create-or-alter table SensorData ingestion json mapping "SensorMapping" 
'[{"column":"DeviceId","Properties":{"Path":"$.deviceId"}},{"column":"Temperature","Properties":{"Path":"$.temperature"}},{"column":"EventTime","Properties":{"Path":"$.timestamp"}}]'
```

#### Batch Ingestion

```kql
// Ingest từ external storage
.ingest into table SensorData 
  (h'https://storage.blob.core.windows.net/container/data.csv')
  with (
    format = 'csv',
    creationTime = '2026-01-01',
    ignoreFirstRecord = true
  )
```

### 6.3. Querying và Transforming bằng KQL

```kql
// === Real-time monitoring ===
SensorData
| where EventTime > ago(1h)
| summarize 
    AvgTemp = avg(Temperature),
    MaxTemp = max(Temperature),
    AlertCount = countif(Temperature > 80)
    by DeviceId, bin(EventTime, 5m)
| order by EventTime desc

// === Anomaly Detection ===
SensorData
| where EventTime > ago(24h)
| make-series AvgTemp = avg(Temperature) on EventTime from ago(24h) to now() step 1h by DeviceId
| extend anomalies = series_decompose_anomalies(AvgTemp)
| mv-expand EventTime, AvgTemp, anomalies
| where anomalies == 1
| project DeviceId, EventTime, AvgTemp

// === Update Policy (auto-transform on ingestion) ===
// Tạo bảng target cho transformed data
.create table SensorDataClean (
    DeviceId: string,
    Temperature: real,
    Humidity: real,
    EventTime: datetime,
    AlertLevel: string
)

// Tạo function cho transformation
.create-or-alter function TransformSensorData() {
    SensorData
    | extend AlertLevel = case(
        Temperature > 80, "Critical",
        Temperature > 50, "Warning",
        "Normal"
    )
    | project DeviceId, Temperature, Humidity, EventTime, AlertLevel
}

// Apply update policy (auto-execute khi data mới vào SensorData)
.alter table SensorDataClean policy update 
  @'[{"IsEnabled": true, "Source": "SensorData", "Query": "TransformSensorData()", "IsTransactional": true}]'
```

### 6.4. KQL Update Policy — Key Concept

Update Policy tự động chạy transformation khi data mới được ingested vào source table.

```
SensorData (source table)
    │ new data arrives
    ▼
TransformSensorData() function
    │ auto-executes
    ▼
SensorDataClean (target table)
    = transformed data available immediately
```

> 📌 **Điểm thi**: Update Policy là cách **tự động transform data trong Eventhouse** khi ingestion xảy ra. Nó hoạt động tương tự trigger/materialized view — data biến đổi ngay tại thời điểm insert.

---

## 7. Create Windowing Functions

### 7.1. Tổng quan Windowing Functions

Windowing functions chia dòng dữ liệu vô hạn (infinite stream) thành các **cửa sổ thời gian hữu hạn** (finite windows) để thực hiện aggregations.

### 7.2. Các loại Window

#### A. Tumbling Window (Cửa sổ lăn)

- **Đặc điểm**: Kích thước cố định, KHÔNG overlap, contiguous (liên tục)
- **Mỗi event thuộc đúng 1 window**

```
Time:  |----5min----|----5min----|----5min----|
       [  Window 1  ][  Window 2  ][  Window 3  ]
        ↑   events   ↑   events   ↑   events
```

**Ví dụ PySpark**:
```python
from pyspark.sql.functions import window, avg, count

# Tumbling window 5 phút
result = stream_df \
    .withWatermark("EventTime", "10 minutes") \
    .groupBy(
        window("EventTime", "5 minutes"),    # size = 5 min
        "DeviceId"
    ) \
    .agg(
        avg("Temperature").alias("AvgTemp"),
        count("*").alias("EventCount")
    )
```

**Ví dụ Eventstream SQL Operator**:
```sql
SELECT
    DeviceId,
    System.Timestamp() AS WindowEnd,
    AVG(Temperature) AS AvgTemp,
    COUNT(*) AS EventCount
FROM IoTEvents
TIMESTAMP BY EventTime
GROUP BY 
    DeviceId,
    TumblingWindow(minute, 5)     -- Tumbling window 5 phút
```

**Ví dụ KQL**:
```kql
SensorData
| summarize 
    AvgTemp = avg(Temperature),
    EventCount = count()
    by DeviceId, bin(EventTime, 5m)    // bin() = tumbling window
```

#### B. Hopping Window (Cửa sổ nhảy)

- **Đặc điểm**: Kích thước cố định, CÓ overlap
- **Một event có thể thuộc NHIỀU windows**
- **Parameters**: Window Size + Hop Size

```
Window Size = 10 min, Hop Size = 5 min

Time:  |---------------------------------------------------->
       [    Window 1 (10min)    ]
                [    Window 2 (10min)    ]
                         [    Window 3 (10min)    ]
       <--5min-->  (hop interval)
```

**Ví dụ PySpark**:
```python
# Hopping window: size=10min, hop=5min
result = stream_df \
    .withWatermark("EventTime", "10 minutes") \
    .groupBy(
        window("EventTime", "10 minutes", "5 minutes"),  # size=10, slide=5
        "DeviceId"
    ) \
    .agg(avg("Temperature").alias("AvgTemp"))
```

**Ví dụ Eventstream SQL Operator**:
```sql
SELECT
    DeviceId,
    System.Timestamp() AS WindowEnd,
    AVG(Temperature) AS AvgTemp
FROM IoTEvents
TIMESTAMP BY EventTime
GROUP BY 
    DeviceId,
    HoppingWindow(minute, 10, 5)   -- size=10min, hop=5min
```

> 📌 **Mẹo nhớ**: Khi **Hop Size = Window Size** → Hopping Window **= Tumbling Window**

#### C. Sliding Window (Cửa sổ trượt)

- **Đặc điểm**: Chỉ output khi nội dung window **thay đổi** (event enter hoặc exit)
- **Không có schedule cố định** — event-driven
- **Phù hợp cho**: Phát hiện pattern trong khoảng thời gian cụ thể

```
Sliding Window = 5 minutes
Output ONLY when an event enters or exits the window

Event A arrives at t=0  → Window [t-5, t] outputs
Event B arrives at t=2  → Window [t-3, t+2] outputs
Event A exits at t=5    → Window [t, t+5] outputs
```

**Ví dụ Eventstream SQL Operator**:
```sql
SELECT
    DeviceId,
    COUNT(*) AS EventsInWindow
FROM IoTEvents
TIMESTAMP BY EventTime
GROUP BY 
    DeviceId,
    SlidingWindow(minute, 5)      -- Outputs khi content thay đổi
HAVING COUNT(*) > 3               -- Alert nếu > 3 events trong 5 phút
```

#### D. Session Window (Cửa sổ phiên)

- **Đặc điểm**: Groups events gần nhau theo thời gian, phân tách bởi **gap** (khoảng im lặng)
- **Kích thước KHÔNG cố định** — phụ thuộc vào sự liên tục của events
- **Phù hợp cho**: User session tracking, activity bursts

```
Gap timeout = 5 minutes

Events:  A  B  C         D  E     F
Time:    |--|--|---gap>5--|--|--gap>5--|
         [Session 1]     [Session 2]  [Session 3]
         (A,B,C)         (D,E)        (F)
```

**Ví dụ Eventstream SQL Operator**:
```sql
SELECT
    UserId,
    MIN(EventTime) AS SessionStart,
    MAX(EventTime) AS SessionEnd,
    DATEDIFF(second, MIN(EventTime), MAX(EventTime)) AS SessionDuration,
    COUNT(*) AS EventsInSession
FROM UserClickEvents
TIMESTAMP BY EventTime
GROUP BY 
    UserId,
    SessionWindow(minute, 5)      -- Gap = 5 phút inactivity
```

#### E. Snapshot Window (Cửa sổ ảnh chụp)

- **Đặc điểm**: Groups events có **cùng timestamp**
- **Phù hợp cho**: Batch-like events arriving simultaneously

**Ví dụ Eventstream SQL Operator**:
```sql
SELECT
    COUNT(*) AS SimultaneousEvents,
    System.Timestamp() AS WindowTime
FROM Events
TIMESTAMP BY EventTime
GROUP BY SnapshotWindow()
```

### 7.3. Tóm tắt Windowing Functions

| Window Type | Size | Overlap | Trigger | Use Case |
|------------|------|---------|---------|----------|
| **Tumbling** | Fixed | ❌ No | Time interval | Periodic reports (mỗi 5 phút) |
| **Hopping** | Fixed | ✅ Yes | Time interval | Smoothed averages (moving avg) |
| **Sliding** | Fixed | ✅ Yes | Event enter/exit | Pattern detection |
| **Session** | Variable | ❌ No | Gap timeout | User sessions, activity tracking |
| **Snapshot** | N/A | N/A | Same timestamp | Simultaneous event batches |

### 7.4. Windowing trong KQL (Eventhouse)

KQL sử dụng `bin()` function cho tumbling window và `make-series` cho time-series analysis:

```kql
// === Tumbling Window (bin) ===
SensorData
| where EventTime > ago(1h)
| summarize 
    AvgTemp = avg(Temperature),
    MaxTemp = max(Temperature),
    Events = count()
    by bin(EventTime, 5m), DeviceId       // 5-minute tumbling window

// === Hopping Window (simulation với bin và extend) ===
let windowSize = 10m;
let hopSize = 5m;
SensorData
| where EventTime > ago(1h)
| extend WindowStart = bin(EventTime - windowSize, hopSize) + hopSize
| extend WindowEnd = WindowStart + windowSize
| where EventTime between (WindowStart .. WindowEnd)
| summarize AvgTemp = avg(Temperature) by WindowStart, WindowEnd, DeviceId

// === Session Window (session_count) ===
UserEvents
| sort by UserId asc, EventTime asc
| extend SessionId = row_cumsum(
    iff(prev(UserId) != UserId or EventTime - prev(EventTime) > 5m, 1, 0)
)
| summarize 
    SessionStart = min(EventTime),
    SessionEnd = max(EventTime),
    EventCount = count()
    by UserId, SessionId

// === Time-series Analysis (make-series) ===
SensorData
| make-series AvgTemp = avg(Temperature) 
    on EventTime from ago(24h) to now() step 1h 
    by DeviceId
| render timechart
```

---

## 8. Câu hỏi mô phỏng bài thi (Practice Questions)

### Question 1

**You are designing a streaming solution in Microsoft Fabric. The requirements are:**
- **Sub-second latency** for a live operational dashboard
- **Alerting** when temperature exceeds thresholds
- **Historical storage** in a Delta Lake for monthly analytics

**Which architecture should you implement?**

A. Spark Structured Streaming → Lakehouse → Power BI  
B. Eventstream → Eventhouse only  
C. Eventstream → Eventhouse (for dashboard + alerts) AND Eventstream → Lakehouse (for historical storage)  
D. Data pipeline with 1-minute schedule → Warehouse

**✅ Correct Answer: C**

> **Giải thích**:
> - **(C) Đúng** — Eventstream hỗ trợ **fan-out** — 1 nguồn gửi tới nhiều đích. Eventhouse cho sub-second dashboard, Data Activator cho alerting, và Lakehouse cho historical analytics. Đây là kiến trúc Lambda architecture chuẩn trong Fabric.
> - **(A) Sai trong scenario này** — Spark Structured Streaming phù hợp streaming ETL/stateful processing nhưng không phải lựa chọn ưu tiên khi yêu cầu chính là dashboard KQL độ trễ thấp. Latency thực tế có thể từ giây đến phút tùy cấu hình.
> - **(B) Sai** — Chỉ Eventhouse không đáp ứng yêu cầu historical storage trong Delta Lake.
> - **(D) Sai** — Pipeline batch 1 phút không phải real-time.

---

### Question 2

**You have Delta tables in a Fabric Lakehouse that contain historical sales data. You need to query this data using KQL in an Eventhouse for time-series analysis. You want to minimize data duplication but need fast query performance.**

**What should you configure?**

A. Create native tables in the Eventhouse and copy the data  
B. Create standard OneLake shortcuts to the Lakehouse tables  
C. Create OneLake shortcuts with query acceleration enabled  
D. Use Spark Structured Streaming to write data to the Eventhouse

**✅ Correct Answer: C**

> **Giải thích**:
> - **(C) Đúng** — OneLake shortcuts + Query Acceleration cung cấp fast KQL queries trên Delta data mà KHÔNG cần copy data (minimize duplication). Acceleration indexes data locally cho performance gần bằng native tables.
> - **(A) Sai** — Copy data vi phạm yêu cầu "minimize duplication".
> - **(B) Sai** — Standard shortcuts không có indexing → query performance chậm, không đáp ứng "fast query".
> - **(D) Sai** — Streaming từ Lakehouse tới Eventhouse cũng tạo duplicate.

---

### Question 3

**You are building an Eventstream in Fabric that receives IoT sensor events. You need to calculate the average temperature for each device every 5 minutes, where each event belongs to exactly one time window with no overlap.**

**Which windowing function should you use?**

A. Hopping Window  
B. Sliding Window  
C. Session Window  
D. Tumbling Window

**✅ Correct Answer: D**

> **Giải thích**:
> - **(D) Đúng** — Tumbling Window có kích thước cố định, KHÔNG overlap, mỗi event thuộc **đúng 1 window**. Phù hợp cho periodic aggregation mỗi 5 phút.
> - **(A) Sai** — Hopping Window CÓ overlap → mỗi event có thể thuộc nhiều windows.
> - **(B) Sai** — Sliding Window trigger khi nội dung window thay đổi, không phải periodic.
> - **(C) Sai** — Session Window kích thước biến đổi dựa trên gap, không cố định 5 phút.

---

### Question 4

**You need to track user browsing sessions on a website. A new session should start after 15 minutes of inactivity. Each session should aggregate the total number of page views and session duration.**

**Which windowing function should you use?**

A. Tumbling Window with 15-minute size  
B. Hopping Window with 15-minute size and 5-minute hop  
C. Session Window with a 15-minute gap timeout  
D. Sliding Window with 15-minute duration

**✅ Correct Answer: C**

> **Giải thích**:
> - **(C) Đúng** — Session Window groups events dựa trên **gap timeout**. Khi không có event trong 15 phút → session kết thúc, session mới bắt đầu. Kích thước session KHÔNG cố định — phụ thuộc vào hoạt động của user.
> - **(A) Sai** — Tumbling Window cắt session cố định mỗi 15 phút, không liên quan tới user activity.
> - **(B) Sai** — Hopping Window cũng cố định, không phản ánh user behavior.
> - **(D) Sai** — Sliding Window trigger khi event enter/exit, không dựa trên inactivity gap.

---

### Question 5

**You have a Spark Structured Streaming job that processes events from Event Hubs. Some events arrive up to 10 minutes late due to network delays. You are performing windowed aggregations with 5-minute tumbling windows.**

**How should you handle the late-arriving events?**

A. Use `.withWatermark("EventTime", "10 minutes")` để giới hạn state bằng event-time watermark  
B. Increase the tumbling window size to 15 minutes  
C. Use `trigger(processingTime="10 minutes")` to wait for late data  
D. Remove the windowed aggregation and use batch processing instead

**✅ Correct Answer: A**

> **Giải thích**:
> - **(A) Đúng** — `withWatermark` dùng max event time đã thấy để tính current watermark. Với stateful operator, record cũ hơn watermark có thể bị drop; không phải phép so sánh đơn giản với wall-clock.
> - **(B) Sai** — Tăng window size thay đổi business logic, không giải quyết vấn đề late data.
> - **(C) Sai** — `processingTime` chỉ là khoảng cách giữa các micro-batches, không liên quan tới late data handling.
> - **(D) Sai** — Loại bỏ streaming không cần thiết khi watermark giải quyết được vấn đề.

---

### Question 6

**In a KQL database (Eventhouse), you want to automatically transform raw sensor data into a cleaned format as soon as new data is ingested. The transformation should add a calculated "AlertLevel" column.**

**Which KQL feature should you use?**

A. Materialized view  
B. Update policy  
C. Continuous export  
D. Stored function called manually

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — Update Policy tự động chạy transformation function mỗi khi data mới được ingested vào source table. Kết quả được append vào target table ngay lập tức. Đây là cách chuẩn để auto-transform data trong Eventhouse.
> - **(A) Sai** — Materialized views tối ưu cho pre-aggregation, nhưng Update Policy phù hợp hơn cho row-level transformation.
> - **(C) Sai** — Continuous export dùng để export data ra OneLake/ADLS, không phải transform.
> - **(D) Sai** — Stored function gọi thủ công không đáp ứng yêu cầu "automatically".

---

### Question 7

**You are using Eventstream to process clickstream data. You need to calculate a running count of events per user, where the window only produces output when an event enters or exits the time range, not at fixed intervals.**

**Which windowing function should you use?**

A. Tumbling Window  
B. Hopping Window  
C. Sliding Window  
D. Session Window

**✅ Correct Answer: C**

> **Giải thích**:
> - **(C) Đúng** — Sliding Window chỉ output khi **nội dung window thay đổi** (event enter hoặc exit). Nó không output tại fixed intervals như Tumbling/Hopping. Đây là window duy nhất event-driven.
> - **(A) Sai** — Tumbling Window output tại fixed intervals.
> - **(B) Sai** — Hopping Window cũng output tại fixed intervals (hop size).
> - **(D) Sai** — Session Window groups theo inactivity gap, không phải event enter/exit.

---

### Question 8

**You are configuring streaming data ingestion into a Fabric Eventhouse. The data source is high-velocity IoT telemetry (1 million events per second). You need the lowest possible query latency for real-time dashboards.**

**Which storage option should you use in the Eventhouse?**

A. OneLake shortcut to a Lakehouse Delta table  
B. OneLake shortcut with query acceleration enabled  
C. Native table with streaming ingestion policy enabled  
D. OneLake shortcut to Amazon S3

**✅ Correct Answer: C**

> **Giải thích**:
> - **(C) Đúng** — Native table + streaming ingestion là lựa chọn ưu tiên khi cần query latency thấp nhất trong Eventhouse và chấp nhận ingest/cached copy. Data được physically ingested, indexed, và available cho query trong giây. Đây thường là lựa chọn ưu tiên khi cần native Kusto ingestion/indexing và query latency thấp; throughput/latency phải sizing theo event size, partitions và capacity.
> - **(A) Sai** — Standard shortcut không có indexing, latency cao.
> - **(B) Sai** — Accelerated shortcut tốt nhưng không bằng native table, và không phù hợp cho high-velocity streaming (shortcuts designed cho batch data).
> - **(D) Sai** — Shortcut tới S3 có network latency cao nhất.

---

### Question 9

**You need to calculate a "smoothed" moving average of sensor temperatures using a 10-minute window that advances every 2 minutes, so that each data point is included in multiple overlapping calculations.**

**Which windowing function should you use?**

A. Tumbling Window (10 minutes)  
B. Hopping Window (10-minute size, 2-minute hop)  
C. Sliding Window (10 minutes)  
D. Session Window (10-minute gap)

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — Hopping Window cho phép tạo overlapping windows (size=10min, hop=2min). Mỗi data point sẽ thuộc **5 windows** (10/2 = 5), tạo ra smoothed moving average effect.
> - **(A) Sai** — Tumbling Window KHÔNG overlap → mỗi point chỉ thuộc 1 window, không tạo moving average.
> - **(C) Sai** — Sliding Window trigger theo event, không theo fixed hop interval.
> - **(D) Sai** — Session Window dựa trên inactivity gap, không liên quan tới moving average.

---

### Question 10

**You are running a Spark Structured Streaming job in production that continuously processes events from Azure Event Hubs. The job occasionally fails due to transient errors. You need to ensure the job automatically restarts on failure.**

**What should you use?**

A. Run the streaming job in an interactive Fabric Notebook  
B. Use a Spark Job Definition with retry policies configured  
C. Add a try/except block around the streaming code  
D. Schedule the notebook to run every 5 minutes using a pipeline

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — Spark Job Definitions hỗ trợ **retry policies** built-in, tự động restart job khi gặp transient errors. Đây là best practice cho production streaming workloads trong Fabric (2026).
> - **(A) Sai** — Interactive Notebook không có auto-retry mechanism.
> - **(C) Sai** — try/except trong code không thể restart Spark session sau cluster-level failures.
> - **(D) Sai** — Schedule notebook mỗi 5 phút có thể tạo duplicate processing nếu job trước chưa fail.

---

### Question 11

**A company uses Eventstream to ingest data from Azure Event Hubs. They need to:**
1. **Filter** out test events (where `Environment = "test"`)
2. **Route** production events to BOTH an Eventhouse AND a Lakehouse simultaneously

**How should they configure the Eventstream?**

A. Create two separate Eventstreams — one for each destination  
B. Create one Eventstream with a Filter transformation and two destinations (Eventhouse + Lakehouse)  
C. Create one Eventstream → Eventhouse, then use a pipeline to copy from Eventhouse to Lakehouse  
D. Use Spark Structured Streaming instead of Eventstream

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — Eventstream natively hỗ trợ **in-stream transformations** (Filter) và **multiple destinations** (fan-out). Một Eventstream có thể filter test events VÀ route production events tới cả Eventhouse và Lakehouse cùng lúc.
> - **(A) Sai** — Hai Eventstreams riêng biệt sẽ consume Event Hub partition doubled, phức tạp không cần thiết.
> - **(C) Sai** — Pipeline copy thêm latency và complexity.
> - **(D) Sai** — Spark Structured Streaming phức tạp hơn cho task này khi Eventstream UI đã đủ.

---

### Question 12 — Drag and Drop Style

**Match each streaming scenario with the most appropriate technology:**

| Scenario | Technology |
|----------|-----------|
| 1. Sub-second alerting dashboard for manufacturing IoT | **?** |
| 2. Streaming ETL from Event Hub to Delta Lake for ML training | **?** |
| 3. Low-code event routing and filtering | **?** |
| 4. Auto-transform raw events upon ingestion in KQL database | **?** |

**Options**: A. Eventstream   B. Spark Structured Streaming   C. Eventhouse + KQL Update Policy   D. Eventhouse + Real-Time Dashboard

**✅ Correct Answers:**
1. **D** — Eventhouse + Real-Time Dashboard (low-latency KQL queries)
2. **B** — Spark Structured Streaming (streaming ETL → Delta Lake)
3. **A** — Eventstream (low-code UI for routing/filtering)
4. **C** — Eventhouse + KQL Update Policy (auto-transform on ingestion)

> **Giải thích**:
> - Scenario 1: Real-Time Dashboard + Eventhouse phù hợp live monitoring với low-latency KQL; không coi sub-second là SLA mặc định.
> - Scenario 2: Spark Structured Streaming là công cụ chuẩn cho streaming ETL vào Delta Lake.
> - Scenario 3: Eventstream có UI low-code cho routing, filtering, splitting streams.
> - Scenario 4: KQL Update Policy tự động transform data khi ingestion xảy ra.

---

## 📋 TỔNG KẾT: Ingest and Transform Data (30–35%)

### Checklist kiến thức cần nắm vững

| # | Chủ đề | Mức độ quan trọng |
|---|--------|-------------------|
| 1 | Full Load vs Incremental Load (Watermark, CDC, MERGE INTO) | ⭐⭐⭐⭐⭐ |
| 2 | Star Schema, Surrogate Keys, SCD Type 1 & 2 | ⭐⭐⭐⭐⭐ |
| 3 | Chọn Data Store (Lakehouse vs Warehouse vs Eventhouse) | ⭐⭐⭐⭐⭐ |
| 4 | Chọn Transformation Tool (Dataflows Gen2 vs Notebook vs T-SQL vs KQL) | ⭐⭐⭐⭐⭐ |
| 5 | OneLake Shortcuts (types, limitations, billing) | ⭐⭐⭐⭐ |
| 6 | Mirroring (supported sources, limitations) | ⭐⭐⭐⭐ |
| 7 | Pipeline activities (Copy, ForEach, Lookup, Parameters) | ⭐⭐⭐⭐ |
| 8 | PySpark transformations (filter, join, window, aggregate) | ⭐⭐⭐⭐⭐ |
| 9 | Windowing Functions (Tumbling, Hopping, Sliding, Session) | ⭐⭐⭐⭐⭐ |
| 10 | Eventstream (sources, transforms, destinations, fan-out) | ⭐⭐⭐⭐ |
| 11 | Spark Structured Streaming (triggers, checkpoints, watermarks) | ⭐⭐⭐⭐ |
| 12 | Native Tables vs Shortcuts vs Query Acceleration | ⭐⭐⭐⭐ |
| 13 | Handling duplicates, missing, late-arriving data | ⭐⭐⭐⭐ |
| 14 | KQL basics (summarize, parse_json, update policy) | ⭐⭐⭐ |
| 15 | Denormalization and aggregation patterns | ⭐⭐⭐ |

### Quick Reference: Keyword → Answer Mapping (cho thi)

| Keyword trong câu hỏi | Likely Answer |
|------------------------|---------------|
| "near real-time, many tables, zero-ETL" | **Mirroring** |
| "without copying data" | **OneLake Shortcut** |
| "no-code, citizen developer, small data" | **Dataflow Gen2** |
| "complex transformation, large data" | **Notebook (Spark)** |
| "sub-second latency, real-time dashboard" | **Eventhouse + Eventstream** |
| "watermark, last modified date" | **Incremental Load (Watermark pattern)** |
| "historical tracking, dimension changes" | **SCD Type 2** |
| "non-overlapping, fixed interval" | **Tumbling Window** |
| "overlapping, moving average" | **Hopping Window** |
| "inactivity gap, user sessions" | **Session Window** |
| "process all available then stop" | **trigger(availableNow=True)** |
| "auto-transform on ingestion in KQL" | **Update Policy** |
| "ADLS Gen2, S3, GCS access" | **OneLake Shortcut** |
| "Azure SQL, Cosmos DB, Snowflake replication" | **Mirroring** |

---

*Tài liệu được tổng hợp dựa trên Skills Measured as of July 21, 2026*  
*Nguồn tham khảo: Microsoft Learn, Microsoft Fabric Documentation*


---

## 9. BỔ SUNG ÔN THI 2026: KQL WINDOW VÀ QUERY ACCELERATION

### 9.1 KQL tumbling window

```kusto
Telemetry
| where Timestamp > ago(30m)
| summarize AvgTemp = avg(Temperature), EventCount = count()
    by DeviceId, bin(Timestamp, 5m)
```

### 9.2 KQL moving window gần đúng bằng `make-series`

```kusto
Telemetry
| make-series AvgTemp = avg(Temperature) default=double(null)
    on Timestamp from ago(1h) to now() step 1m by DeviceId
| extend Moving5m = series_moving_avg_fl(AvgTemp, 5)
```

### 9.3 Native table, shortcut hay acceleration

- Native table: ingest/copy vào Eventhouse, indexes/cache tối ưu, feature set KQL đầy đủ nhất.
- Standard OneLake shortcut: không copy, query bằng `external_table()`, chi phí lưu trữ thấp nhưng chậm hơn.
- Query acceleration: cache một khoảng thời gian của external Delta table; hiệu năng gần native cho query phù hợp nhưng có billing/limitations và không hỗ trợ mọi feature như native table.

### 9.4 Câu hỏi tình huống bổ sung

**Dữ liệu đã ở Delta trong OneLake. Yêu cầu tránh pipeline ingest trùng lặp nhưng dashboard KQL đọc 7 ngày gần nhất phải nhanh. Chọn gì?**

A. Standard shortcut không cache  
B. OneLake shortcut với query acceleration và caching period phù hợp  
C. Spark batch overwrite mỗi phút  
D. Import vào semantic model

<details><summary>Đáp án</summary>

**B.** Query acceleration cache phạm vi dữ liệu cần query mà vẫn giữ source of truth trong OneLake.
</details>


---

## 10. CÂU HỎI SCENARIO — STREAMING DATA

> Các câu hỏi dưới đây là câu tự soạn theo blueprint DP-700, **không phải câu hỏi thật hoặc exam dump**.

### Câu D2-ST1 — Streaming Dashboard Real-Time

**Yêu cầu: latency thấp (gần real-time), alert khi ngưỡng vượt quá, source từ Event Hub, và dashboard KQL. Giải pháp nào phù hợp nhất?**

A. Eventstream → Eventhouse (Real-Time Intelligence)  
B. Pipeline batch hàng ngày  
C. Import semantic model  
D. Full load notebook mỗi giờ  

<details><summary>Đáp án và giải thích</summary>

**A.** [Real-Time Intelligence (RTI)](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/overview) gồm Eventstream (ingestion) → Eventhouse (KQL storage) → Real-Time Dashboard phù hợp hoàn hảo cho operational real-time analytics với latency thấp và alerting. Batch pipeline không đáp ứng được yêu cầu near real-time.

</details>

---

### Câu D2-ST2 — Stateful Stream-Stream Join

**Cần thực hiện stream-stream join giữa hai event streams (orders và inventory) dựa trên key và time window, sau đó ghi kết quả vào Delta Lake với checkpoint đảm bảo exactly-once. Công cụ nào phù hợp?**

A. Spark Structured Streaming  
B. Deployment pipeline  
C. Sensitivity label  
D. Git integration  

<details><summary>Đáp án và giải thích</summary>

**A.** [Spark Structured Streaming](https://learn.microsoft.com/en-us/fabric/data-engineering/structured-streaming-overview) hỗ trợ **stateful stream-stream join** với state store, checkpoint (fault-tolerance), và Delta Lake sink. Đây là công cụ phù hợp nhất cho stateful ETL streaming phức tạp.

</details>

---

### Câu D2-ST3 — Shortcut + Query Acceleration

**Delta table lưu trong OneLake. Không muốn copy/ingest vào Eventhouse, nhưng dashboard KQL cần query nhanh trên 7 ngày gần nhất. Giải pháp tối ưu?**

A. Standard shortcut (không cache)  
B. OneLake shortcut + query acceleration (cache 7 ngày)  
C. Chỉ dùng native table (phải ingest)  
D. Export CSV ra ngoài  

<details><summary>Đáp án và giải thích</summary>

**B.** [Query acceleration](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/query-acceleration-overview) cache một khoảng thời gian của external Delta table trong Eventhouse — giữ **source of truth trong OneLake** (không copy toàn bộ) nhưng tăng query performance đáng kể cho phạm vi được cache. Standard shortcut query chậm hơn vì không có cache.

</details>

---

### Câu D2-ST4 — Tumbling Window

**Mỗi event chỉ thuộc đúng một cửa sổ thời gian 5 phút, các cửa sổ không chồng lên nhau. Window type nào phù hợp?**

A. Tumbling window  
B. Sliding window  
C. Session window  
D. Snapshot window  

<details><summary>Đáp án và giải thích</summary>

**A.** [Tumbling window](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/create-notebookv2) có kích thước cố định, **không chồng lấp (non-overlapping)** — mỗi event thuộc đúng một window. Sliding window các cửa sổ chồng lên nhau; session window dựa trên inactivity gap.

</details>

---

### Câu D2-ST5 — Session Window

**Muốn nhóm events theo phiên làm việc của user, mỗi phiên kết thúc sau 15 phút không có activity. Window type nào?**

A. Tumbling  
B. Sliding  
C. Session  
D. Hopping 1 ngày  

<details><summary>Đáp án và giải thích</summary>

**C.** [Session window](https://learn.microsoft.com/en-us/azure/stream-analytics/stream-analytics-window-functions#session-window) sử dụng **inactivity gap** — window đóng lại sau khoảng thời gian không có event mới. Phù hợp cho user session analysis, clickstream, hoặc bất kỳ pattern nào dựa trên khoảng nghỉ giữa events.

</details>

---

### Câu D2-ST6 — State Unbounded Growth

**Spark Structured Streaming job đang chạy nhưng memory sử dụng tăng liên tục không giới hạn. Nguyên nhân và giải pháp?**

A. Bỏ checkpoint để giảm tải  
B. Thêm **watermark** và/hoặc **TTL** phù hợp; thiết kế key và window hợp lý để bound state  
C. Tăng Viewer role cho pipeline  
D. Git commit lại notebook  

<details><summary>Đáp án và giải thích</summary>

**B.** **Unbounded state** là vấn đề phổ biến khi dùng stateful operations (join, groupBy không window). Giải pháp:
- Thêm [watermark](https://learn.microsoft.com/en-us/fabric/data-engineering/structured-streaming-overview#watermarking) để bound late data
- Dùng `mapGroupsWithState` với TTL (Time-To-Live)
- Thiết kế key granularity phù hợp (tránh key cardinality quá cao)

Bỏ checkpoint sẽ phá vỡ fault tolerance, không giải quyết được memory issue.

</details>

---

## 11. LAB THỰC HÀNH — STREAMING DATA

### Lab 3 — Spark Structured Streaming Với Watermark

**Mục tiêu:** Thực hành streaming aggregation có watermark để xử lý late-arriving data.

```python
from pyspark.sql.functions import avg, count, window

# ═══════════════════════════════════════════════════
# Tạo stream giả lập (rate source — dùng trong Fabric Notebook)
# ═══════════════════════════════════════════════════
stream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 10)
    .load()
    .selectExpr(
        "timestamp AS EventTime",
        "CAST(value % 5 AS STRING) AS DeviceId",
        "CAST(20 + value % 15 AS DOUBLE) AS Temperature"
    )
)

# ═══════════════════════════════════════════════════
# Aggregation với watermark: cho phép event đến muộn ≤2 phút
# ═══════════════════════════════════════════════════
agg = (
    stream
    # Watermark = max EventTime đã thấy - 2 phút; record cũ hơn current watermark có thể bị loại
    .withWatermark("EventTime", "2 minutes")
    # Tumbling window 1 phút per DeviceId
    .groupBy(window("EventTime", "1 minute"), "DeviceId")
    .agg(
        avg("Temperature").alias("AvgTemp"),
        count("*").alias("EventCount")
    )
)

# ═══════════════════════════════════════════════════
# Ghi kết quả vào Delta table với checkpoint
# ═══════════════════════════════════════════════════
query = (
    agg.writeStream
    .format("delta")
    .outputMode("append")              # append: chỉ ghi kết quả đã finalized
    .option("checkpointLocation", "Files/checkpoints/lab_streaming")
    .trigger(processingTime="30 seconds")  # micro-batch mỗi 30 giây
    .toTable("lab.device_minute_agg")
)
# query.stop()  # Dừng query khi xong
```

**Điểm cần hiểu:**
- `withWatermark` → giới hạn state size và bounded lateness theo event-time watermark
- `outputMode("append")` → chỉ output khi window được finalized (sau watermark)
- `checkpointLocation` → đảm bảo fault-tolerance, rerun tiếp tục từ chỗ dừng

---

### Lab 4 — KQL Core Operators

**Mục tiêu:** Thực hành các KQL operator thường gặp trong đề thi — filter, aggregate, bin, percentile.

```kusto
// Phân tích telemetry 1 giờ gần nhất
let StartTime = ago(1h);
Telemetry
| where Timestamp >= StartTime
| project Timestamp, DeviceId, Temperature, Site   // Chọn cột cần thiết
| summarize
    AvgTemp  = avg(Temperature),
    P95Temp  = percentile(Temperature, 95),         // Percentile 95
    Events   = count()
    by Site, DeviceId, bin(Timestamp, 5m)           // Group theo 5-minute bucket
| where P95Temp > 80                                // Filter sau aggregation
| order by Timestamp desc
```

**Các operator cần nhớ:**

| Operator | Mục đích |
|---------|---------|
| `where` | Filter rows (giống WHERE trong SQL) |
| `project` | Chọn/rename cột (giống SELECT) |
| `summarize` | Aggregate + group by |
| `bin(column, interval)` | Làm tròn giá trị xuống bucket gần nhất |
| `percentile(col, N)` | Tính percentile thứ N |
| `order by` | Sắp xếp kết quả |

---

### Lab 5 — KQL Join Với Reference Data

**Mục tiêu:** Kết hợp streaming data với reference table trong Eventhouse.

```kusto
// Left outer join: lấy thông tin device từ reference table
Telemetry
| where Timestamp > ago(30m)
| join kind=leftouter (
    DeviceReference                    // Reference table (lookup)
) on DeviceId                          // Join key
| project
    Timestamp,
    DeviceId,
    Site,
    DeviceType,                        // Từ DeviceReference
    Temperature
| summarize
    AvgTemp = avg(Temperature)
    by Site, DeviceType, bin(Timestamp, 5m)
```

**Các loại join trong KQL:**

| Kind | Mô tả |
|------|-------|
| `inner` | Chỉ giữ rows match cả hai bên |
| `leftouter` | Giữ tất cả rows từ left, NULL nếu không match |
| `rightouter` | Giữ tất cả rows từ right |
| `fullouter` | Giữ tất cả rows từ cả hai |

> [!TIP]
> Tham khảo KQL documentation: [Kusto Query Language overview](https://learn.microsoft.com/en-us/kusto/query/)

---

## 12. CODE CHEAT SHEET — STREAMING/KQL

```kusto
Telemetry
| where Timestamp > ago(1h)
| extend Temp=todouble(Temperature)
| project Timestamp,DeviceId,Site,Temp
| summarize AvgTemp=avg(Temp),MaxTemp=max(Temp),Events=count()
  by Site,DeviceId,bin(Timestamp,5m)
| order by Timestamp desc
```

```kusto
DeviceStatus
| summarize arg_max(Timestamp, *) by DeviceId
```

```kusto
RawEvents
| extend p=parse_json(Body)
| extend DeviceId=tostring(p.deviceId), Metrics=p.metrics
| mv-expand Metrics
| project Timestamp,DeviceId,Metric=tostring(Metrics.name),Value=todouble(Metrics.value)
```

```kusto
Telemetry
| join kind=leftouter (DeviceReference | project DeviceId,Site,DeviceType) on DeviceId
| summarize AvgTemp=avg(Temperature) by Site,DeviceType
```

**Eventstream SQL operator:**
```sql
SELECT System.Timestamp AS WindowEnd, DeviceId, AVG(Temperature) AS AvgTemp
INTO output
FROM input
GROUP BY DeviceId, TumblingWindow(minute,5)
```

```sql
SELECT System.Timestamp AS WindowEnd, UserId, COUNT(*) AS OrderCount
INTO output
FROM input
GROUP BY UserId, HoppingWindow(minute,10,2)
HAVING COUNT(*)>20
```

```text
Tumbling = fixed non-overlap
Hopping  = fixed overlap (size + hop)
Sliding  = event-driven
Session  = inactivity gap
Snapshot = same timestamp
KQL bin() = fixed bucket cho summarize, không phải toàn bộ Stream Analytics window semantics
```

---

## NGUỒN CHÍNH THỨC ĐÃ ĐỐI CHIẾU

- [DP-700 Study Guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)
- [Structured Streaming overview](https://learn.microsoft.com/en-us/fabric/data-engineering/structured-streaming-overview)
- [Structured Streaming best practices](https://learn.microsoft.com/en-us/fabric/data-engineering/structured-streaming-best-practices)
- [Real-time Mode](https://learn.microsoft.com/en-us/fabric/data-engineering/structured-streaming-real-time-mode)
- [Eventstream overview](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/event-streams/overview)
- [Eventstream SQL operator](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/event-streams/process-events-using-sql-code-editor)
- [Query acceleration](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/query-acceleration-overview)
- [KQL update policy](https://learn.microsoft.com/en-us/kusto/management/update-policy?view=microsoft-fabric)


- [DP-700 study guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)
- [Query acceleration](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/query-acceleration-overview)
- [Structured Streaming best practices](https://learn.microsoft.com/en-us/fabric/data-engineering/structured-streaming-best-practices)
- [OneLake shortcuts in KQL database](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/onelake-shortcuts)

---

