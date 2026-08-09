# DP-700 HANDS-ON LABS: PYSPARK, KQL, T-SQL VÀ PIPELINE

> Các lab này dùng dữ liệu mẫu tự tạo. Hãy chạy trong workspace test, không dùng production data.

## Lab 1 — Full load và incremental MERGE

```python
from pyspark.sql import functions as F
from delta.tables import DeltaTable

initial = spark.createDataFrame([
    (1, "A", 10.0, "2026-08-01T10:00:00"),
    (2, "B", 20.0, "2026-08-01T10:00:00"),
], ["OrderId", "Status", "Amount", "ModifiedDate"])

(initial
 .withColumn("ModifiedDate", F.to_timestamp("ModifiedDate"))
 .write.format("delta").mode("overwrite").saveAsTable("lab.orders"))

changes = spark.createDataFrame([
    (2, "C", 25.0, "2026-08-02T10:00:00"),
    (3, "A", 30.0, "2026-08-02T10:00:00"),
], ["OrderId", "Status", "Amount", "ModifiedDate"]).withColumn(
    "ModifiedDate", F.to_timestamp("ModifiedDate")
)

target = DeltaTable.forName(spark, "lab.orders")
(target.alias("t")
 .merge(changes.alias("s"), "t.OrderId = s.OrderId")
 .whenMatchedUpdateAll(condition="s.ModifiedDate >= t.ModifiedDate")
 .whenNotMatchedInsertAll()
 .execute())
```

**Kiểm tra:** rerun MERGE không tạo duplicate; xem `DESCRIBE HISTORY lab.orders`.

## Lab 2 — SCD Type 2 tối giản

```python
from pyspark.sql import functions as F
from delta.tables import DeltaTable

incoming = spark.createDataFrame([
    (101, "Alice", "Gold"),
    (102, "Bob", "Silver"),
], ["CustomerId", "Name", "Segment"])

# Khởi tạo dimension
(initial := incoming
 .withColumn("EffectiveFrom", F.current_timestamp())
 .withColumn("EffectiveTo", F.lit(None).cast("timestamp"))
 .withColumn("IsCurrent", F.lit(True))) \
 .write.format("delta").mode("overwrite").saveAsTable("lab.dim_customer")

# Bài tập: tạo changes cho CustomerId=101, expire current row rồi insert version mới.
```

## Lab 3 — Structured Streaming với watermark

```python
from pyspark.sql.functions import avg, count, window

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

agg = (
    stream
    .withWatermark("EventTime", "2 minutes")
    .groupBy(window("EventTime", "1 minute"), "DeviceId")
    .agg(avg("Temperature").alias("AvgTemp"), count("*").alias("Events"))
)

query = (
    agg.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "Files/checkpoints/lab_rate")
    .trigger(processingTime="30 seconds")
    .toTable("lab.device_minute")
)
```

## Lab 4 — KQL core operators

```kusto
let Start = ago(1h);
Telemetry
| where Timestamp >= Start
| project Timestamp, DeviceId, Temperature, Site
| summarize AvgTemp=avg(Temperature), P95=percentile(Temperature,95), Events=count()
    by Site, DeviceId, bin(Timestamp, 5m)
| where P95 > 80
| order by Timestamp desc
```

## Lab 5 — KQL join và late reference data

```kusto
Telemetry
| where Timestamp > ago(30m)
| join kind=leftouter DeviceReference on DeviceId
| project Timestamp, DeviceId, Site, DeviceType, Temperature
| summarize AvgTemp=avg(Temperature) by Site, DeviceType, bin(Timestamp, 5m)
```

## Lab 6 — Warehouse T-SQL

```sql
CREATE TABLE dbo.FactSales (
    SalesId bigint NOT NULL,
    OrderDate date NOT NULL,
    CustomerId bigint NOT NULL,
    Amount decimal(18,2) NOT NULL
);

SELECT OrderDate, CustomerId, SUM(Amount) AS Revenue
FROM dbo.FactSales
GROUP BY OrderDate, CustomerId;
```

## Lab 7 — Optimization

```sql
OPTIMIZE lab.orders VORDER;
DESCRIBE HISTORY lab.orders;
VACUUM lab.orders DRY RUN;
```

**Bài tập:** so sánh file count trước/sau; không chạy retention thấp trên production.

## Lab 8 — Metadata-driven pipeline

Tạo control table gồm `EntityName`, `SourceQuery`, `TargetTable`, `LoadType`, `WatermarkColumn`. Pipeline:

1. Lookup enabled entities.
2. ForEach với batch count 4–5.
3. If `LoadType=Full` → Copy overwrite/replace.
4. Nếu incremental → Lookup watermark → Copy staging → Notebook MERGE.
5. Chỉ update watermark sau success.
6. On failure → Fail activity với message chứa entity/run id.

## Lab 9 — Workspace-wide monitoring query

```kusto
ItemJobEventLogs
| where ingestion_time() > ago(30m)
| summarize Runs=count(), Failures=countif(JobStatus == "Failed")
    by JobType, ItemName
| extend FailureRate = todouble(Failures) / Runs
| order by FailureRate desc
```

## Lab 10 — Security validation

- User A: Contributor → kiểm tra đọc mọi Lakehouse data.
- User B: Viewer + OneLake role chỉ `Sales` → kiểm tra `Sales` được đọc, `Payroll` bị chặn/filtered.
- Ghi lại access path: Spark, SQL endpoint, shortcut; so sánh enforcement và limitations.
