# DP-700: Identify and Resolve Errors
## Monitor and Optimize an Analytics Solution (30–35%) — Phần 2/3


> [!IMPORTANT]
> **Bản hiệu chỉnh ngày 09/08/2026.** Nội dung được đối chiếu với blueprint DP-700 có hiệu lực từ **21/07/2026**. Microsoft cho biết phần lớn câu hỏi tập trung vào tính năng GA, nhưng tính năng Preview phổ biến vẫn có thể xuất hiện. Với các bảng “supported items”, runtime, limits và trạng thái Preview/GA, hãy kiểm tra Microsoft Learn lần cuối trước ngày thi.

> **Skills measured as of July 21, 2026**  
> Tài liệu chuẩn bị thi chứng chỉ **Microsoft Fabric Data Engineer Associate (DP-700)**  
> Tham chiếu: [Microsoft Learn — Study Guide DP-700](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)

> [!NOTE]
> ### Cách dùng tài liệu này
> Hãy debug theo thứ tự: **scope → status → connection → identity/permission → schema/data → compute/capacity → retry decision**. Không học thuộc error code như flashcard độc lập.


---

## MỤC LỤC

1. [Identify and Resolve Pipeline Errors](#1-identify-and-resolve-pipeline-errors)
2. [Identify and Resolve Dataflow Gen2 Errors](#2-identify-and-resolve-dataflow-gen2-errors)
3. [Identify and Resolve Notebook Errors](#3-identify-and-resolve-notebook-errors)
4. [Identify and Resolve Eventhouse Errors](#4-identify-and-resolve-eventhouse-errors)
5. [Identify and Resolve Eventstream Errors](#5-identify-and-resolve-eventstream-errors)
6. [Identify and Resolve T-SQL Errors](#6-identify-and-resolve-t-sql-errors)
7. [Identify and Resolve OneLake Shortcut Errors](#7-identify-and-resolve-onelake-shortcut-errors)
8. [Câu hỏi mô phỏng bài thi (Practice Questions)](#8-câu-hỏi-mô-phỏng-bài-thi-practice-questions)

---

## 1. Identify and Resolve Pipeline Errors

### 1.1. Phương pháp debug Pipeline

**Bước 1: Monitoring Hub**
- Lọc Type = "Pipeline", Status = "Failed"
- Click vào pipeline run → xem failed activity

**Bước 2: Activity Error Details**
- Click vào failed activity → tab "Error"
- Đọc **Error Code** và **Error Message**
- Kiểm tra **Input** và **Output** tabs

**Bước 3: Dùng thông tin chẩn đoán có sẵn**
- Đọc error code/message, Input/Output, connection status, activity metrics và workspace monitoring logs
- Copilot/operations agent có thể hỗ trợ ở một số tenant hoặc trạng thái Preview, nhưng không thay thế quy trình xác minh root cause và không nên học như một nút UI bắt buộc trong bài thi

### 1.2. Lỗi Pipeline phổ biến và cách khắc phục

| Lỗi | Nguyên nhân | Giải pháp |
|-----|------------|-----------|
| **UserErrorFailedToConnectToSource** | Không kết nối được nguồn dữ liệu | Kiểm tra connection string, firewall, on-prem gateway |
| **UserErrorInvalidSource** | Source query sai hoặc table không tồn tại | Kiểm tra tên table, schema, query syntax |
| **UserErrorColumnMappingNotFound** | Cột trong mapping không tồn tại ở source/destination | Kiểm tra column mapping, schema drift |
| **UserErrorFailedToWriteToSink** | Không ghi được vào destination | Kiểm tra permissions, table lock, schema mismatch |
| **PipelineTimeoutError** | Pipeline chạy quá timeout | Tăng timeout setting, optimize query source |
| **AuthenticationError** | Token hết hạn hoặc credentials sai | Refresh credentials, kiểm tra Service Principal |
| **GatewayUnreachable** | On-premises gateway offline | Kiểm tra gateway service, network connectivity |
| **ConcurrentJobsLimitReached** | Vượt quá giới hạn concurrent runs | Giảm concurrency hoặc scale capacity |

### 1.3. Ví dụ Debug Pipeline Error

```
Scenario: Copy Activity failed
│
├── Error: UserErrorFailedToConnectToSource
│   Message: "TCP Provider - Could not establish connection to server 'myserver.database.windows.net'"
│
├── Root Cause Analysis:
│   1. ✅ Server name đúng?
│   2. ✅ Port 1433 mở trên firewall?
│   3. ✅ IP của Fabric được whitelist?
│   4. ✅ Credentials còn hợp lệ?
│   5. ✅ On-prem gateway (nếu cần) running?
│
└── Fix: Thêm Fabric IP ranges vào Azure SQL Firewall rules
```

### 1.4. Pipeline Retry Policy

```json
// Cấu hình retry cho activity
{
    "retryCount": 3,
    "retryInterval": "00:00:30",  // 30 giây giữa mỗi retry
    "secureInput": false,
    "secureOutput": false,
    "timeout": "0.12:00:00"       // Ví dụ 12 giờ; chọn theo SLA và giới hạn hiện hành
}
```

> 📌 **Điểm thi**: Retry policy giải quyết **transient errors** (network glitch, temporary throttle). Nếu lỗi persistent (sai schema, wrong credentials) → retry không giúp.

---

## 2. Identify and Resolve Dataflow Gen2 Errors

### 2.1. Khi nào Dataflow Gen2 lỗi?

Dataflow Gen2 validate ở 2 thời điểm:
1. **Publish time**: Kiểm tra cấu trúc, references, destination mapping
2. **Refresh time**: Kiểm tra kết nối, data types, permissions

### 2.2. Lỗi phổ biến Dataflow Gen2

| Lỗi | Nguyên nhân | Giải pháp |
|-----|------------|-----------|
| **Insufficient permissions for staging artifacts** | Owner chưa login >90 ngày hoặc đã rời tổ chức | User hiện tại **Take Ownership** |
| **Destination query missing** | Query không có destination mapping | Mở Dataflow → gán destination cho mọi output query |
| **Type mismatch error** | Data type nguồn không khớp destination schema | Thêm step "Change Type" trong Power Query |
| **Connection error** | Source không thể truy cập | Kiểm tra credentials, network, gateway |
| **Timeout error** | Refresh quá lâu | Optimize query (folding), giảm data volume |
| **Memory error** | Data quá lớn cho Dataflow | Chuyển sang Notebook (Spark) cho data lớn |
| **Broken reference** | Query reference tới query đã bị xóa/đổi tên | Sửa references trong Power Query editor |

### 2.3. Query Folding — Tối ưu Dataflow Gen2

**Query Folding** là khả năng push down transformation logic xuống source database (thay vì xử lý trong memory).

**Kiểm tra Query Folding**:
- Trong Power Query Editor → chuột phải lên step → "View Native Query"
  - Nếu thấy SQL query → ✅ Đã fold
  - Nếu greyed out → ❌ Không fold → performance chậm

**Steps thường KHÔNG fold**:
- Merge (Join) giữa nguồn khác nhau
- Custom columns với M functions phức tạp
- Group By trên merged results
- Pivot/Unpivot phức tạp

> 📌 **Best Practice**: Đặt foldable steps TRƯỚC non-foldable steps. Ví dụ: Filter → Select Columns → (fold tới đây) → Custom Column → Group By

---


## 3. Identify and Resolve Notebook Errors

Tài liệu: [Notebook limitations](https://learn.microsoft.com/en-us/fabric/data-engineering/notebook-limitation) và [Fabric Environments](https://learn.microsoft.com/en-us/fabric/data-engineering/environment-manage-library).

### 3.1. Lỗi phổ biến Notebook/Spark

| Lỗi | Root cause phổ biến | Hướng xử lý |
|---|---|---|
| `ModuleNotFoundError` | Library không có trong session | Environment hoặc inline install phù hợp |
| OOM / executor lost | partition lớn, skew, collect/cache | repartition, reduce data, fix skew |
| `AnalysisException` table not found | wrong context/schema/lakehouse | attachment + fully qualified name |
| Permission denied | thiếu workspace/item/data permission | grant đúng scope |
| Runtime incompatibility | package/runtime mismatch | chọn supported runtime/library |
| Session startup fail | service/capacity/transient | retry transient + check Capacity Metrics |

### 3.2. `%pip install` trong pipeline — cập nhật 2026

Bản cũ nói `%pip install` **không hoạt động** trong orchestrated notebook là quá tuyệt đối.

Current behavior:
- inline `%pip install` **disabled by default** khi notebook chạy qua pipeline/API;
- có thể bật bằng parameter `_inlineInstallationEnabled = true`;
- không hỗ trợ trong `notebookutils.notebook.run()` reference runs;
- không hỗ trợ High Concurrency;
- libraries không được retained giữa runs;
- dependency versions có thể drift.

Payload mẫu:

```json
{
  "parameters": [
    {
      "name": "_inlineInstallationEnabled",
      "value": true,
      "type": "Boolean"
    }
  ]
}
```

**Production recommendation vẫn là Fabric Environment** vì reproducible.

```text
Stable production dependencies → Fabric Environment
One-off inline install + supported execution mode → _inlineInstallationEnabled=true có thể đúng
```

### 3.3. Debug OOM / skew

```python
print(df.rdd.getNumPartitions())

df2 = df.repartition(200, "CustomerId")
df3 = df2.coalesce(50)

df3.explain("formatted")
```

Detect skew:

```python
from pyspark.sql import functions as F

(df.groupBy("CustomerId")
   .count()
   .orderBy(F.desc("count"))
   .show(20, truncate=False))
```

> ⚠️ `df.count()` trigger full action; không dùng chỉ để “ước lượng kích thước miễn phí”. Tránh `collect()` trên dataset lớn vì đưa toàn bộ data về driver.


## 4. Identify and Resolve Eventhouse Errors

Tài liệu: [Manage and monitor an Eventhouse](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/manage-monitor-eventhouse) · [Capacity Planner](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/eventhouse-smart-capacity-control).

### 4.1. Capacity Planner — tên mới của Always-On

Current UI/docs dùng **Capacity Planner** (trước đây gọi Always-On).

Capacity Planner:
- giữ Eventhouse active để tránh wake-up latency;
- recurring 7-day schedule;
- mỗi block 60 phút có thể đặt minimum CUs;
- autoscale vẫn scale up;
- khi enable mode mà chưa cấu hình schedule, docs hiện nêu baseline mặc định 2 CUs.

> 📌 Nếu đề cũ dùng “Always-On”, hiểu cùng dòng capability; khi thi 2026 ưu tiên thuật ngữ **Capacity Planner**.

### 4.2. Ingestion troubleshooting

```kusto
.show ingestion failures
| where FailedOn > ago(24h)
| project FailedOn, Table, FailureKind, Details
| order by FailedOn desc
```

```kusto
.show table SensorData policy streamingingestion
.show table SensorData policy caching
.show table SensorData policy retention
```

### 4.3. Streaming ingestion policy — cú pháp đúng

```kusto
.alter table SensorData policy streamingingestion
'{"IsEnabled": true}'
```

Disable:

```kusto
.alter table SensorData policy streamingingestion
'{"IsEnabled": false}'
```

### 4.4. Query chậm

```text
KQL query chậm
├─ Quét quá dài? → where time filter sớm
├─ Nhiều cột thừa? → project sớm
├─ Recent data không ở hot cache? → caching policy
├─ Repeated aggregation? → materialized view
└─ Capacity? → Eventhouse system overview / Capacity Metrics
```

Cache period phải nhỏ hơn hoặc bằng retention. Xem [KQL data policies](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/data-policies).


## 5. Identify and Resolve Eventstream Errors

### 5.1. Debug theo topology

```text
NO DATA / ERROR
├─ SOURCE
│  ├─ Source có đang phát event?
│  └─ Credential/network/firewall?
├─ PROCESSOR
│  ├─ Filter có loại hết data?
│  ├─ Schema/type conversion?
│  └─ SQL/operator expression?
└─ DESTINATION
   ├─ Destination available?
   └─ Mapping/schema/permission?
```

### 5.2. Workspace Monitoring

Khi workspace monitoring bật, các bảng có thể gồm:
- `EventStreamNodeStatus`
- `EventStreamMetrics`
- `EventStreamErrorMetrics`

```kusto
EventStreamErrorMetrics
| where Timestamp > ago(1h)
| order by Timestamp desc
```

> ⚠️ **Hiệu chỉnh:** Không học con số cứng kiểu “maximum/recommended ≤11 components” nếu current Microsoft Learn không xác nhận. Limits và preview behavior thay đổi nhanh.

Một số eventstream cũ có thể cần **republish** để tham gia workspace monitoring; preview cũng chưa cung cấp mọi detailed diagnostic log.


## 6. Identify and Resolve T-SQL Errors

Tài liệu bắt buộc: [T-SQL surface area in Fabric Warehouse](https://learn.microsoft.com/en-us/fabric/data-warehouse/tsql-surface-area).

### 6.1. Những điểm current dễ bị tài liệu cũ nói sai

#### `#temp` tables — ĐƯỢC hỗ trợ

Fabric Warehouse hiện hỗ trợ **session-scoped distributed temporary tables**.

```sql
CREATE TABLE #TopCustomers
(
    CustomerId BIGINT,
    Revenue DECIMAL(18,2)
);

INSERT INTO #TopCustomers
SELECT TOP 100
    CustomerId,
    SUM(Revenue)
FROM dbo.FactSales
GROUP BY CustomerId
ORDER BY SUM(Revenue) DESC;

SELECT * FROM #TopCustomers;
```

#### `MERGE` — GA

```sql
MERGE dbo.DimCustomer AS tgt
USING dbo.StgCustomer AS src
    ON tgt.CustomerCode = src.CustomerCode
WHEN MATCHED THEN
    UPDATE SET tgt.CustomerName = src.CustomerName
WHEN NOT MATCHED THEN
    INSERT (CustomerCode, CustomerName)
    VALUES (src.CustomerCode, src.CustomerName);
```

#### `IDENTITY` — Preview, `BIGINT` only

```sql
CREATE TABLE dbo.DimCustomer
(
    CustomerKey BIGINT IDENTITY,
    CustomerCode VARCHAR(50) NOT NULL,
    CustomerName VARCHAR(200)
);
```

Current limitations:
- chỉ `BIGINT`;
- không custom seed/increment;
- không `IDENTITY_INSERT`;
- không add IDENTITY vào existing table bằng `ALTER TABLE`.

Xem [IDENTITY columns](https://learn.microsoft.com/en-us/fabric/data-warehouse/identity).

#### Materialized views — KHÔNG hỗ trợ trong Warehouse

Warehouse T-SQL current surface area liệt kê **materialized views unsupported**.

Nếu cần declarative persisted transform trong **Lakehouse**, feature đúng là **Materialized Lake View** với `CREATE MATERIALIZED LAKE VIEW`.

### 6.2. Một số unsupported items cần nhớ

Current matrix có các ví dụ:
- materialized views;
- triggers;
- synonyms;
- recursive queries;
- manually created multi-column statistics.

> Không học một danh sách SQL Server cũ như luật vĩnh viễn; kiểm tra live T-SQL surface area trước ngày thi.

### 6.3. Query Insights để debug

```sql
SELECT TOP 20
    distributed_statement_id,
    query_hash,
    status,
    allocated_cpu_time_ms,
    data_scanned_remote_storage_mb,
    data_scanned_memory_mb,
    data_scanned_disk_mb,
    command
FROM queryinsights.exec_requests_history
ORDER BY start_time DESC;
```

Long-running:

```sql
SELECT TOP 20 *
FROM queryinsights.long_running_queries;
```

Current Warehouse guidance dùng **SHOWPLAN_XML trong SSMS** cho execution plan, không mặc định `EXPLAIN SELECT` như Spark.


## 7. Identify and Resolve OneLake Shortcut Errors

### 7.1. Checklist

```text
Shortcut không đọc được
├─ Target path/object còn tồn tại?
├─ Connection credential còn hợp lệ?
├─ User/workspace identity có quyền target?
├─ Firewall/private networking?
├─ Nếu shortcut ở Tables/: data có Delta-compatible?
├─ Schema/path source có đổi?
└─ Cache/network/egress có gây stale/latency?
```

### 7.2. Authentication mental model

- Internal OneLake shortcut: kiểm tra Fabric/OneLake permissions.
- ADLS Gen2/external: kiểm tra connection, target permission, firewall/private network.
- Trusted workspace access: cân nhắc Workspace Identity theo scenario.

### 7.3. Shortcut caching

[OneLake shortcut caching](https://learn.microsoft.com/en-us/fabric/onelake/onelake-shortcuts-cloud-cache) hỗ trợ một số external shortcut types như GCS, S3/S3-compatible và on-premises data gateway shortcuts.

Điểm cần nhớ:
- cache retention 1–28 ngày;
- files >1 GB có caching limitation;
- access có thể reset retention của cached file.

> ⚠️ Không học generic “Force Refresh Metadata” như solution chuẩn cho mọi shortcut. Current behavior chủ yếu tự phản ánh source; debug **path → connection → permission → table format → cache/network** trước.


## 8. Câu hỏi mô phỏng bài thi (Practice Questions)

### Question 1 — Transient failure
Pipeline thi thoảng timeout mạng và retry thường thành công.  
A. Retry with backoff · B. Grant Admin · C. Recreate Lakehouse · D. Disable schema validation  
**✅ Đáp án: A**

### Question 2 — Persistent credential
Password source đã đổi và pipeline fail mọi lần.  
A. Retry 10 lần · B. Update connection credential · C. Scale capacity · D. OPTIMIZE  
**✅ Đáp án: B**

### Question 3 — Production notebook dependency
Notebook chạy bằng pipeline cần package version ổn định/reproducible.  
A. `%pip install` mỗi run · B. Fabric Environment · C. Delta row · D. More partitions  
**✅ Đáp án: B**

### Question 4 — Inline install nuance
One-off pipeline run cần `%pip install`, không dùng High Concurrency/reference run.  
A. `_inlineInstallationEnabled = true` · B. V-Order · C. Viewer · D. DirectLakeOnly  
**✅ Đáp án: A**

### Question 5 — Eventhouse terminology
Tên hiện tại của capability trước gọi Always-On?  
A. Capacity Planner · B. V-Order · C. Query Insights · D. OneLake Cache  
**✅ Đáp án: A**

### Question 6 — `#temp`
Câu nào đúng?  
A. Unsupported · B. Session-scoped distributed `#temp` supported · C. Spark only · D. Lakehouse only  
**✅ Đáp án: B**

### Question 7 — IDENTITY
Fabric Warehouse IDENTITY hiện:  
A. GA mọi integer · B. Preview, `BIGINT` only · C. Unsupported · D. Lakehouse only  
**✅ Đáp án: B**

### Question 8 — Materialized view
Persisted declarative transform trong Lakehouse?  
A. `CREATE MATERIALIZED VIEW` Warehouse · B. `CREATE MATERIALIZED LAKE VIEW` · C. Trigger · D. #temp  
**✅ Đáp án: B**

### Question 9 — Eventstream no data
Source có event nhưng destination không nhận.  
A. Warehouse stats · B. Check processor + destination metrics/errors · C. Git · D. DirectLakeOnly  
**✅ Đáp án: B**

### Question 10 — Shortcut denied
Shortcut hiển thị nhưng external data access denied.  
A. Delete Monitor · B. Check connection/target permissions · C. More partitions · D. VACUUM  
**✅ Đáp án: B**

---

## 9. TROUBLESHOOTING PLAYBOOK

```text
Symptom → Scope → Monitoring evidence → Connection → Identity/permission
→ Schema/data → Compute/capacity → Retry or fix root cause
```

### PySpark skew quick check

```python
from pyspark.sql import functions as F

(df.groupBy("JoinKey")
   .count()
   .orderBy(F.desc("count"))
   .show(20, truncate=False))
```

### KQL ingestion failures

```kusto
.show ingestion failures
| where FailedOn > ago(1d)
| summarize Failures = count() by Table, FailureKind
| order by Failures desc
```

---

## 10. LAB THỰC HÀNH

- **Pipeline:** tạo wrong credential, wrong column và transient timeout; ghi `Symptom → Evidence → Root cause → Fix → Retry?`.
- **Notebook:** Environment vs inline `%pip`, đọc `_inlineInstallationEnabled`.
- **Warehouse:** chạy `#temp`, test `IDENTITY` nếu Preview enabled.
- **Eventhouse:** `.show ingestion failures`, caching/retention policy, mở Capacity Planner.

---

## 11. NGUỒN MICROSOFT LEARN TRỌNG YẾU

- [DP-700 Study Guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)
- [Notebook limitations](https://learn.microsoft.com/en-us/fabric/data-engineering/notebook-limitation)
- [Fabric Environment libraries](https://learn.microsoft.com/en-us/fabric/data-engineering/environment-manage-library)
- [Eventhouse overview](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/eventhouse)
- [Capacity Planner](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/eventhouse-smart-capacity-control)
- [KQL data policies](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/data-policies)
- [T-SQL surface area](https://learn.microsoft.com/en-us/fabric/data-warehouse/tsql-surface-area)
- [IDENTITY columns](https://learn.microsoft.com/en-us/fabric/data-warehouse/identity)
- [Query Insights](https://learn.microsoft.com/en-us/fabric/data-warehouse/query-insights)
- [OneLake shortcut caching](https://learn.microsoft.com/en-us/fabric/onelake/onelake-shortcuts-cloud-cache)

---

# 🎯 CHƯƠNG BỔ SUNG 06/09/2026 — HỌC LẠI TỪ GỐC

> **Vì sao có chương này:** *Identify and resolve errors* có tới **7 objective** trải khắp pipeline, Dataflow Gen2, notebook, Eventhouse, Eventstream, T-SQL và shortcut. Điều đề thật sự kiểm tra không phải là bạn thuộc mã lỗi, mà là bạn có **quy trình chẩn đoán** hay không: mở đúng nơi, đọc đúng thứ, sửa đúng nguyên nhân. Chương này viết lại theo dạng **playbook**.
> Xem thêm: [File tổng chỉ huy](DP700_00_MASTER_Trong_Tam_On_Thi.md)

## A. QUY TRÌNH CHẨN ĐOÁN CHUNG — DÙNG ĐƯỢC CHO MỌI LOẠI LỖI

```text
1. LỖI Ở ĐÂU?        Một item, hay nhiều item cùng lúc?
                     ├── Một item      → đi tiếp bước 2
                     └── Nhiều item    → nghi CAPACITY (Capacity Metrics) hoặc
                                          credential/connection dùng chung

2. THÔNG BÁO THẬT Ở ĐÂU?
                     Pipeline chỉ hiển thị lỗi chung chung của activity.
                     Lỗi thật nằm trong run history của chính notebook/dataflow đó.

3. ĐẦU VÀO CÓ ĐÚNG KHÔNG?
                     Mở tab INPUT của activity: parameter đã resolve thành giá trị thật chưa?
                     (Rất nhiều lỗi "path không tồn tại" thực chất là expression sai.)

4. LỖI THUỘC LOẠI NÀO?
                     ├── TRANSIENT   (timeout, throttle tạm thời)  → retry có ích
                     └── DETERMINISTIC (credential, schema, cú pháp, quyền) → retry vô ích

5. SỬA RỒI CHẠY LẠI TỪ ĐÂU?
                     Rerun từ activity lỗi nếu các bước trước idempotent và đã thành công.
```

> 📌 Bước 4 là chỗ đề hay gài: đáp án "tăng số lần retry" chỉ đúng với lỗi **transient**. Với lỗi credential/schema, retry chỉ làm tốn CU và che giấu nguyên nhân.

## B. PIPELINE — BỐN NHÓM LỖI VÀ CÁCH XỬ LÝ

| Nhóm lỗi | Dấu hiệu | Cách xử lý |
|---|---|---|
| **Expression/parameter sai** | Path lạ, tên bảng rỗng, `@{...}` xuất hiện nguyên văn | Mở tab **Input** của activity xem giá trị đã resolve; kiểm tra `@activity()`, `@item()`, `@concat()` |
| **Connection/credential** | Lỗi xác thực, 401/403, gateway | Kiểm tra connection, quyền của identity, secret hết hạn. **Retry vô ích** |
| **Downstream item lỗi** | Activity Notebook/Dataflow fail với thông báo chung | Mở **run history của item đó** để lấy stack trace thật |
| **Capacity/throttling** | Nhiều pipeline cùng fail hoặc timeout vào giờ cao điểm | **Capacity Metrics**; dời lịch, giảm concurrency, scale-up |

**Dependency conditions — dùng để xử lý lỗi có kiểm soát:**

```text
Activity A ──Success──► Activity B      (chỉ chạy khi A thành công)
           ──Failure──► Activity Log     (ghi log / gửi cảnh báo)
           ──Completion──► Activity C    (chạy dù A thành công hay thất bại)
           ──Skipped──► Activity D       (chạy khi A bị bỏ qua)
```

> 📌 `getActivityOutput` chỉ trả về output của activity **thành công**. Muốn truyền ngữ cảnh lỗi sang bước sau, hãy dùng parameter/variable hoặc activity **Fail** với thông báo có chứa tên entity và run id.

## C. DATAFLOW GEN2 — BA LỖI KINH ĐIỂN

| Lỗi | Nguyên nhân thật | Cách xử lý |
|---|---|---|
| "Refresh thành công nhưng dữ liệu sai/thiếu" | Lỗi ở tầng Power Query không làm fail refresh | Mở **refresh history của dataflow**, xem chi tiết từng query |
| Lỗi credential/gateway | Connection hết hạn hoặc thiếu quyền | Sửa connection; retry không giúp |
| Nhiều dataflow chạy song song trong pipeline trả lỗi chung chung | Tranh chấp compute/metadata | Giảm concurrency, tách lịch chạy; đọc lỗi cụ thể trong refresh history |

**Query folding — vừa là tối ưu, vừa là nguồn lỗi:**

```text
Query folding = Power Query đẩy phép biến đổi xuống NGUỒN thực thi
     ✔ Fold được  → nhanh, ít tài nguyên
     ✘ Không fold → kéo hết dữ liệu về Fabric rồi mới xử lý → chậm, dễ timeout
```

Bước làm gãy folding thường gặp: hàm tự viết, `Table.Buffer`, một số phép merge/pivot phức tạp. Kiểm tra bằng **View Native Query** trong Power Query.

## D. NOTEBOOK / SPARK — BỐN LỖI VÀ CÁCH ĐỌC

| Lỗi | Dấu hiệu trên Spark UI / log | Xử lý |
|---|---|---|
| **OOM / driver crash** | Executor bị kill, `OutOfMemoryError` | Tránh `collect()` trên dữ liệu lớn, giảm broadcast, tăng node size, chia nhỏ bước |
| **Data skew** | Max task duration lệch hẳn median | Salting, AQE skew join, repartition |
| **Spill** | Cột Spill (memory/disk) > 0 | Lọc/chiếu sớm, chỉnh shuffle partitions |
| **Thiếu thư viện** | `ModuleNotFoundError` | Cài trong **Environment** (dùng chung, ổn định) thay vì `%pip install` trong từng notebook (chỉ có hiệu lực cho session hiện tại) |

> 📌 Bẫy hay gặp: notebook chạy tay thì được, chạy trong pipeline thì lỗi thiếu thư viện → vì `%pip install` chỉ áp cho session, còn pipeline khởi tạo session mới theo Environment của workspace/item.

## E. T-SQL TRONG WAREHOUSE — GIỚI HẠN PHẢI BIẾT

Nhiều "lỗi" thực ra là **tính năng không được hỗ trợ**, không phải bug. Nhớ nhóm này để loại đáp án nhanh:

- Fabric Warehouse **không hỗ trợ materialized view** (Materialized **Lake** View thuộc Lakehouse/Spark).
- `IDENTITY` là **Preview** và có giới hạn (chỉ `BIGINT`, không tuỳ chỉnh seed/increment) — kiểm tra trạng thái trước khi dựa vào.
- Một số cấu trúc T-SQL truyền thống (ví dụ một vài kiểu ràng buộc, `#temp` với hành vi như SQL Server, một số hàm) có khác biệt — luôn đối chiếu **T-SQL surface area**.
- Ràng buộc khoá (PRIMARY KEY/UNIQUE/FOREIGN KEY) trong Warehouse **không được cưỡng chế** như SQL Server — không được dựa vào chúng để đảm bảo tính duy nhất; phải tự dedup.

> 📌 Khi đề đưa một câu lệnh T-SQL "chuẩn SQL Server" và hỏi vì sao lỗi trong Fabric, phản xạ đầu tiên là **surface area**, không phải cú pháp.

## F. EVENTHOUSE / EVENTSTREAM — CHẨN ĐOÁN THEO TOPOLOGY

```text
SOURCE ──► EVENTSTREAM ──► PROCESSOR ──► DESTINATION ──► BẢNG ĐÍCH
   │            │              │              │              │
   ▼            ▼              ▼              ▼              ▼
Có event    Có nhận      Filter có loại   Mapping    Ingestion có
gửi lên?    được không?  hết dữ liệu?     đúng cột?  báo lỗi không?
```

Khi "không thấy dữ liệu ở đích", đi lần lượt từ trái sang phải — đừng nhảy thẳng tới cuối. Các nguyên nhân theo thứ tự phổ biến:

1. Nguồn thật sự chưa gửi event (kiểm tra tại nguồn).
2. Bộ lọc/processor trong Eventstream loại hết bản ghi.
3. **Schema/mapping** giữa stream và bảng đích không khớp → ingestion fail.
4. Ingestion policy: dữ liệu vào theo **batching** nên chưa xuất hiện ngay (khác streaming ingestion).
5. Lỗi ở **update policy** của bảng đích (ví dụ join bị cấm khi source có streaming ingestion policy).

## G. ONELAKE SHORTCUT — CHECKLIST 5 BƯỚC

```text
1. Credential/identity tạo shortcut còn hợp lệ không? (secret hết hạn là nguyên nhân số 1)
2. Quyền ở PHÍA NGUỒN (S3 bucket policy, ADLS RBAC/ACL) đã cấp cho identity đó chưa?
3. Đường dẫn/bucket/container có đúng không? (phân biệt hoa thường, prefix)
4. Người dùng có quyền trong FABRIC trên item chứa shortcut không?
5. Có OneLake security/caching nào đang chặn hoặc trả dữ liệu cũ không?
```

> 📌 Phân biệt hai loại "không truy cập được": **shortcut hỏng** (Fabric không đọc được nguồn) và **người dùng bị chặn** (Fabric đọc được nhưng user không đủ quyền). Hai nguyên nhân, hai cách sửa khác nhau.

## H. RETRY VÀ IDEMPOTENCY — CẶP ĐI LIỀN NHAU

Retry chỉ an toàn khi bước đó **idempotent**. Đây là lý do phần xử lý lỗi luôn dính tới loading pattern:

```text
Bước KHÔNG idempotent:  INSERT thẳng vào bảng đích     → retry sinh dòng trùng
Bước idempotent:        MERGE có dedup + match key     → retry cho cùng kết quả

Watermark: chỉ commit SAU khi toàn bộ bước load thành công
           → retry không làm mất dữ liệu, cũng không nạp trùng
```

Cấu hình retry ở activity: số lần retry + khoảng chờ. Dùng cho lỗi transient; đặt số lần quá lớn sẽ làm chậm việc phát hiện lỗi thật.

## I. ACTIVE RECALL — 12 CÂU (che đáp án, nói thành tiếng)

<details><summary><b>1.</b> Activity Notebook fail, pipeline chỉ báo lỗi chung — bước tiếp theo?</summary>

Mở **run history của chính notebook** để lấy stack trace thật; đồng thời xem tab **Input** của activity để kiểm tra parameter đã resolve đúng.
</details>

<details><summary><b>2.</b> Khi nào retry có ích, khi nào vô ích?</summary>

Có ích với lỗi **transient** (timeout, throttle tạm thời). Vô ích với lỗi **deterministic** (credential, schema, cú pháp, quyền).
</details>

<details><summary><b>3.</b> Nhiều pipeline khác nhau cùng fail vào giờ cao điểm — nghi gì đầu tiên?</summary>

**Capacity throttling** (hoặc một connection/credential dùng chung). Mở Capacity Metrics trước khi debug từng pipeline.
</details>

<details><summary><b>4.</b> Vì sao notebook chạy tay được nhưng chạy trong pipeline lại thiếu thư viện?</summary>

`%pip install` chỉ có hiệu lực cho **session hiện tại**. Pipeline khởi tạo session mới theo **Environment** — phải cài thư viện trong Environment.
</details>

<details><summary><b>5.</b> Dataflow "refresh succeeded" nhưng dữ liệu thiếu — xem ở đâu?</summary>

**Refresh history của dataflow**, xem chi tiết từng query; lỗi ở tầng Power Query có thể không làm fail toàn bộ refresh.
</details>

<details><summary><b>6.</b> Query folding là gì và vì sao liên quan tới lỗi timeout?</summary>

Folding là đẩy phép biến đổi xuống nguồn thực thi. Khi gãy folding, toàn bộ dữ liệu bị kéo về Fabric rồi mới xử lý → chậm và dễ timeout. Kiểm tra bằng **View Native Query**.
</details>

<details><summary><b>7.</b> Bốn dependency condition và cái nào dùng để luôn chạy bước dọn dẹp?</summary>

Success, Failure, Completion, Skipped. Bước dọn dẹp/ghi log luôn chạy → dùng **Completion**.
</details>

<details><summary><b>8.</b> Vì sao không nên dựa vào PRIMARY KEY của Fabric Warehouse để chống trùng?</summary>

Ràng buộc khoá trong Warehouse **không được cưỡng chế** — phải tự dedup trong quá trình load.
</details>

<details><summary><b>9.</b> Eventstream chạy nhưng bảng đích không có dữ liệu — kiểm tra theo thứ tự nào?</summary>

Source có gửi → eventstream có nhận → processor/filter có loại hết → mapping/schema với đích có khớp → ingestion policy (batching vs streaming) → update policy của bảng đích.
</details>

<details><summary><b>10.</b> Shortcut báo lỗi truy cập — hai loại nguyên nhân khác nhau là gì?</summary>

(a) **Shortcut hỏng**: credential/quyền phía nguồn/đường dẫn sai. (b) **Người dùng bị chặn**: Fabric đọc được nhưng user thiếu quyền trên item hoặc bị OneLake security chặn.
</details>

<details><summary><b>11.</b> Retry an toàn khi nào?</summary>

Khi bước đó **idempotent**: MERGE có dedup + match key duy nhất, và watermark chỉ commit sau khi thành công.
</details>

<details><summary><b>12.</b> Câu lệnh T-SQL chuẩn SQL Server báo lỗi trong Fabric Warehouse — phản xạ đầu tiên?</summary>

Kiểm tra **T-SQL surface area** của Fabric Warehouse — nhiều tính năng không được hỗ trợ (ví dụ materialized view) chứ không phải lỗi cú pháp.
</details>

## J. MINI-LAB 60 PHÚT — TẠO LỖI RỒI CHỮA

1. Tạo pipeline với activity Notebook. Cố tình truyền sai tên bảng qua parameter → chạy → đọc lỗi ở pipeline, rồi mở run history của notebook để so sánh hai thông báo.
2. Sửa expression, dùng tab **Input** để xác nhận giá trị đã resolve đúng.
3. Thêm nhánh **Failure** gửi cảnh báo, và một activity **Completion** ghi log. Chạy lại và xác nhận cả hai nhánh hoạt động đúng.
4. Trong notebook, dùng `%pip install` một thư viện rồi chạy qua pipeline → tái hiện lỗi thiếu thư viện → sửa bằng cách cài trong **Environment**.
5. Tạo dataflow có bước làm gãy query folding; dùng **View Native Query** để xác nhận; sửa lại cho fold được.
6. Tạo shortcut tới một container với credential sai → đọc thông báo lỗi → sửa credential.
7. Chạy `INSERT` hai lần vào một bảng để tạo dòng trùng; sau đó thay bằng `MERGE` idempotent và chạy ba lần → xác nhận số dòng không đổi.

**Ngưỡng đạt:** đọc quy trình 5 bước ở mục A từ trí nhớ, và trả lời đúng ≥11/12 câu ở mục I.
