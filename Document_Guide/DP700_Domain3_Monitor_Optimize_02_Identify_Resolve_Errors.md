# DP-700: Identify and Resolve Errors
## Monitor and Optimize an Analytics Solution (30–35%) — Phần 2/3


> [!IMPORTANT]
> **Bản hiệu chỉnh ngày 04/08/2026.** Nội dung được đối chiếu với blueprint DP-700 có hiệu lực từ **21/07/2026**. Microsoft cho biết phần lớn câu hỏi tập trung vào tính năng GA, nhưng tính năng Preview phổ biến vẫn có thể xuất hiện. Với các bảng “supported items”, runtime, limits và trạng thái Preview/GA, hãy kiểm tra Microsoft Learn lần cuối trước ngày thi.

> **Skills measured as of July 21, 2026**  
> Tài liệu chuẩn bị thi chứng chỉ **Microsoft Fabric Data Engineer Associate (DP-700)**  
> Tham chiếu: [Microsoft Learn — Study Guide DP-700](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)

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

### 3.1. Lỗi phổ biến Notebook

| Lỗi | Nguyên nhân | Giải pháp |
|-----|------------|-----------|
| **Session timed out** | Notebook idle >20 phút | Rerun notebook (new session) |
| **Out of Memory (OOM)** | Data quá lớn cho executor memory | Tăng executor memory, repartition data, cache strategically |
| **ModuleNotFoundError** | Library chưa được install | Dùng **Fabric Environment** (không dùng `%pip install` trong pipeline) |
| **AnalysisException: Table not found** | Table/view không tồn tại | Kiểm tra tên table, schema, Lakehouse attachment |
| **Permission denied** | Thiếu quyền truy cập Lakehouse/Warehouse | Gán workspace role hoặc item-level permissions |
| **Kernel died** | Cluster crash hoặc capacity exhaustion | Kiểm tra Capacity Metrics App, tăng capacity |
| **Cannot connect to Livy endpoint** | Session startup failure | Retry sau vài phút, kiểm tra Fabric service health |

### 3.2. Debug Notebook có hệ thống

1. Đọc exception đầy đủ và xác định lỗi driver, executor, library, permission hay data/schema.
2. Kiểm tra Spark application/SQL logs và stage có skew, spill, failed tasks.
3. Xác minh Lakehouse attachment, Environment đã publish và runtime compatibility.
4. Copilot có thể gợi ý cách sửa khi tenant hỗ trợ, nhưng không giả định một lệnh `/fix` hay giới hạn kernel cụ thể nếu tài liệu chính thức không nêu.

### 3.3. Environment Management cho Notebooks

**Vấn đề**: `%pip install` trong notebook cell KHÔNG hoạt động khi notebook được gọi từ pipeline (bị restricted).

**Giải pháp**: Tạo **Fabric Environment**:
1. Workspace → New → Environment
2. Thêm libraries cần thiết (public PyPI hoặc custom .whl)
3. Publish Environment
4. Gán Environment cho Notebook hoặc Workspace default

```
✅ Đúng: Dùng Fabric Environment với pre-installed libraries
❌ Sai: Dùng %pip install trong orchestrated notebook
```

### 3.4. Xử lý OOM (Out of Memory) trong Spark

```python
# 1. Kiểm tra kích thước dữ liệu
df.count()
df.printSchema()

# 2. Repartition để phân bổ đều
df = df.repartition(200)  # Tăng partitions

# 3. Cache chỉ khi cần thiết
df.cache()  # Chỉ cache nếu df được dùng nhiều lần
df.unpersist()  # Giải phóng khi xong

# 4. Xử lý từng chunk thay vì toàn bộ
# Sử dụng filter để xử lý theo partition
for year in [2023, 2024, 2025]:
    chunk = df.filter(f"Year = {year}")
    process(chunk)

# 5. Tránh collect() trên large DataFrame
# ❌ Sai:
all_data = df.collect()  # Load toàn bộ vào driver memory

# ✅ Đúng:
df.limit(100).show()     # Xem sample
df.write.format("delta").save(...)  # Write trực tiếp
```

---

## 4. Identify and Resolve Eventhouse Errors

### 4.1. Lỗi phổ biến Eventhouse

| Lỗi | Nguyên nhân | Giải pháp |
|-----|------------|-----------|
| **Ingestion stopped** | Eventhouse paused do capacity downscale | Enable **"Always-On"** hoặc set Minimum CU |
| **Query timeout** | Query quá phức tạp hoặc data quá lớn | Thêm time filters, optimize KQL query |
| **Ingestion failure (type mismatch)** | Schema mismatch giữa source và table | Sửa ingestion mapping hoặc table schema |
| **Capacity throttling** | Vượt CU limit | Tăng capacity hoặc optimize queries |
| **Data conversion failure** | DateTime format không đúng | Convert qua int64 trước khi cast datetime |
| **Table not found** | Tên table sai hoặc database khác | Kiểm tra database context và tên table |

### 4.2. Giải quyết Ingestion Stoppage

```kql
// Kiểm tra ingestion status
.show ingestions failures

// Xem chi tiết lỗi ingestion
.show ingestion failures 
| where FailedOn > ago(24h)
| project Table, FailedOn, FailureKind, Details

// Kiểm tra streaming ingestion policy
.show table SensorData policy streamingingestion
```

**Enable Always-On**:
1. Mở Eventhouse → Settings
2. Tìm **"Always-On"** toggle → Enable
3. Hoặc set **Minimum Consumption Unit** > 0

### 4.3. Caching Policy Issues

```kql
// Kiểm tra caching policy hiện tại
.show table SensorData policy caching

// Dữ liệu query chậm → có thể data ở cold storage
// Giải pháp: Mở rộng hot cache period
.alter table SensorData policy caching hot = 30d

// Kiểm tra retention policy
.show table SensorData policy retention
```

---

## 5. Identify and Resolve Eventstream Errors

### 5.1. Lỗi phổ biến Eventstream

| Lỗi | Nguyên nhân | Giải pháp |
|-----|------------|-----------|
| **Failed to commit topology** | Browser storage đầy hoặc vượt giới hạn components | Clear browser cache; giảm components (<11) |
| **Failed to update** | Eventhouse paused sau capacity scaling | Enable "Always-On" trên Eventhouse |
| **Connection timeout** | Network issues, firewall blocking | Mở port 443; kiểm tra firewall rules |
| **Semaphore timeout** | On-premises source quá chậm phản hồi | Kiểm tra source server performance |
| **Data conversion failure** | Schema mismatch giữa source events và destination | Thêm transformation step để fix data types |
| **No data flowing** | Source stopped producing events | Kiểm tra Event Hub/IoT Hub/Kafka source |

### 5.2. Debug Eventstream

**Bước 1**: Mở Eventstream → **Monitoring tab**
- Xem message count, throughput, error count
- Identify node nào gặp lỗi

**Bước 2**: Kiểm tra từng node
- **Source node**: Events có đang được produce?
- **Transform nodes**: Có lỗi conversion không?
- **Destination nodes**: Destination có accessible?

**Bước 3**: Runtime logs
- Xem detailed error messages
- Phân biệt source-side vs stream-side failures

### 5.3. Topology Limits

```
Eventstream topology limits:
├── Maximum sources: Varies by source type
├── Maximum transformations: Depends on complexity
├── Maximum destinations: Multiple supported
└── Total components: Recommended ≤ 11
    (Vượt quá → "Failed to commit topology")
```

---

## 6. Identify and Resolve T-SQL Errors

### 6.1. Lỗi T-SQL phổ biến trong Fabric Warehouse

| Lỗi | Nguyên nhân | Giải pháp |
|-----|------------|-----------|
| **Unsupported feature** | T-SQL feature không supported trong Fabric | Kiểm tra [supported T-SQL syntax](https://learn.microsoft.com/en-us/fabric/data-warehouse/tsql-surface-area) |
| **Object not found** | Table/view/schema không tồn tại | Kiểm tra tên, schema prefix, database context |
| **Permission denied** | Thiếu quyền trên table/schema | Gán GRANT SELECT/INSERT/etc. |
| **Data type overflow** | Giá trị vượt quá data type range | Kiểm tra CAST/CONVERT, dùng BIGINT thay INT |
| **Lock timeout** | Concurrent writes gây blocking | Retry, optimize transaction scope |
| **Query timeout** | Query chạy quá lâu | Optimize query, thêm WHERE filters, check statistics |

### 6.2. T-SQL Syntax KHÔNG được hỗ trợ trong Fabric Warehouse

> ⚠️ **Rất quan trọng cho thi** — Fabric Warehouse KHÔNG hỗ trợ 100% T-SQL syntax

| Không hỗ trợ | Thay thế bằng |
|-------------|---------------|
| **CREATE INDEX** | Dùng Liquid Clustering, V-Order (auto-optimized) |
| **Triggers** | Dùng Pipeline orchestration |
| **Cursors** | Dùng set-based operations |
| **Temporary tables (#temp)** | Dùng CTEs hoặc CREATE TABLE |
| **Dynamic SQL (EXEC/sp_executesql)** | Hạn chế — dùng parameterized queries |
| **User-defined functions (complex)** | Dùng Views hoặc external transformations |
| **IDENTITY columns** | Dùng BIGINT + Surrogate Key logic |

### 6.3. Debug T-SQL trong Warehouse

```sql
-- Kiểm tra execution history
SELECT 
    command,
    status,
    error_code,
    DATEDIFF(second, start_time, end_time) AS duration_sec
FROM queryinsights.exec_requests_history
WHERE status = 'Failed'
ORDER BY start_time DESC;

-- Kiểm tra table metadata
SELECT 
    TABLE_SCHEMA, 
    TABLE_NAME, 
    TABLE_TYPE
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'gold';

-- Kiểm tra column data types
SELECT 
    COLUMN_NAME, 
    DATA_TYPE, 
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'fact_sales';
```

---

## 7. Identify and Resolve OneLake Shortcut Errors

### 7.1. Lỗi phổ biến OneLake Shortcuts

| Lỗi | Nguyên nhân | Giải pháp |
|-----|------------|-----------|
| **Shortcut not found** | Target path không tồn tại | Kiểm tra source container/folder path |
| **Access denied** | Authentication failure | Kiểm tra credentials, Service Principal, Workspace Identity |
| **Metadata sync failure** | Delta table metadata corrupted/changed | Force "Refresh metadata" trong Lakehouse |
| **Data not visible** | Shortcut target dữ liệu trống | Verify data exists ở source |
| **Schema not detected** | Files trong Files/ folder (không phải Tables/) | Chuyển shortcut vào Tables/ folder cho auto-schema detection |
| **Cross-tenant access denied** | Tenant policies block cross-tenant shortcuts | Configure cross-tenant access policies |
| **Egress cost spike** | Frequent queries trên external shortcuts | Enable shortcut caching (1–28 ngày) |

### 7.2. Troubleshooting Shortcut Authentication

```
Shortcut Authentication Flow:
│
├── Internal OneLake Shortcut
│   └── Dùng workspace permissions (tự động)
│
├── ADLS Gen2 Shortcut
│   ├── Pass-through: User's Entra ID token
│   ├── Service Principal: App registration + secret/certificate
│   └── Workspace Identity: Trusted Workspace Access (behind firewall)
│
├── S3 Shortcut
│   └── IAM Role/Access Key authentication
│
└── GCS Shortcut
    └── Service Account authentication

Common Fix: Tạo lại connection → test → verify permissions
```

### 7.3. Shortcut Caching để giảm errors và cost

```
Shortcut Caching Configuration:
├── Location: OneLake Settings → Shortcut Caching
├── Retention: 1 – 28 ngày
├── Benefit: 
│   ├── Giảm egress cost cho external shortcuts
│   ├── Giảm latency (cached data gần hơn)
│   └── Giảm timeout errors (không phụ thuộc network)
└── Lưu ý: Cache tốn OneLake storage
```

---

## 8. Câu hỏi mô phỏng bài thi (Practice Questions)

### Question 1

**A data pipeline in Microsoft Fabric fails with the error "UserErrorFailedToConnectToSource" when trying to copy data from an on-premises SQL Server database.**

**Which THREE troubleshooting steps should you take? (Select THREE)**

A. Verify that the on-premises data gateway is running  
B. Check if the SQL Server port (1433) is open on the firewall  
C. Increase the Fabric capacity size  
D. Verify the connection credentials have not expired  
E. Convert the pipeline to use Dataflow Gen2 instead

**✅ Correct Answers: A, B, D**

> **Giải thích**:
> - **(A) Đúng** — On-premises data gateway là bridge giữa Fabric và on-prem SQL Server. Nếu gateway offline → connection fails.
> - **(B) Đúng** — Firewall blocking port 1433 là nguyên nhân phổ biến nhất cho connection errors.
> - **(D) Đúng** — Expired credentials (password changed, token expired) gây authentication failure.
> - **(C) Sai** — Capacity size không liên quan tới connection errors.
> - **(E) Sai** — Chuyển sang Dataflow Gen2 không giải quyết vấn đề kết nối.

---

### Question 2

**A Spark notebook in Fabric runs successfully when executed interactively but fails when triggered by a pipeline with the error: "ModuleNotFoundError: No module named 'pandas_profiling'".**

**What is the correct fix?**

A. Add `%pip install pandas-profiling` at the beginning of the notebook  
B. Create a Fabric Environment with `pandas-profiling` installed and attach it to the notebook  
C. Install the library directly on the Spark cluster  
D. Upgrade the Python version to 3.12

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — Khi notebook chạy trong pipeline (orchestrated), `%pip install` bị **restricted**. Cách đúng là tạo Fabric Environment với library đã pre-installed, rồi attach vào notebook.
> - **(A) Sai** — `%pip install` bị blocked trong pipeline execution. Chỉ works interactively.
> - **(C) Sai** — Fabric managed Spark clusters không cho phép direct installation.
> - **(D) Sai** — Python version không liên quan tới library installation method.

---

### Question 3

**Your Eventstream stops ingesting data after a Fabric capacity downscale event. The Eventhouse that was the destination appears to be in a "Paused" state.**

**What should you do to prevent this from happening in the future?**

A. Create a new Eventstream every time capacity changes  
B. Enable the "Always-On" setting on the Eventhouse or set a Minimum Consumption Unit  
C. Switch from Eventstream to Spark Structured Streaming  
D. Disable capacity autoscale

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — "Always-On" hoặc Minimum CU ngăn Eventhouse tự động pause khi capacity downscale. Eventhouse chạy liên tục → không bị ngắt Eventstream ingestion.
> - **(A) Sai** — Tạo mới Eventstream không giải quyết root cause (Eventhouse pausing).
> - **(C) Sai** — Spark Structured Streaming giải quyết khác nhưng không tối ưu nếu đã dùng Eventstream.
> - **(D) Sai** — Disable autoscale có thể tốn cost và không giải quyết pause issue.

---

### Question 4

**A T-SQL query in a Fabric Warehouse fails with the error: "This feature is not supported in the current version of Fabric Warehouse."**

**The query attempts to create a clustered index on a table. What should you do?**

A. Create a non-clustered index instead  
B. Use Liquid Clustering on the table instead of traditional indexes  
C. Move the table to an Azure SQL Database  
D. Contact Microsoft support to enable the feature

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — Fabric Warehouse KHÔNG hỗ trợ CREATE INDEX (cả clustered và non-clustered). Thay vào đó, dùng **Liquid Clustering** hoặc **V-Order** — là cơ chế optimization tự động của Fabric.
> - **(A) Sai** — Non-clustered indexes cũng không được hỗ trợ trong Fabric Warehouse.
> - **(C) Sai** — Chuyển sang Azure SQL Database mất lợi ích của Fabric ecosystem.
> - **(D) Sai** — Đây là by-design limitation, không phải bug.

---

### Question 5

**A Dataflow Gen2 refresh fails after a team member who originally created the dataflow left the organization 4 months ago. The error message mentions "insufficient permissions for staging artifacts."**

**What is the recommended fix?**

A. Delete the Dataflow Gen2 and recreate it  
B. Have a current workspace member take ownership of the Dataflow Gen2  
C. Re-add the departed user's account to the workspace  
D. Change the Dataflow destination to a different Lakehouse

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — Khi creator rời tổ chức, credentials hết hiệu lực. Solution chuẩn là **Take Ownership** — một user hiện tại nhận quyền sở hữu Dataflow, cập nhật credentials.
> - **(A) Sai** — Recreate mất toàn bộ transformation logic, không cần thiết.
> - **(C) Sai** — Re-add departed user thường không khả thi và vi phạm security policy.
> - **(D) Sai** — Lỗi liên quan staging artifacts, không phải destination.

---

### Question 6

**An OneLake shortcut pointing to an Azure Data Lake Storage Gen2 account occasionally returns "access denied" errors during query execution. The same queries work fine when run by the shortcut creator.**

**What is the most likely cause?**

A. The ADLS Gen2 account is in a different Azure region  
B. The shortcut uses pass-through authentication and the querying user lacks ADLS Gen2 permissions  
C. The OneLake shortcut cache has expired  
D. The Fabric capacity is too small

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — Khi shortcut dùng **pass-through authentication**, mỗi user cần có quyền trực tiếp trên ADLS Gen2. Creator có quyền nên query thành công. Users khác thiếu quyền → access denied.
> - **(A) Sai** — Different region gây latency, không gây access denied.
> - **(C) Sai** — Cache expired không gây access denied, chỉ gây slower queries.
> - **(D) Sai** — Capacity size không liên quan tới authentication errors.

---

### Question 7

**You are debugging a Spark notebook that produces an OutOfMemoryError. The Spark UI shows high "Disk Spill" values across most executors.**

**Which TWO actions are most appropriate? (Select TWO)**

A. Increase `spark.executor.memory`  
B. Decrease `spark.sql.shuffle.partitions`  
C. Repartition the DataFrame to distribute data more evenly  
D. Add more filter conditions to reduce data volume before processing  
E. Switch to a Dataflow Gen2

**✅ Correct Answers: A and C (or A and D)**

> **Giải thích**:
> - **(A) Đúng** — High disk spill = data tràn từ RAM xuống disk. Tăng executor memory giảm spill.
> - **(C) Đúng** — Repartition phân bổ data đều hơn giữa executors, giảm áp lực memory trên individual executor.
> - **(D) Cũng đúng** — Filter sớm giảm data volume, giảm memory pressure tổng thể.
> - **(B) Sai** — Giảm shuffle partitions TĂNG kích thước mỗi partition → MORE spill, không giảm.
> - **(E) Sai** — Dataflow Gen2 không giải quyết OOM cho data lớn — nó có giới hạn memory riêng.

---

### Question 8

**A KQL query in an Eventhouse that usually returns results in under 1 second suddenly takes over 30 seconds. No changes were made to the query or the data volume.**

**What should you investigate FIRST?**

A. Check if the data has aged out of the hot cache and is now being read from cold storage  
B. Rewrite the KQL query to use a different syntax  
C. Delete and recreate the Eventhouse  
D. Increase the Eventstream throughput

**✅ Correct Answer: A**

> **Giải thích**:
> - **(A) Đúng** — Eventhouse sử dụng tiered storage (Hot SSD → Cold Blob). Nếu caching policy quá ngắn, data "ages out" khỏi hot cache → query phải đọc từ cold storage (chậm hơn nhiều). Kiểm tra `.show table <name> policy caching`.
> - **(B) Sai** — Query không thay đổi, nên syntax không phải vấn đề.
> - **(C) Sai** — Recreate Eventhouse là quá mức.
> - **(D) Sai** — Eventstream throughput ảnh hưởng ingestion, không ảnh hưởng query speed.

---

### Question 9

**You are troubleshooting a Fabric pipeline that intermittently fails with "GatewayUnreachable" errors. The failures happen approximately every 7 days.**

**What is the most likely cause?**

A. The source database is being restarted weekly  
B. The on-premises data gateway auto-update requires a restart  
C. The Fabric capacity license is expiring  
D. The pipeline timeout is set to 7 days

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — On-premises data gateway có auto-update cycle. Khi gateway update, nó cần restart → briefly unavailable → pipeline gặp "GatewayUnreachable". Chu kỳ ~7 ngày khớp với update schedule.
> - **(A) Sai** — Database restart thường không theo chu kỳ 7 ngày chính xác.
> - **(C) Sai** — License expiring gây lỗi khác, không phải gateway unreachable.
> - **(D) Sai** — Pipeline timeout gây timeout error, không phải gateway unreachable.

---

### Question 10

**In a Fabric Warehouse, you execute a MERGE INTO statement and receive the error: "MERGE is not supported when the target table has row-level security (RLS) enabled."**

**What should you do?**

A. Disable RLS, run the MERGE, then re-enable RLS  
B. Use INSERT and UPDATE statements separately instead of MERGE  
C. Convert the Warehouse table to a Lakehouse table  
D. Remove all security from the workspace

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — Khi MERGE không compatible với RLS, thay thế bằng logic INSERT + UPDATE riêng biệt. Đây là workaround phổ biến.
> - **(A) Sai** — Disable RLS tạo security gap, có thể vi phạm compliance.
> - **(C) Sai** — Chuyển sang Lakehouse không giải quyết RLS constraint.
> - **(D) Sai** — Remove all security là vi phạm nghiêm trọng.

---

*Tài liệu được tổng hợp dựa trên Skills Measured as of July 21, 2026*  
*Nguồn tham khảo: Microsoft Learn, Microsoft Fabric Documentation*


---

## 9. BỔ SUNG: TROUBLESHOOTING PLAYBOOK

### 9.1 Thứ tự ưu tiên

1. Xác định phạm vi: một run, một item, workspace hay toàn capacity.
2. Lấy timestamp, correlation/run/job ID và error code chính xác.
3. Kiểm tra dependency: credentials, gateway, source/sink, permissions, schema.
4. Phân biệt transient với deterministic error trước khi tăng retry.
5. So sánh Capacity Metrics và Spark/SQL/KQL operation logs tại cùng thời điểm.
6. Chỉ thay đổi một yếu tố mỗi lần và rerun bằng dữ liệu nhỏ có kiểm soát.

### 9.2 PySpark kiểm tra skew nhanh

```python
from pyspark.sql.functions import count, desc

(
    df.groupBy("CustomerId")
      .agg(count("*").alias("rows"))
      .orderBy(desc("rows"))
      .show(20, truncate=False)
)
```

### 9.3 Câu hỏi tình huống bổ sung

**Pipeline lỗi `AuthenticationError` mọi lần chạy. Retry 5 lần không đổi. Hành động đầu tiên?**

A. Tăng retry lên 20  
B. Xác minh credentials/connection identity và quyền source/sink  
C. Tăng Spark partitions  
D. Bật V-Order

<details><summary>Đáp án</summary>

**B.** Đây là deterministic configuration/identity error; retry chỉ phù hợp transient failures.
</details>


---

## NGUỒN CHÍNH THỨC ĐÃ ĐỐI CHIẾU

- [DP-700 study guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)
- [Take ownership of Fabric items](https://learn.microsoft.com/en-us/fabric/fundamentals/item-ownership-take-over)
- [Monitor Copy activity](https://learn.microsoft.com/en-us/fabric/data-factory/monitor-copy-activity)

---

## 10. CÂU HỎI SCENARIO — IDENTIFY & RESOLVE ERRORS

> Các câu hỏi dưới đây là câu tự soạn theo blueprint DP-700, **không phải câu hỏi thật hoặc exam dump**.

### Câu D3-E1 — Authentication Error Liên Tục

**Pipeline bị lỗi `AuthenticationError` trong mỗi lần chạy. Đã retry 5 lần nhưng kết quả giống nhau. Hành động **đầu tiên** cần làm là gì?**

A. Tăng retry lên 20 lần  
B. Kiểm tra và xác minh credentials/connection identity, quyền trên source và sink  
C. Tăng số Spark partitions  
D. Bật V-Order  

<details><summary>Đáp án và giải thích</summary>

**B.** `AuthenticationError` xảy ra **nhất quán** qua mọi lần chạy là **deterministic error** — không phải transient (tạm thời). Retry chỉ hữu ích cho transient errors (network timeout, throttling). Hành động đúng: kiểm tra [credentials/connection](https://learn.microsoft.com/en-us/fabric/data-factory/connector-troubleshoot-guide), identity (managed identity, service principal), và quyền trên source/sink.

</details>

---

### Câu D3-E2 — Data Skew Trong Spark

**Spark job có 200 tasks. Đa số tasks hoàn thành trong 3 giây, nhưng 5 tasks mất hơn 8 phút. Nguyên nhân **hợp lý nhất** là gì?**

A. Data skew — một số partition keys có quá nhiều dữ liệu hơn các keys khác  
B. Git conflict trong notebook  
C. Sensitivity label cấu hình sai  
D. Airflow pool bị paused  

<details><summary>Đáp án và giải thích</summary>

**A.** **Straggler tasks** (tasks chậm hơn rất nhiều so với phần còn lại) là triệu chứng điển hình của [data skew](https://learn.microsoft.com/en-us/fabric/data-engineering/spark-job-definition) — dữ liệu phân phối không đều, một số partitions chứa nhiều dữ liệu hơn → những tasks đó mất thêm thời gian. Giải pháp: key salting, AQE skew join, repartition.

</details>

---

### Câu D3-E3 — Dataflow Owner Rời Tổ Chức

**Dataflow Gen2 bắt đầu báo lỗi staging permission sau khi owner rời công ty và tài khoản bị vô hiệu hóa. Fix nào phù hợp nhất?**

A. Take ownership Dataflow và refresh credentials/connections với identity mới  
B. Scale capacity lên tier cao hơn  
C. Chạy VACUUM trên Lakehouse  
D. Đổi KQL hot cache policy  

<details><summary>Đáp án và giải thích</summary>

**A.** Khi owner rời tổ chức, credentials và connections gắn với tài khoản cũ sẽ fail. Giải pháp: [Take ownership](https://learn.microsoft.com/en-us/fabric/fundamentals/item-ownership-take-over) Dataflow với account mới → refresh/cập nhật credentials và connections. Đây là root cause phổ biến của Dataflow permission errors trong môi trường production.

</details>

---

### Câu D3-E4 — GatewayUnreachable Định Kỳ

**Pipeline báo lỗi `GatewayUnreachable` một cách định kỳ (ví dụ: mỗi tháng một lần). Hành động đầu tiên để điều tra nguyên nhân là gì?**

A. Kết luận ngay đây là do gateway auto-update định kỳ  
B. Đối chiếu gateway logs, service logs và network logs theo timestamp để tìm correlation  
C. DROP TABLE liên quan và tạo lại  
D. Tắt security để test  

<details><summary>Đáp án và giải thích</summary>

**B.** Không nên kết luận nguyên nhân từ chu kỳ quan sát mà không có evidence. Quy trình đúng:
1. Ghi lại **timestamp chính xác** của mỗi lần lỗi
2. Đối chiếu với [gateway logs](https://learn.microsoft.com/en-us/data-integration/gateway/service-gateway-tshoot), service health dashboard, và network infrastructure logs
3. Tìm pattern correlation — chỉ kết luận sau khi có evidence

Đây là nguyên tắc **systematic troubleshooting** — không đoán mò.

</details>

