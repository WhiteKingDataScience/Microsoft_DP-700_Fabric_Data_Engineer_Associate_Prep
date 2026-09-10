# DP-700: Monitor Fabric Items & Configure Alerts
## Monitor and Optimize an Analytics Solution (30–35%) — Phần 1/3


> [!IMPORTANT]
> **Bản hiệu chỉnh ngày 09/08/2026.** Nội dung được đối chiếu với blueprint DP-700 có hiệu lực từ **21/07/2026**. Microsoft cho biết phần lớn câu hỏi tập trung vào tính năng GA, nhưng tính năng Preview phổ biến vẫn có thể xuất hiện. Với các bảng “supported items”, runtime, limits và trạng thái Preview/GA, hãy kiểm tra Microsoft Learn lần cuối trước ngày thi.

> **Skills measured as of July 21, 2026**  
> Tài liệu chuẩn bị thi chứng chỉ **Microsoft Fabric Data Engineer Associate (DP-700)**  
> Tham chiếu: [Microsoft Learn — Study Guide DP-700](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)

> [!NOTE]
> ### Cách dùng tài liệu này
> Blueprint DP-700 hiện hành có thể hỏi **related topics** ngoài đúng từng bullet, vì vậy mục tiêu của tài liệu không phải học thuộc màn hình UI mà là hình thành phản xạ:
>
> `job status/error → Monitoring Hub` · `CU/throttling → Capacity Metrics` · `workspace logs/KQL → Workspace Monitoring` · `Spark stage → Spark UI` · `Warehouse query history → Query Insights`.
>
> Microsoft Learn: [Monitoring Hub](https://learn.microsoft.com/en-us/fabric/admin/monitoring-hub) · [Capacity Metrics](https://learn.microsoft.com/en-us/fabric/enterprise/metrics-app) · [Workspace Monitoring](https://learn.microsoft.com/en-us/fabric/data-factory/workspace-monitoring)


---

## MỤC LỤC

1. [Tổng quan công cụ Monitoring trong Fabric](#1-tổng-quan-công-cụ-monitoring-trong-fabric)
2. [Monitor Data Ingestion](#2-monitor-data-ingestion)
3. [Monitor Data Transformation](#3-monitor-data-transformation)
4. [Monitor Semantic Model Refresh](#4-monitor-semantic-model-refresh)
5. [Configure Alerts](#5-configure-alerts)
6. [Câu hỏi mô phỏng bài thi (Practice Questions)](#6-câu-hỏi-mô-phỏng-bài-thi-practice-questions)

---

## 1. Tổng quan công cụ Monitoring trong Fabric

### 1.1. Hai trụ cột Monitoring

Microsoft Fabric cung cấp hai công cụ giám sát chính:

| Công cụ | Mục đích | Người dùng |
|---------|----------|-----------|
| **Monitoring Hub** | Giám sát **tác vụ (operations)** — trạng thái, lỗi, lịch sử chạy | Data Engineers, Developers |
| **Capacity Metrics App** | Giám sát **tài nguyên (resources)** — CU usage, throttling, cost | Capacity Admins |


### 1.2. Monitoring Hub — Chi tiết

[Monitoring Hub](https://learn.microsoft.com/en-us/fabric/admin/monitoring-hub) là nơi xem **execution health, progress và outcome** của các Fabric jobs mà bạn có quyền xem.

**Bạn cần nhớ cho kỳ thi:**
- Xem trạng thái `Running / Succeeded / Failed / Cancelled`.
- Xem historical runs để so sánh, điều tra lỗi và xác nhận rerun; Microsoft hiện nêu khả năng điều tra các failure trong **30 ngày gần nhất**.
- Filter theo status, item type, start time, submitter/location.
- Với pipeline có **hierarchical view** để theo dõi upstream/downstream jobs.
- Có thể retry toàn pipeline; trong run detail có thể rerun từ failed activity hoặc selected activity.

Xem thêm: [Monitor pipeline runs in Monitoring Hub](https://learn.microsoft.com/en-us/fabric/data-factory/monitoring-hub-pipeline-runs).

> 📌 **Điểm thi:** Monitoring Hub trả lời câu hỏi **“job nào chạy/fail và fail ở đâu?”**. Nó không phải bằng chứng trực tiếp rằng capacity đang thiếu CU.


### 1.3. Capacity Metrics App — Chi tiết

[Microsoft Fabric Capacity Metrics app](https://learn.microsoft.com/en-us/fabric/enterprise/metrics-app) dùng để phân tích **resource consumption** và capacity health.

Các view quan trọng:

| View | Dùng để làm gì? |
|---|---|
| **Health** | Nhìn throttling/rejection và capacity health |
| **Compute** | CU consumption, operations, trends; lịch sử thường dùng 14 ngày |
| **Timepoint / Timepoint Summary** | Drill vào một timepoint khoảng 30 giây để tìm operation tiêu thụ resource |

#### Smoothing và throttling — bẫy thi quan trọng

**Không học:** `CU > 100% = reject ngay`.

Fabric áp dụng smoothing/overage protection. Khi overload kéo dài, mức ảnh hưởng có thể tiến triển từ overage protection → interactive delay → interactive rejection → background rejection. Microsoft hiện mô tả các khoảng điển hình khoảng **10 phút, 60 phút và 24 giờ**, nhưng hãy dùng documentation hiện hành làm source of truth.

Tham khảo: [Capacity Metrics Compute page](https://learn.microsoft.com/en-us/fabric/enterprise/metrics-app-compute-page).

**Decision pattern:**
- Nhiều workload cùng chậm/timeout → **Capacity Metrics**.
- Một Spark stage có vài straggler tasks → **Spark UI**, chưa vội scale capacity.
- Peak 120–130% nhưng chưa reject → xem smoothing/throttling timeline, không kết luận Metrics sai.

> 📌 **Điểm thi:** Monitoring Hub = **operational status**. Capacity Metrics = **resource health/CU/throttling**.

## 2. Monitor Data Ingestion

### 2.1. Pipeline Monitoring

**Cách giám sát pipeline ingestion**:

1. **Monitoring Hub**: Lọc Type = "Pipeline" → xem status tất cả pipeline runs
2. **Pipeline Run Details**: Click vào pipeline run → xem chi tiết từng activity
3. **Activity Run Details**: Click vào activity → xem:
   - Input/Output data
   - Error message (nếu failed)
   - Duration
   - Data read/written (rows, bytes)
   - Copy throughput (MB/s)

**Metrics quan trọng khi monitor Copy Activity**:

| Metric | Ý nghĩa |
|--------|---------|
| **Data Read** | Số bytes/rows đọc từ source |
| **Data Written** | Số bytes/rows ghi vào destination |
| **Duration** | Thời gian copy |
| **Throughput** | Tốc độ truyền (MB/s) |
| **Used Data Integration Units** | Số DIU sử dụng |

### 2.2. Copy Job Monitoring

Copy Job (ingestion liên tục) có dashboard riêng:
- **Sync Status**: Running, Paused, Error
- **Tables Synced**: Số bảng đã sync thành công
- **Rows Replicated**: Tổng số rows đã replicate
- **Latency**: Độ trễ giữa source và destination

### 2.3. Mirroring Monitoring

Mirrored databases có trang monitoring riêng:
- **Replication Status**: Active, Paused, Error
- **Tables Status**: Per-table sync status
- **Last Sync Time**: Thời điểm sync gần nhất
- **Lag**: Độ trễ replication


### 2.4. Eventstream Monitoring

Khi [Workspace Monitoring](https://learn.microsoft.com/en-us/fabric/data-factory/workspace-monitoring) được bật, Eventstream có thể ghi metrics/error information vào monitoring Eventhouse.

Các bảng quan trọng có thể gặp:
- `EventStreamNodeStatus`
- `EventStreamMetrics`
- `EventStreamErrorMetrics`

Mental model:

```text
Không có data / latency tăng
├─ Source có phát event không?
├─ Processor/transform node có xử lý không?
└─ Destination có nhận/commit được không?
```

Ví dụ KQL:

```kusto
EventStreamErrorMetrics
| where Timestamp > ago(1h)
| order by Timestamp desc
```

```kusto
EventStreamMetrics
| where Timestamp > ago(30m)
| summarize AvgValue = avg(MetricValue) by ArtifactId, MetricName
| order by AvgValue desc
```

> ⚠️ **Preview caveat:** Workspace monitoring cho Eventstream còn Preview; eventstream cũ có thể cần republish và một số bảng/metrics có độ trễ hoặc chưa có detailed diagnostic logs. Không học một “topology limit” cứng nếu Microsoft Learn hiện tại không xác nhận.

## 3. Monitor Data Transformation

### 3.1. Notebook / Spark Job Monitoring

**Trong Monitoring Hub**:
- Lọc Type = "Notebook" hoặc "Spark Job Definition"
- Xem status, duration, submitted by

**Spark UI** (công cụ chi tiết nhất):

| Tab | Thông tin |
|-----|----------|
| **Jobs** | Tổng quan các Spark jobs trong session |
| **Stages** | Chi tiết từng stage — tasks, shuffle, duration |
| **Storage** | Cached RDDs/DataFrames |
| **Environment** | Spark configuration hiện tại |
| **Executors** | Trạng thái executors — memory, cores, disk spill |
| **SQL/DataFrame** | DAG execution plan cho SQL queries |

**Metrics quan trọng trong Spark UI**:

| Metric | Ý nghĩa | Hành động khi bất thường |
|--------|---------|-------------------------|
| **Shuffle Read/Write** | Lượng data shuffle giữa executors | Quá lớn → optimize joins, repartition |
| **Disk Spill** | Data tràn từ RAM xuống disk | > 0 → tăng executor memory |
| **Task Duration** | Thời gian mỗi task | Chênh lệch lớn → data skew |
| **GC Time** | Thời gian Garbage Collection | Cao → tăng memory hoặc giảm partition size |
| **Failed Tasks** | Số task thất bại | > 0 → kiểm tra logs |

**Run Series Analysis (2026)**:
- So sánh run hiện tại với "known-good" baseline
- Phát hiện regression sau code/config changes
- Truy cập từ Spark Job Detail page

### 3.2. Dataflow Gen2 Monitoring

**Trong Monitoring Hub**:
- Lọc Type = "Dataflow Gen2"
- Xem refresh history, status, duration

**Refresh Details**:
- **Per-query status**: Mỗi query trong Dataflow có status riêng
- **Error messages**: Chi tiết lỗi cho từng query
- **Rows processed**: Số rows xử lý
- **Duration per query**: Thời gian xử lý từng query


### 3.3. Warehouse Query Monitoring

Dùng [Query Insights](https://learn.microsoft.com/en-us/fabric/data-warehouse/query-insights) để xem historical query execution trong Fabric Warehouse.

Các view trọng yếu:
- `queryinsights.exec_requests_history`
- `queryinsights.exec_sessions_history`
- `queryinsights.long_running_queries`
- `queryinsights.frequently_run_queries`

Ví dụ tìm query scan nhiều dữ liệu từ OneLake:

```sql
SELECT TOP 20
    distributed_statement_id,
    query_hash,
    status,
    start_time,
    end_time,
    allocated_cpu_time_ms,
    data_scanned_remote_storage_mb,
    data_scanned_memory_mb,
    data_scanned_disk_mb,
    command
FROM queryinsights.exec_requests_history
ORDER BY data_scanned_remote_storage_mb DESC;
```

Ví dụ tìm query tốn CPU:

```sql
SELECT TOP 20
    query_hash,
    allocated_cpu_time_ms,
    data_scanned_remote_storage_mb,
    command
FROM queryinsights.exec_requests_history
ORDER BY allocated_cpu_time_ms DESC;
```

**Cold-cache reasoning:** `data_scanned_remote_storage_mb > 0` nghĩa execution phải fetch data từ OneLake. Không benchmark Warehouse chỉ dựa vào lần chạy đầu tiên.

Xem: [Warehouse performance guidelines](https://learn.microsoft.com/en-us/fabric/data-warehouse/guidelines-warehouse-performance).

> ⚠️ **Hiệu chỉnh:** Không dùng `result_cache_hit` như một cột bắt buộc của `queryinsights.exec_requests_history`; documentation hiện tại tập trung vào memory/disk/remote-storage scan metrics.

## 4. Monitor Semantic Model Refresh

### 4.1. Các loại Semantic Model trong Fabric

| Loại | Cơ chế Refresh | Monitoring |
|------|---------------|------------|
| **Import Mode** | Full/Incremental refresh — load data vào VertiPaq | Refresh History trong Settings |
| **Direct Lake** | Reframing — metadata update, không copy data | Tự động detect changes |
| **DirectQuery** | Không refresh — query trực tiếp nguồn | N/A |

### 4.2. Monitor Import Mode Refresh

**Cách xem Refresh History**:
1. Mở semantic model trong workspace
2. Click **"..."** → **Refresh history**
3. Xem danh sách refresh runs: Status, Start/End time, Duration

**Cách trigger refresh từ Pipeline**:
```
Pipeline:
├── ... (data loading activities)
└── Semantic Model Refresh Activity
    ├── Semantic Model: SalesModel
    ├── Wait for completion: Yes
    └── On failure → Notification
```


### 4.3. Monitor Direct Lake Refresh (Framing)

Tài liệu chính: [How Direct Lake works](https://learn.microsoft.com/en-us/fabric/fundamentals/direct-lake-how-it-works).

**Khái niệm phải phân biệt:**
- **Automatic updates** của Direct Lake semantic model mặc định được bật.
- Refresh có thể **frame** model tới trạng thái metadata/data mới.
- **Direct Lake on OneLake** chạy exclusively ở `DirectLakeOnly` và **không hỗ trợ DirectQuery fallback**.
- **Direct Lake on SQL endpoints** mới có khả năng fallback sang DirectQuery.

#### DirectLakeBehavior — chỉ cho Direct Lake on SQL endpoints

| Giá trị | Hành vi |
|---|---|
| `Automatic` | Default; có thể fallback DirectQuery |
| `DirectLakeOnly` | Nếu không đáp ứng Direct Lake thì query fail — hữu ích khi development |
| `DirectQueryOnly` | Ép DirectQuery để test |

Các nguyên nhân fallback phổ biến:
- RLS/DDM/OLS được định nghĩa ở SQL endpoint;
- unmaterialized SQL views;
- Delta table vượt guardrails;
- table chưa được frame sau thay đổi;
- capacity memory pressure.

**DAX chẩn đoán:**

```dax
EVALUATE TABLETRAITS()
```

Xem cột `DirectLakeFallbackInfo`.

> 📌 **Exam trap:** “Fallback → chọn DirectLakeOnly” chỉ hợp lý khi model là **Direct Lake on SQL endpoint**. Direct Lake on OneLake vốn không fallback.


## 5. Configure Alerts

### 5.1. Decision table

| Requirement | Lựa chọn phù hợp |
|---|---|
| Email đơn giản khi **scheduled pipeline** fail | Built-in scheduled failure notification |
| Alert manual + scheduled Job events, Teams/email, custom condition | **Activator on Job events** |
| Alert toàn workspace dựa trên logs/KQL | **Workspace Monitoring + KQL Queryset + Activator** |
| Khi activity fail thì pipeline tự gửi thông báo/cleanup | Outlook/Teams activity + dependency path |
| Natural-language operations monitoring | Operations Agent (Preview), kiến thức bổ sung |

### 5.2. Built-in Failure Notifications

Với scheduled pipeline runs, có thể cấu hình email failure notification.

**Giới hạn cần nhớ:**
- đơn giản;
- chủ yếu scheduled runs;
- email;
- không thay thế custom condition/Teams alert cho mọi run.

Tham khảo: [Create alerts for pipeline runs](https://learn.microsoft.com/en-us/fabric/data-factory/create-alerts-for-pipeline-runs).

### 5.3. Fabric Activator trên Job Events

[Job events trong Real-Time hub](https://learn.microsoft.com/en-us/fabric/real-time-hub/explore-fabric-job-events) có action **Set alert** để tạo Activator rule trực tiếp.

```text
Fabric Job Events
      │
      ├─ Set alert → Activator → Email / Teams / Run Fabric activity
      │
      └─ Create Eventstream → khi cần route/transform/fan-out
```

> ⚠️ **Hiệu chỉnh quan trọng:** Muốn dùng Activator cho Job events **không bắt buộc** phải tạo Eventstream. Eventstream chỉ là một lựa chọn khi cần stream processing/routing.

### 5.4. Workspace-wide pipeline failure alert bằng KQL

Theo [Workspace Monitoring](https://learn.microsoft.com/en-us/fabric/data-factory/workspace-monitoring):

```kusto
ItemJobEventLogs
| extend SecondsAgo = datetime_diff('second', now(), ingestion_time())
| where JobType == "Pipeline" and JobStatus == "Failed"
| where SecondsAgo <= 540
| order by Timestamp desc
| project Timestamp, ItemName, WorkspaceName, JobStartTime, JobEndTime, JobStatus
```

Sau đó:
1. tạo **KQL Queryset**;
2. dùng KQL Queryset trigger trong **Activator**;
3. chọn action Email/Teams;
4. enable rule.

### 5.5. Workflow notification trong Pipeline

Nếu requirement là *“activity X fail thì ngay trong pipeline gửi Teams rồi cleanup”*:

```text
Copy Activity
   ├─ On Success → Next
   └─ On Failure → Teams/Outlook Activity → Cleanup
```

Đây là **orchestration error handling**, khác centralized monitoring.

> 📌 **Tip thi:**  
> `simple scheduled email` → built-in notification  
> `manual + scheduled / Teams / complex condition` → Activator  
> `workspace-wide KQL alert` → Workspace Monitoring + KQL + Activator


## 6. Câu hỏi mô phỏng bài thi (Practice Questions)

### Question 1 — Job status

Bạn cần xem pipeline nào fail trong 24 giờ qua và drill-down vào failed activity.

A. Capacity Metrics app  
B. Monitoring Hub  
C. Git Integration  
D. Domain settings  

**✅ Đáp án: B**

---

### Question 2 — Capacity overload

Nhiều notebooks và pipelines cùng chậm. Bạn cần chứng minh capacity có bị throttling.

A. Spark UI của một notebook  
B. Fabric Capacity Metrics app  
C. Deployment Pipeline  
D. SQL endpoint  

**✅ Đáp án: B**

---

### Question 3 — CU trên 100%

Capacity Metrics hiển thị 125% trong một timepoint nhưng chưa reject requests.

A. Metrics sai  
B. Trên 100% luôn reject ngay  
C. Smoothing/throttling phụ thuộc overload kéo dài; xem Health/Compute timeline  
D. Xóa Monitoring Hub  

**✅ Đáp án: C**

---

### Question 4 — Spark skew

Trong một Spark stage, 98 tasks chạy 5 giây, 2 tasks chạy 8 phút.

A. Data skew  
B. Direct Lake fallback  
C. Git conflict  
D. V-Order  

**✅ Đáp án: A**

---

### Question 5 — Teams alert cho manual + scheduled failures

A. Built-in scheduled email only  
B. Activator trên Fabric Job events  
C. VACUUM  
D. Refresh history  

**✅ Đáp án: B**

---

### Question 6 — Workspace-wide alert

Bạn muốn query failed pipelines trong toàn workspace bằng KQL rồi alert nếu có lỗi trong 10 phút gần nhất.

A. Workspace Monitoring → KQL Queryset → Activator  
B. Spark UI  
C. Dataflow Query Folding  
D. GitHub  

**✅ Đáp án: A**

---

### Question 7 — Direct Lake fallback

Semantic model dùng **Direct Lake on SQL endpoint** đang fallback. Trong development, bạn muốn fallback trở thành lỗi rõ ràng để tìm root cause.

A. Import mode  
B. `DirectLakeBehavior = DirectLakeOnly`  
C. Disable automatic updates  
D. Increase Dataflow memory  

**✅ Đáp án: B**

Sau đó có thể dùng:

```dax
EVALUATE TABLETRAITS()
```

---

### Question 8 — Direct Lake on OneLake

Câu nào đúng?

A. Có DirectQuery fallback mặc định  
B. Không hỗ trợ DirectQuery fallback  
C. Bắt buộc Import refresh  
D. Bắt buộc SQL endpoint  

**✅ Đáp án: B**

---

## 7. KQL/SQL Cheat Sheet

### Failed jobs

```kusto
ItemJobEventLogs
| where Timestamp > ago(24h)
| where JobStatus == "Failed"
| summarize Failures = count() by ItemKind, ItemName
| order by Failures desc
```

### Warehouse top remote scans

```sql
SELECT TOP 10
    query_hash,
    data_scanned_remote_storage_mb,
    data_scanned_memory_mb,
    data_scanned_disk_mb,
    allocated_cpu_time_ms,
    command
FROM queryinsights.exec_requests_history
ORDER BY data_scanned_remote_storage_mb DESC;
```

---

## 8. LAB THỰC HÀNH

### Lab A — Monitoring Hub vs Capacity Metrics
1. Chạy pipeline + notebook.
2. Xem status trong Monitoring Hub.
3. Mở run detail.
4. Mở Capacity Metrics tại cùng timepoint.
5. Giải thích: vấn đề thuộc operation hay capacity?

### Lab B — Workspace Monitoring + KQL
1. `Workspace Settings > Monitoring`.
2. Enable log workspace activity.
3. Mở monitoring Eventhouse.
4. Query `ItemJobEventLogs`.
5. Viết query failed jobs 24h.
6. Tạo KQL Queryset.

### Lab C — Alert
1. Real-Time hub → Job events.
2. `Set alert`.
3. Chọn Teams/email.
4. So sánh với built-in scheduled failure notification.

### Readiness gate

Không nhìn tài liệu, phải giải thích được:
- Monitoring Hub vs Capacity Metrics vs Workspace Monitoring.
- Spark UI vs Run Series Analysis.
- Query Insights và `data_scanned_remote_storage_mb`.
- Direct Lake on OneLake vs Direct Lake on SQL endpoint.
- Built-in notification vs Activator.
- Vì sao CU >100% chưa chắc reject ngay.

---

## 9. NGUỒN MICROSOFT LEARN TRỌNG YẾU

- [DP-700 Study Guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)
- [Monitoring Hub](https://learn.microsoft.com/en-us/fabric/admin/monitoring-hub)
- [Monitor pipeline runs](https://learn.microsoft.com/en-us/fabric/data-factory/monitoring-hub-pipeline-runs)
- [Capacity Metrics app](https://learn.microsoft.com/en-us/fabric/enterprise/metrics-app)
- [Workspace Monitoring](https://learn.microsoft.com/en-us/fabric/data-factory/workspace-monitoring)
- [Item job event logs](https://learn.microsoft.com/en-us/fabric/fundamentals/item-job-event-logs)
- [Job events](https://learn.microsoft.com/en-us/fabric/real-time-hub/explore-fabric-job-events)
- [Pipeline alerts](https://learn.microsoft.com/en-us/fabric/data-factory/create-alerts-for-pipeline-runs)
- [Query Insights](https://learn.microsoft.com/en-us/fabric/data-warehouse/query-insights)
- [Warehouse performance guidelines](https://learn.microsoft.com/en-us/fabric/data-warehouse/guidelines-warehouse-performance)
- [How Direct Lake works](https://learn.microsoft.com/en-us/fabric/fundamentals/direct-lake-how-it-works)

---

# 🎯 CHƯƠNG BỔ SUNG 06/09/2026 — HỌC LẠI TỪ GỐC

> **Vì sao có chương này:** Fabric có tới **năm** công cụ giám sát với tên gọi na ná nhau, và đề DP-700 hỏi rất nhiều dạng "để trả lời câu hỏi này thì mở công cụ nào". Chọn nhầm công cụ là mất điểm dù bạn hiểu bản chất vấn đề. Chương này viết lại theo hướng **một công cụ — một câu hỏi nó trả lời**.
> Xem thêm: [File tổng chỉ huy](DP700_00_MASTER_Trong_Tam_On_Thi.md)

## A. HIỂU BẢN CHẤT TRONG 60 GIÂY

Hãy tách bạch **hai câu hỏi hoàn toàn khác nhau**:

```text
  CÂU HỎI 1: "CÔNG VIỆC của tôi thế nào?"          CÂU HỎI 2: "TÀI NGUYÊN còn không?"
  (job chạy tới đâu, có lỗi không)                  (capacity có đang quá tải không)
            │                                                  │
            ▼                                                  ▼
     MONITORING HUB                                    CAPACITY METRICS APP
     ├── lọc theo item type, status, thời gian         ├── Compute: CU theo thời gian
     ├── mở run detail → activity nào fail             ├── Timepoint (~30 giây): ai ăn CU
     └── rerun / rerun từ activity lỗi                 └── Health: throttling, rejection

  Đi sâu hơn khi cần:
     SPARK UI            → bên trong MỘT Spark job (stage, task, skew, spill)
     QUERY INSIGHTS      → bên trong query T-SQL của Warehouse (lịch sử, query lâu)
     WORKSPACE MONITORING→ LOG CHI TIẾT của cả workspace, truy vấn được bằng KQL
```

**Một câu để nhớ:** Monitoring Hub cho biết **cái gì đã xảy ra**; Capacity Metrics cho biết **vì sao mọi thứ chậm**; Workspace Monitoring cho phép **tự truy vấn** log; Spark UI và Query Insights soi **bên trong một job/query**.

## B. BẢNG "CÂU HỎI → CÔNG CỤ" (dạng câu hỏi thi phổ biến nhất)

| Câu hỏi trong đề | Công cụ đúng | Vì sao các công cụ khác sai |
|---|---|---|
| "Pipeline tối qua chạy thành công không, fail ở activity nào?" | **Monitoring Hub** | Capacity Metrics không biết activity; Spark UI không thấy pipeline |
| "Vì sao mọi báo cáo và pipeline đều chậm từ 9h sáng?" | **Capacity Metrics** | Monitoring Hub chỉ cho status từng job, không chứng minh thiếu CU |
| "Ai/khoản nào tiêu thụ nhiều CU nhất lúc 9h15?" | **Capacity Metrics → Timepoint** | Không công cụ nào khác có dữ liệu CU theo timepoint |
| "Tôi muốn tự viết truy vấn trên log của mọi item trong workspace" | **Workspace Monitoring** (Eventhouse + KQL) | Monitoring Hub không cho query; Capacity Metrics là report cố định |
| "Stage nào trong notebook chạy lâu, có skew không?" | **Spark UI / Spark history server** | Không công cụ nào khác có task-level metrics |
| "Query T-SQL nào chạy lâu nhất tuần qua trong Warehouse?" | **Query Insights** | Spark UI không liên quan; Monitoring Hub không có query text |
| "Semantic model Import refresh lúc mấy giờ, có lỗi gì?" | **Refresh history** của semantic model | — |
| "Cảnh báo qua Teams khi pipeline fail" | **Failure notification** / activity Teams / **Activator** | Monitoring Hub chỉ để xem, không tự gửi cảnh báo |

## C. CAPACITY METRICS — ĐỌC ĐÚNG BA TRANG

| Trang | Trả lời | Chi tiết cần nhớ |
|---|---|---|
| **Compute** | CU tiêu thụ theo thời gian, phân loại **interactive vs background** | Lịch sử thường khoảng 14 ngày |
| **Timepoint** | Tại một mốc (~30 giây), operation nào tiêu thụ bao nhiêu | Đây là nơi tìm "thủ phạm" |
| **Health / throttling** | Capacity có bị delay/rejection không, khi nào | Đọc timeline throttling, không chỉ nhìn % |

**Ba cách đọc sai thường gặp:**

1. **"CU vượt 100% nghĩa là job bị chặn."** Sai — có overage protection và smoothing; vượt 100% có thể vẫn chạy bình thường.
2. **"Job này ngốn CU nên phải là thủ phạm."** Cần xem tại **timepoint** cụ thể, vì background đã được trải 24 giờ nên tác động phân bố khác với lúc job thực chạy.
3. **"Cứ chậm là scale-up."** Chỉ đúng khi throttling thật; nếu nguyên nhân là skew/small files thì scale không giải quyết.

## D. MONITORING HUB — NĂM THAO TÁC PHẢI BIẾT

```text
1. Lọc:      item type (pipeline/notebook/dataflow/semantic model/eventstream), status, thời gian, người chạy
2. Xem:      run history để so sánh lần chạy trước/sau
3. Mở sâu:   pipeline run → hierarchical view → activity nào fail, input/output của activity
4. Rerun:    chạy lại cả pipeline, hoặc rerun TỪ activity bị lỗi (tiết kiệm thời gian và CU)
5. Cancel:   huỷ job đang chạy
```

> 📌 Với activity Notebook hoặc Dataflow, thông báo lỗi hiển thị ở pipeline thường **rất chung chung**. Muốn biết nguyên nhân thật phải mở **run history của chính notebook/dataflow đó**.

## E. WORKSPACE MONITORING — BỐN ĐIỀU PHẢI NHỚ

1. Bật ở **workspace settings**; Fabric tạo một **Eventhouse/KQL database** trong workspace để chứa log.
2. Thu log và metric chi tiết của pipeline, notebook, warehouse, lakehouse, semantic model, eventstream — gồm cả **query text và duration**.
3. Truy vấn bằng **KQL** (hoặc T-SQL). Chỉ người có role đủ cao (từ Contributor trở lên) truy cập được.
4. **Không bật đồng thời** workspace monitoring và Log Analytics; và metric ở đây **không** được smoothing/bursting như Capacity Metrics — đừng dùng nó để kết luận về capacity.

## F. GIÁM SÁT SEMANTIC MODEL — IMPORT vs DIRECT LAKE

| | Import | Direct Lake |
|---|---|---|
| "Refresh" nghĩa là gì | Nạp dữ liệu vào model | **Framing** — cập nhật metadata trỏ tới file Delta mới nhất |
| Xem ở đâu | **Refresh history** của semantic model | Refresh history (framing) + trạng thái model |
| Chi phí | Tốn CU đáng kể, thời gian dài | Rất nhẹ |
| Lỗi hay gặp | Timeout, credential, vượt bộ nhớ | **Framing fail** khi bảng vượt guardrail; **fallback** sang DirectQuery |
| Chẩn đoán fallback | — | DAX `EVALUATE TABLETRAITS()` → cột `DirectLakeFallbackInfo` |

> 📌 Đề hỏi "báo cáo hiển thị dữ liệu cũ dù pipeline đã chạy xong" với model Direct Lake → nguyên nhân thường là **chưa framing lại**, không phải lỗi pipeline.

## G. ALERT — CHỌN CƠ CHẾ NÀO

| Yêu cầu trong đề | Cơ chế đúng |
|---|---|
| "Gửi email khi pipeline này fail" | **Failure notification** built-in của pipeline (ít công sức nhất) |
| "Gửi tin Teams có nội dung tuỳ biến khi fail" | Activity **Office 365 Outlook / Teams** đặt ở nhánh **Failure** |
| "Cảnh báo khi một điều kiện dữ liệu xảy ra (ví dụ nhiệt độ > 80)" | **Activator** trên Eventstream/Real-Time hub |
| "Cảnh báo khi bất kỳ pipeline nào trong workspace fail" | **Activator trên job events**, hoặc truy vấn KQL trên **Workspace Monitoring** |
| "Cảnh báo khi capacity sắp bị throttle" | Theo dõi **Capacity Metrics**; kết hợp cảnh báo ở tầng Azure Monitor nếu cần |

Nguyên tắc chọn: **ít cấu hình nhất mà đáp ứng đủ**. Đề hỏi "minimize administrative effort" → chọn built-in notification, không chọn dựng KQL alert.

## H. ACTIVE RECALL — 12 CÂU (che đáp án, nói thành tiếng)

<details><summary><b>1.</b> Monitoring Hub và Capacity Metrics — mỗi cái trả lời câu hỏi gì?</summary>

Monitoring Hub: **trạng thái vận hành** (job nào chạy/fail/fail ở đâu). Capacity Metrics: **sức khoẻ tài nguyên** (CU, throttling, timepoint).
</details>

<details><summary><b>2.</b> Timepoint trong Capacity Metrics dài bao lâu và dùng làm gì?</summary>

Khoảng **30 giây**; dùng để drill vào một mốc thời gian và tìm operation nào đang tiêu thụ tài nguyên.
</details>

<details><summary><b>3.</b> Vượt 100% CU có nghĩa là job bị từ chối không?</summary>

**Không.** Có smoothing và overage protection. Chỉ khi mức tiêu thụ tương lai vượt ngưỡng mới lần lượt xảy ra interactive delay → interactive rejection → background rejection.
</details>

<details><summary><b>4.</b> Workspace Monitoring lưu log ở đâu và truy vấn bằng gì?</summary>

Trong một **Eventhouse/KQL database** được tạo trong workspace; truy vấn bằng **KQL** (và T-SQL).
</details>

<details><summary><b>5.</b> Có bật đồng thời Workspace Monitoring và Log Analytics được không?</summary>

**Không** — chỉ chọn một trong hai.
</details>

<details><summary><b>6.</b> Pipeline báo lỗi chung chung ở activity Notebook — mở gì tiếp?</summary>

**Run history của chính notebook đó** để lấy stack trace thật; đồng thời xem tab **Input** của activity để kiểm tra parameter đã resolve đúng chưa.
</details>

<details><summary><b>7.</b> Rerun cả pipeline và rerun từ activity lỗi — nên chọn cái nào?</summary>

Rerun **từ activity lỗi** khi các bước trước đã thành công và idempotent — tiết kiệm thời gian và CU.
</details>

<details><summary><b>8.</b> "Refresh" của Direct Lake thực chất là gì?</summary>

**Framing**: cập nhật metadata để model trỏ tới tập file Delta mới nhất. Không nạp dữ liệu như Import.
</details>

<details><summary><b>9.</b> Báo cáo Direct Lake hiển thị dữ liệu cũ dù pipeline đã xong — nguyên nhân thường gặp?</summary>

Chưa **framing** lại. Model chỉ thấy dữ liệu tại thời điểm framing gần nhất.
</details>

<details><summary><b>10.</b> Muốn cảnh báo Teams khi pipeline fail với ít công sức nhất — chọn gì?</summary>

**Failure notification** built-in của pipeline. Chỉ dựng activity Teams/Activator khi cần nội dung tuỳ biến hoặc phạm vi rộng hơn.
</details>

<details><summary><b>11.</b> Muốn cảnh báo cho MỌI pipeline trong workspace — chọn gì?</summary>

**Activator trên job events**, hoặc truy vấn KQL định kỳ trên **Workspace Monitoring** — vì cấu hình từng pipeline một là không mở rộng được.
</details>

<details><summary><b>12.</b> Một Spark stage chậm bất thường — mở gì, nhìn số liệu nào?</summary>

**Spark UI → Stages → Summary Metrics for Tasks**: so **Max** với **Median** duration (phát hiện skew) và cột **Spill** (thiếu RAM).
</details>

## I. MINI-LAB 60 PHÚT

1. Chạy một pipeline có 3 activity, cố tình để activity thứ 2 fail. Vào **Monitoring Hub**, tìm run đó, mở hierarchical view, xác định activity lỗi, đọc tab Input/Output.
2. **Rerun từ activity lỗi**, xác nhận activity 1 không chạy lại.
3. Chạy một notebook Spark nặng. Mở **Spark UI**, ghi lại: số stage, stage lâu nhất, Max vs Median task duration, có spill không.
4. Mở **Capacity Metrics App**: tìm timepoint tiêu thụ cao nhất trong 14 ngày; xác định operation gây ra; phân loại interactive hay background.
5. Bật **Workspace Monitoring**; viết một truy vấn KQL liệt kê các job fail trong 24 giờ qua.
6. Tạo một **alert**: bật failure notification cho pipeline, rồi làm nó fail để nhận thông báo.
7. Tạo semantic model **Direct Lake** trên bảng Gold; nạp thêm dữ liệu bằng notebook; kiểm tra báo cáo **trước và sau khi framing** để tận mắt thấy sự khác biệt.

**Ngưỡng đạt:** che cột "Công cụ đúng" của bảng mục B và tự trả lời đúng ≥7/8 dòng; trả lời đúng ≥11/12 câu ở mục H.
