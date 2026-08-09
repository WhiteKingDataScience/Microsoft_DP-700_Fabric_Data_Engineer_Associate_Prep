# DP-700: Monitor Fabric Items & Configure Alerts
## Monitor and Optimize an Analytics Solution (30–35%) — Phần 1/3


> [!IMPORTANT]
> **Bản hiệu chỉnh ngày 04/08/2026.** Nội dung được đối chiếu với blueprint DP-700 có hiệu lực từ **21/07/2026**. Microsoft cho biết phần lớn câu hỏi tập trung vào tính năng GA, nhưng tính năng Preview phổ biến vẫn có thể xuất hiện. Với các bảng “supported items”, runtime, limits và trạng thái Preview/GA, hãy kiểm tra Microsoft Learn lần cuối trước ngày thi.

> **Skills measured as of July 21, 2026**  
> Tài liệu chuẩn bị thi chứng chỉ **Microsoft Fabric Data Engineer Associate (DP-700)**  
> Tham chiếu: [Microsoft Learn — Study Guide DP-700](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)

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

**Monitoring Hub** là trung tâm giám sát tập trung, truy cập từ navigation panel bên trái của Fabric portal.

**Khả năng**:
- Xem **real-time status** của mọi item: Pipeline, Notebook, Dataflow Gen2, Spark Job, Semantic Model refresh
- Lọc theo: **Item type**, **Status** (Succeeded/Failed/Running/Cancelled), **Time range**
- Drill-down vào chi tiết lỗi của từng activity trong pipeline
- Xem **run history** của từng item
- Liên kết trực tiếp tới item để debug

**Các cột thông tin quan trọng**:

| Cột | Mô tả |
|-----|--------|
| **Activity Name** | Tên item đã chạy |
| **Type** | Pipeline, Notebook, Dataflow, Spark Job... |
| **Status** | Succeeded ✅, Failed ❌, Running 🔄, Cancelled ⛔ |
| **Start Time** | Thời gian bắt đầu |
| **Duration** | Thời gian chạy |
| **Submitted By** | Người/Service Principal kích hoạt |
| **Workspace** | Workspace chứa item |

### 1.3. Capacity Metrics App — Chi tiết

**Microsoft Fabric Capacity Metrics App** cung cấp dashboard cho capacity administrators.

**Các trang chính**:

| Trang | Chức năng |
|-------|----------|
| **Health Page** | Tổng quan sức khỏe capacity — phát hiện throttling, query rejection |
| **Compute Page** | Chi tiết sử dụng CU trong 14 ngày — trends, peak hours |
| **Items Tab** | Xác định item nào tiêu tốn nhiều CU nhất |
| **Timepoint Explorer** | Phân tích chi tiết tại một thời điểm cụ thể |

**Metrics quan trọng**:

| Metric | Ý nghĩa | Ngưỡng cảnh báo |
|--------|---------|------------------|
| **CU Utilization %** | % sử dụng capacity tại timepoint | >100% cho thấy overload; throttling/delay/rejection phụ thuộc smoothing windows và autoscale |
| **Overages** | Lượng CU vượt quá capacity | Cần scale up |
| **Throttling Events** | Số lần bị giới hạn tốc độ | Nên = 0 |
| **Rejected Requests** | Requests bị từ chối do hết capacity | Nên = 0 |
| **Interactive vs Background** | Tỷ lệ workload interactive/background | Cân bằng |

> 📌 **Điểm thi**: Monitoring Hub → **operational monitoring** (job status, errors). Capacity Metrics App → **resource monitoring** (CU usage, throttling, cost optimization).

---

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

Trong Eventstream workspace monitoring (nếu enabled):
- **Message Count**: Số events đã xử lý
- **Throughput**: Events/second
- **Node Status**: Trạng thái từng node trong topology
- **Error Metrics**: Số events bị lỗi
- **Backlog**: Số events đang chờ xử lý

> ⚠️ **Lưu ý**: Eventstream workspace monitoring tiêu tốn capacity. Cần tính toán resource khi enable.

---

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

**queryinsights.exec_requests_history** — DMV (Dynamic Management View) trong Warehouse:

```sql
-- Xem lịch sử query execution
SELECT 
    command,
    status,
    start_time,
    end_time,
    DATEDIFF(second, start_time, end_time) AS duration_seconds,
    result_cache_hit,
    row_count
FROM queryinsights.exec_requests_history
ORDER BY start_time DESC;

-- Tìm queries chậm nhất
SELECT TOP 10
    command,
    DATEDIFF(second, start_time, end_time) AS duration_seconds,
    row_count
FROM queryinsights.exec_requests_history
WHERE status = 'Succeeded'
ORDER BY duration_seconds DESC;
```

---

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

### 4.3. Monitor Direct Lake Refresh (Reframing)

**Đặc điểm Direct Lake**:
- **Không cần schedule refresh thường xuyên** — tự động detect thay đổi trong Delta tables
- "Refresh" thực chất là **reframing** — cập nhật metadata trỏ tới files mới
- Chi phí rất thấp so với Import mode

**Setting quan trọng**:
- **"Keep your Direct Lake data up to date"** — Bật để tự động reframe khi Delta table thay đổi

**Troubleshoot Direct Lake**:

| Vấn đề | Nguyên nhân | Giải pháp |
|--------|------------|-----------|
| Dữ liệu cũ/stale | Setting auto-update bị tắt | Bật "Keep your Direct Lake data up to date" |
| Schema mismatch error | Cột trong Delta table bị đổi tên/xóa | Update semantic model, republish |
| Fallback to DirectQuery | Unsupported DAX/features | Chuyển sang DirectLakeOnly mode |
| Permission denied | Thiếu Read access trên Lakehouse | Gán Read permission cho user/SPN |
| Data chưa phản ánh | Chưa framing/schema sync hoặc automatic updates tắt | Refresh/reframe semantic model, kiểm tra refresh history và automatic updates; không dùng mốc chờ 20 phút như quy tắc |

> 📌 **Điểm thi**: Direct Lake refresh **KHÔNG** giống Import refresh. Nó là metadata operation rất nhẹ. Không cần schedule frequent refreshes — thường tự động sync.

---

## 5. Configure Alerts

### 5.1. Phương pháp alerting trong Fabric

| Phương pháp | Complexity | Use Case |
|------------|-----------|----------|
| **Built-in Failure Notifications** | Thấp | Email khi scheduled run fails |
| **Activator** | Trung bình | Alert phức tạp, Teams/email, Fabric job/workspace events |
| **Power Automate** | Trung bình | Custom workflow khi có sự kiện |
| **Custom (API + Logic App)** | Cao | Enterprise-grade monitoring |

### 5.2. Built-in Failure Notifications

**Cách cấu hình** (đơn giản nhất):
1. Mở item (Pipeline / Notebook / Dataflow)
2. Chọn **Schedule** tab
3. Tìm phần **"Failure notifications"**
4. Thêm email addresses
5. Save

**Giới hạn**:
- Chỉ hoạt động cho **scheduled runs** (không alert cho manual runs)
- Chỉ hỗ trợ **email** (không Teams, không webhook)
- Không có logic phức tạp (chỉ "on failure")

### 5.3. Fabric Activator

Data Activator cho phép tạo alerts phức tạp hơn dựa trên **Fabric Job Events**.

**Kiến trúc**:
```
Fabric Job Events ──► Real-Time hub / Eventstream ──► Activator
                                            │
                                      ┌─────┼─────┐
                                      │     │     │
                                    Email  Teams  Custom
                                    Alert  Alert  Action
```

**Bước cấu hình**:

1. **Tạo Eventstream** mới trong workspace
2. **Add Source**: Chọn "Fabric Job Events" → chọn workspace & item types (Pipeline, Notebook, etc.)
3. **Add Destination**: Chọn "Data Activator" (hoặc "New Reflex")
4. **Trong Data Activator**:
   - Tạo Object dựa trên event stream
   - Tạo **Rule**: `When status equals "Failed"`
   - Chọn **Action**: Email hoặc Teams message
5. **Start** trigger

**Ví dụ Rule trong Data Activator**:
```
Object: Pipeline_Job_Events
├── Property: Status (from event stream)
├── Condition: Status equals "Failed"
├── Action: Send email to admin@company.com
│   Subject: "Pipeline {ItemName} failed at {Timestamp}"
│   Body: "Error: {ErrorMessage}"
└── Start trigger
```

### 5.4. Power Automate Integration

Dùng Power Automate cho custom workflow:
- Trigger: "When a Fabric item fails"
- Action: Post message to Teams channel, create Jira ticket, send SMS...

### 5.5. So sánh các phương pháp Alert

| Tiêu chí | Built-in Notifications | Data Activator | Power Automate |
|----------|:----------------------|:---------------|:---------------|
| **Setup time** | 1 phút | 15 phút | 30+ phút |
| **Email alert** | ✅ | ✅ | ✅ |
| **Teams alert** | ❌ | ✅ | ✅ |
| **Manual run alert** | ❌ | ✅ | ✅ |
| **Custom logic** | ❌ | ✅ | ✅ |
| **No-code** | ✅ | ✅ | ✅ |
| **Cost** | Free | CU consumption | Per-flow license |

> 📌 **Tip thi**: 
> - Câu hỏi "simple email on scheduled failure" → **Built-in Failure Notifications**
> - Câu hỏi "Teams notification / complex conditions" → **Activator**

---

## 6. Câu hỏi mô phỏng bài thi (Practice Questions)

### Question 1

**You are a Fabric administrator. Users report that their Spark notebooks are running slower than usual, and some pipelines are failing with timeout errors. You need to determine whether the issue is caused by capacity resource exhaustion.**

**Which tool should you use?**

A. Monitoring Hub  
B. Microsoft Fabric Capacity Metrics App  
C. Spark UI  
D. Power BI Service Admin Portal

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — Capacity Metrics App hiển thị CU utilization, throttling events, và rejected requests. Nếu capacity bị exhaustion → throttling → notebooks chậm, pipelines timeout.
> - **(A) Sai** — Monitoring Hub cho thấy job status (succeeded/failed) nhưng KHÔNG cho thấy resource utilization hay throttling.
> - **(C) Sai** — Spark UI chỉ hiển thị metrics của một Spark session cụ thể, không phải tổng thể capacity.
> - **(D) Sai** — Admin Portal quản lý tenant settings, không phải capacity monitoring.

---

### Question 2

**You need to configure alerts so that your team receives a Microsoft Teams notification whenever a data pipeline fails, including both scheduled and manually triggered runs.**

**Which approach should you use?**

A. Configure the built-in failure notification in the pipeline's Schedule settings  
B. Create a Activator connected to Fabric Job Events via an Eventstream  
C. Use Power BI alerts on a dashboard tile  
D. Set up email forwarding rules in Outlook

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — Data Activator + Eventstream (Fabric Job Events) cho phép: (1) Teams notifications, (2) alert cho cả scheduled VÀ manual runs, (3) custom logic/conditions.
> - **(A) Sai** — Built-in failure notifications chỉ hỗ trợ email và chỉ cho scheduled runs, KHÔNG hỗ trợ Teams hay manual runs.
> - **(C) Sai** — Power BI alerts dành cho data-driven alerts trên dashboard tiles, không phải pipeline monitoring.
> - **(D) Sai** — Email forwarding không giải quyết vấn đề tại nguồn.

---

### Question 3

**Your organization uses Direct Lake mode for Power BI semantic models connected to a Fabric Lakehouse. Users report that some reports show stale data despite the underlying Delta tables being updated 2 hours ago.**

**What should you check FIRST?**

A. Schedule a frequent Import refresh for the semantic model  
B. Verify that the "Keep your Direct Lake data up to date" setting is enabled  
C. Recreate the semantic model from scratch  
D. Increase the Fabric capacity size

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — Direct Lake tự động detect changes (reframing) khi setting "Keep your Direct Lake data up to date" được bật. Nếu tắt → model không tự động cập nhật → stale data.
> - **(A) Sai** — Direct Lake KHÔNG cần Import refresh. Schedule frequent refreshes là unnecessary và có thể conflict với auto-reframing.
> - **(C) Sai** — Tạo lại model là quá mức, chỉ cần kiểm tra settings.
> - **(D) Sai** — Capacity size không liên quan tới Direct Lake freshness.

---

### Question 4

**You are monitoring a Spark notebook execution and notice that one stage takes significantly longer than others. In the Spark UI, you observe that most tasks in the stage complete in 5 seconds, but 2 tasks take over 10 minutes.**

**What is the most likely cause?**

A. Insufficient executor memory  
B. Data skew causing uneven task distribution  
C. Network latency to the source  
D. Incorrect checkpoint location

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — Khi hầu hết tasks hoàn thành nhanh nhưng vài tasks mất rất lâu, đây là dấu hiệu điển hình của **data skew** — một số partitions chứa data nhiều hơn hẳn các partition khác → gây "straggler tasks".
> - **(A) Sai** — Thiếu memory gây disk spill cho TẤT CẢ tasks, không chỉ vài tasks.
> - **(C) Sai** — Network latency ảnh hưởng đều tới tất cả tasks.
> - **(D) Sai** — Checkpoint location ảnh hưởng streaming, không liên quan tới stage duration skew.

---

### Question 5

**A Dataflow Gen2 that was running successfully for months suddenly starts failing with an error about "insufficient permissions for accessing staging artifacts."**

**What is the most likely cause and fix?**

A. The Lakehouse destination was deleted — recreate the Lakehouse  
B. The Fabric capacity was scaled down — increase capacity  
C. The original Dataflow creator has not signed in for over 90 days — have a current user take ownership  
D. The Dataflow Gen2 query contains invalid M code — fix the query

**✅ Correct Answer: C**

> **Giải thích**:
> - **(C) Đúng** — Đây là lỗi phổ biến của Dataflow Gen2. Khi người tạo Dataflow không sign in trong >90 ngày (hoặc rời tổ chức), token hết hạn → không thể access staging artifacts. Giải pháp: một user hiện tại **take ownership** của Dataflow.
> - **(A) Sai** — Lỗi nói về "staging artifacts", không phải destination.
> - **(B) Sai** — Capacity scaling gây throttling, không phải permission errors.
> - **(D) Sai** — Invalid M code gây lỗi khác (syntax/transformation errors).

---

### Question 6

**You need to view the centralized status of ALL Fabric items (pipelines, notebooks, dataflows, semantic models) across multiple workspaces to quickly identify failures in the last 24 hours.**

**Which tool should you use?**

A. Power BI Admin Portal  
B. Monitoring Hub  
C. Microsoft Fabric Capacity Metrics App  
D. Azure Monitor

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — Monitoring Hub cung cấp centralized view của tất cả Fabric item runs, có thể lọc theo time range (last 24 hours), status (Failed), và item type. Đây là công cụ duy nhất cho operational monitoring tập trung.
> - **(A) Sai** — Admin Portal quản lý tenant-level settings, không phải job monitoring.
> - **(C) Sai** — Capacity Metrics App theo dõi resource usage, không phải job status.
> - **(D) Sai** — Azure Monitor tích hợp với Azure resources, không trực tiếp với Fabric items.

---

### Question 7

**You are troubleshooting a Direct Lake semantic model that unexpectedly falls back to DirectQuery mode, causing poor report performance.**

**Which action should you take to prevent this fallback?**

A. Schedule more frequent Import refreshes  
B. Configure the model to use "DirectLakeOnly" behavior mode  
C. Increase the size of the Lakehouse tables  
D. Convert the model to Import mode

**✅ Correct Answer: B**

> **Giải thích**:
> - **(B) Đúng** — Setting "DirectLakeOnly" mode ngăn model fallback sang DirectQuery. Nếu model không thể query Direct Lake (do unsupported DAX), nó sẽ báo lỗi thay vì silently fallback. Điều này giúp phát hiện và fix vấn đề gốc.
> - **(A) Sai** — Direct Lake không dùng Import refresh.
> - **(C) Sai** — Kích thước table không liên quan tới DirectQuery fallback.
> - **(D) Sai** — Chuyển sang Import mode mất hết lợi ích Direct Lake.

---

### Question 8

**Your team uses the Fabric Capacity Metrics App and notices that CU utilization consistently exceeds 100% during business hours, resulting in throttling.**

**Which TWO actions can help resolve this? (Select TWO)**

A. Move heavy background jobs to run during off-peak hours  
B. Delete the Monitoring Hub  
C. Enable Autoscale Billing for Spark workloads  
D. Disable all scheduled refreshes  
E. Reduce the number of workspaces

**✅ Correct Answers: A and C**

> **Giải thích**:
> - **(A) Đúng** — Shifting background workloads (batch pipelines, OPTIMIZE tasks) tới off-peak hours giảm CU pressure trong business hours.
> - **(C) Đúng** — Autoscale Billing cho Spark chạy jobs trên serverless resources riêng, không impact shared capacity pool → giảm throttling.
> - **(B) Sai** — Xóa Monitoring Hub không giảm CU usage.
> - **(D) Sai** — Disable ALL refreshes là quá extreme và ảnh hưởng business.
> - **(E) Sai** — Số lượng workspaces không trực tiếp ảnh hưởng CU usage — workload mới quan trọng.

---

*Tài liệu được tổng hợp dựa trên Skills Measured as of July 21, 2026*  
*Nguồn tham khảo: Microsoft Learn, Microsoft Fabric Documentation*


---

## 7. BỔ SUNG ÔN THI 2026: WORKSPACE-WIDE ALERT BẰNG KQL

Khi workspace monitoring ghi job logs vào Eventhouse, có thể dùng KQL Queryset + Activator:

```kusto
ItemJobEventLogs
| where ingestion_time() > ago(10m)
| where JobType == "Pipeline" and JobStatus == "Failed"
| project Timestamp, WorkspaceName, ItemName, JobInstanceId, JobStatus
| order by Timestamp desc
```

### Cách chọn công cụ

- **Monitoring Hub:** run status và drill-down operation.
- **Capacity Metrics app:** CU, overload, smoothing và throttling/rejection.
- **Built-in failure notification:** email cho scheduled pipeline failure.
- **Activator/Job events:** manual + scheduled, Teams/email và conditions phức tạp.

### Câu hỏi tình huống bổ sung

**Capacity đạt peak 130% trong một timepoint nhưng không có request bị reject. Kết luận nào đúng nhất?**

A. Metrics app bị sai  
B. Overload không nhất thiết gây rejection ngay; smoothing/overage protection và autoscale quyết định delay/rejection  
C. Mọi job đã thất bại  
D. Phải VACUUM Warehouse

<details><summary>Đáp án</summary>

**B.** Không đồng nhất “>100%” với “mọi request lập tức bị throttled/rejected”.
</details>


---

## NGUỒN CHÍNH THỨC ĐÃ ĐỐI CHIẾU

- [DP-700 study guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)
- [Capacity Metrics Compute page](https://learn.microsoft.com/en-us/fabric/enterprise/metrics-app-compute-page)
- [Pipeline alerts](https://learn.microsoft.com/en-us/fabric/data-factory/create-alerts-for-pipeline-runs)
- [Job events](https://learn.microsoft.com/en-us/fabric/real-time-hub/explore-fabric-job-events)

---

## 7. CÂU HỎI SCENARIO — MONITORING & ALERTS

> Các câu hỏi dưới đây là câu tự soạn theo blueprint DP-700, **không phải câu hỏi thật hoặc exam dump**.

### Câu D3-M1 — Monitoring Hub: Operational Run Status

**Cần xem trạng thái (Success/Failed/Running) của tất cả pipeline và notebook runs trong workspace trong 24 giờ qua. Công cụ nào phù hợp?**

A. Monitoring Hub  
B. Capacity Metrics app  
C. Git repo commit history  
D. Domain admin portal  

<details><summary>Đáp án và giải thích</summary>

**A.** [Monitoring Hub](https://learn.microsoft.com/en-us/fabric/admin/monitoring-hub) là nơi xem **operational run status** của tất cả Fabric items (pipeline, notebook, Dataflow, Spark job) trong workspace. Capacity Metrics app theo dõi resource usage của capacity, không phải run status từng item.

</details>

---

### Câu D3-M2 — Capacity Metrics: Resource Health

**Cần xác định item nào đang tiêu thụ nhiều CU (Capacity Units) nhất và nguyên nhân capacity bị throttled. Công cụ nào phù hợp?**

A. Capacity Metrics app  
B. Monitoring Hub only  
C. KQL database schema browser  
D. Deployment pipeline  

<details><summary>Đáp án và giải thích</summary>

**A.** [Capacity Metrics app](https://learn.microsoft.com/en-us/fabric/enterprise/metrics-app) phân tích **capacity resource usage** — xem CU consumption theo item, identify top consumers, xem smoothing và throttling stages. Monitoring Hub theo dõi operational status, không phải capacity health.

</details>

---

### Câu D3-M3 — Alert Email Đơn Giản Cho Pipeline

**Pipeline scheduled chạy hàng ngày. Muốn nhận email khi pipeline fail mà không cần cấu hình phức tạp. Giải pháp ít vận hành nhất?**

A. Built-in failure notification trong pipeline settings  
B. Tạo custom Spark pool  
C. V-Order optimization  
D. RLS trên Lakehouse  

<details><summary>Đáp án và giải thích</summary>

**A.** [Built-in failure notification](https://learn.microsoft.com/en-us/fabric/data-factory/create-alerts-for-pipeline-runs) trong pipeline settings cho phép cấu hình email alert khi pipeline fail chỉ bằng vài click — không cần code, không cần external service. Đây là giải pháp có **operational overhead thấp nhất** cho use case này.

</details>

---

### Câu D3-M4 — Activator Cho Alert Phức Tạp

**Cần gửi thông báo Teams cho cả manual và scheduled failures, với condition phức tạp (ví dụ: chỉ alert khi failure rate > 20% trong 1 giờ). Giải pháp nào phù hợp?**

A. Activator trên Job events hoặc KQL query  
B. Chỉ dùng scheduled email notification  
C. VACUUM  
D. GitHub issue tracker  

<details><summary>Đáp án và giải thích</summary>

**A.** [Activator](https://learn.microsoft.com/en-us/fabric/real-time-hub/explore-fabric-job-events) (Fabric Data Activator) cho phép định nghĩa **custom conditions và actions** — monitor Job events hoặc KQL query results, trigger Teams/email/webhook khi condition thỏa mãn. Phù hợp cho alert logic phức tạp hơn built-in notification.

</details>

---

### Câu D3-M5 — Overload CU > 100%

**Capacity Metrics app hiển thị CU usage đạt 120% trong một số timepoints. Tuy nhiên không có request nào bị reject ngay lập tức. Giải thích nào đúng nhất?**

A. Metrics app bị lỗi — không thể vượt 100%  
B. Overload không đồng nghĩa rejection ngay — **smoothing**, **overage protection**, và **autoscale** quyết định khi nào throttling xảy ra  
C. Mọi job đã thất bại  
D. Phải chạy VACUUM Warehouse ngay  

<details><summary>Đáp án và giải thích</summary>

**B.** [Fabric capacity throttling](https://learn.microsoft.com/en-us/fabric/enterprise/throttling) là tiến trình nhiều giai đoạn: first rejection xảy ra ở mức overload cao và kéo dài, không phải ngay khi vượt 100%. **Smoothing** làm mờ peaks tạm thời; **overage protection** cho phép burst một thời gian ngắn. Điều này là điểm bẫy phổ biến trong đề thi.

</details>

---

## 8. LAB THỰC HÀNH — WORKSPACE MONITORING

### Lab 9 — KQL Query Cho Workspace Monitoring

**Mục tiêu:** Sử dụng KQL để phân tích job events trong workspace và tính failure rate.

```kusto
// Query workspace job events từ 30 phút qua
// (Table ItemJobEventLogs có sẵn trong Fabric Real-Time Hub)
ItemJobEventLogs
| where ingestion_time() > ago(30m)
| summarize
    TotalRuns    = count(),
    FailedRuns   = countif(JobStatus == "Failed"),
    SuccessRuns  = countif(JobStatus == "Succeeded")
    by JobType, ItemName
| extend FailureRate = todouble(FailedRuns) / TotalRuns
| where TotalRuns > 0
| order by FailureRate desc
```

**Giải thích từng bước:**

| Bước | KQL | Mục đích |
|------|-----|---------|
| 1 | `where ingestion_time() > ago(30m)` | Lọc 30 phút gần nhất |
| 2 | `summarize count() by JobType, ItemName` | Đếm theo item và loại job |
| 3 | `countif(JobStatus == "Failed")` | Đếm riêng lần thất bại |
| 4 | `extend FailureRate = ...` | Tính tỷ lệ thất bại |
| 5 | `order by FailureRate desc` | Item có failure rate cao nhất lên đầu |

**Mở rộng: Alert khi failure rate cao**

```kusto
// Cấu hình Activator để alert khi FailureRate > 0.5 (50%)
ItemJobEventLogs
| where ingestion_time() > ago(1h)
| summarize
    FailedRuns = countif(JobStatus == "Failed"),
    TotalRuns  = count()
    by ItemName
| extend FailureRate = todouble(FailedRuns) / TotalRuns
| where FailureRate > 0.5 and TotalRuns >= 3   // Ít nhất 3 runs mới alert
| project ItemName, FailureRate, FailedRuns, TotalRuns
```

> [!NOTE]
> `ItemJobEventLogs` chứa Job events từ Fabric workspace. Tham khảo: [Fabric Job events](https://learn.microsoft.com/en-us/fabric/real-time-hub/explore-fabric-job-events)

