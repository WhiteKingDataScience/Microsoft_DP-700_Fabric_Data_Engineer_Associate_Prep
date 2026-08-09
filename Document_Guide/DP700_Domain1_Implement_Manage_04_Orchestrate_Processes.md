# DP-700: Orchestrate Processes
## Implement and manage an analytics solution (30–35%)
### Skills measured as of July 21, 2026

> [!IMPORTANT]
> **Bản hiệu chỉnh ngày 09/08/2026.** Nội dung được đối chiếu với blueprint DP-700 có hiệu lực từ **21/07/2026**. Microsoft cho biết phần lớn câu hỏi tập trung vào tính năng GA, nhưng tính năng Preview phổ biến vẫn có thể xuất hiện. Với các bảng “supported items”, runtime, limits và trạng thái Preview/GA, hãy kiểm tra Microsoft Learn lần cuối trước ngày thi.


> [!CAUTION]
> **Không có tài liệu nào có thể bảo đảm “trúng mọi câu hỏi”.** Microsoft nêu rõ danh sách skills là minh họa và đề có thể hỏi các chủ đề liên quan. Mục tiêu của bản này là: phủ đủ blueprint hiện hành, sửa các facts đã thay đổi, và luyện cách chọn đáp án theo **scenario + constraint + least privilege/least operations**.

> **Tài liệu ôn thi DP-700: Fabric Data Engineer Associate**
> Phần này bao gồm toàn bộ kiến thức về điều phối quy trình (orchestration) trong Microsoft Fabric.

---

## 📋 MỤC LỤC

1. [Tổng quan về Orchestration trong Fabric](#1-tổng-quan-về-orchestration-trong-fabric)
2. [Choose Between Dataflow Gen2, Pipeline, and Notebook](#2-choose-between-dataflow-gen2-pipeline-and-notebook)
3. [Design and Implement Schedules and Event-Based Triggers](#3-design-and-implement-schedules-and-event-based-triggers)
4. [Implement Orchestration Patterns with Notebooks and Pipelines](#4-implement-orchestration-patterns-with-notebooks-and-pipelines)
5. [Câu hỏi mô phỏng bài thi (Practice Questions)](#5-câu-hỏi-mô-phỏng-bài-thi)

---

## 1. TỔNG QUAN VỀ ORCHESTRATION TRONG FABRIC

### 1.1 Orchestration là gì?

**Orchestration** là quá trình điều phối và quản lý thứ tự thực thi các tasks/activities trong data engineering workflows. Trong Fabric, orchestration đảm bảo:

- **Thứ tự đúng:** Extract → Transform → Load
- **Xử lý lỗi:** Retry, skip, hoặc alert khi failure
- **Lập lịch:** Chạy theo schedule hoặc event
- **Parameterization:** Truyền dynamic values giữa các tasks
- **Monitoring:** Theo dõi trạng thái execution

### 1.2 Ba công cụ chính

```
┌─────────────────────────────────────────────────────────┐
│                 Fabric Orchestration Tools                │
├──────────────┬──────────────────┬────────────────────────┤
│ Dataflow     │    Data          │    Notebook            │
│ Gen2         │    Pipeline      │                        │
├──────────────┼──────────────────┼────────────────────────┤
│ Low-code     │ Orchestrator     │ Pro-code               │
│ Visual UI    │ Visual UI        │ Code-first             │
│ Power Query  │ Drag-and-drop    │ PySpark/SQL/R/Scala    │
│              │ Activities       │                        │
└──────────────┴──────────────────┴────────────────────────┘
```

---

## 2. CHOOSE BETWEEN DATAFLOW GEN2, PIPELINE, AND NOTEBOOK

### 2.1 So sánh chi tiết

| Tiêu chí | Dataflow Gen2 | Data Pipeline | Notebook |
|----------|---------------|---------------|----------|
| **Approach** | Low-code (Power Query/M) | Visual orchestration | Pro-code (PySpark, SQL, Scala, R) |
| **Primary Role** | Data preparation & transformation | Workflow orchestration & sequencing | Complex data engineering & ML |
| **Target User** | Analyst, Citizen Developer | All levels | Data Engineer, Data Scientist |
| **Transformation** | ✅ Power Query/M và nhiều connectors | ⚠️ Chủ yếu orchestration/copy; có mapping và gọi công cụ transform | ✅ Full flexibility |
| **Orchestration** | ❌ Limited | ✅ Full (conditional, loops, etc.) | ⚠️ Basic (chaining via `%run`) |
| **Scheduling** | ✅ Basic schedule | ✅ Advanced (schedule + event triggers) | ✅ Basic schedule |
| **Parameters** | ✅ Public parameters | ✅ Full dynamic expressions | ✅ Widget/parameter cells |
| **Data Volume** | Small-Medium | N/A (depends on activities) | Medium-Very Large |
| **Compute Cost** | Higher per record | Minimal (orchestration only) | Lower per record at scale |

### 2.2 Khi nào sử dụng Dataflow Gen2?

**✅ SỬ DỤNG khi:**
- Kết nối tới nguồn dữ liệu có connector phù hợp trong Dataflow Gen2
- Transformations đơn giản: rename, filter, merge, pivot, unpivot
- Users không biết code (citizen developers, analysts)
- Quick data preparation cho reports
- Logic Power Query phù hợp, query folding tốt và SLA/capacity đáp ứng

**❌ KHÔNG SỬ DỤNG khi:**
- Workload cần code tùy biến, stateful logic hoặc Spark-scale processing → cân nhắc Notebook
- Logic phức tạp (SCD Type 2, complex schema evolution)
- Cần full programming flexibility (loops, custom functions)
- High-frequency runs (chi phí tích lũy)

### 2.3 Khi nào sử dụng Data Pipeline?

**✅ SỬ DỤNG khi:**
- Cần **orchestrate** nhiều activities theo thứ tự
- Cần **Copy data** từ source tới destination (Copy activity)
- Cần **conditional logic** (If/Else, Switch)
- Cần **event-based triggers** (file arrival, schedule)
- Cần **error handling** (retry, on-failure paths)
- Cần **dynamic parameters** và expressions

**❌ KHÔNG SỬ DỤNG khi:**
- Chỉ cần đơn giản transform data (dùng Dataflow Gen2 hoặc Notebook)
- Không cần orchestration (single step process)

### 2.4 Khi nào sử dụng Notebook?

**✅ SỬ DỤNG khi:**
- Dữ liệu cần distributed processing hoặc logic phức tạp; không dùng một ngưỡng GB cứng để quyết định
- Logic phức tạp: SCD Type 2, advanced joins, custom UDFs
- Machine Learning workflows
- Schema evolution, dynamic table creation
- Data quality checks phức tạp
- Incremental processing patterns (merge/upsert)
- Cần full programming control

**❌ KHÔNG SỬ DỤNG khi:**
- Users không biết code
- Simple data prep (dùng Dataflow Gen2 nhanh hơn)
- Chỉ cần orchestrate (dùng Pipeline)

### 2.5 Patterns kết hợp phổ biến

```
Pattern 1: Pipeline + Notebook
┌──────────────────────────────────────────┐
│ Data Pipeline                            │
│ ├── Copy Activity (source → staging)     │
│ ├── Notebook Activity (transform)        │
│ ├── Notebook Activity (quality check)    │
│ └── Notebook Activity (load to gold)     │
└──────────────────────────────────────────┘

Pattern 2: Pipeline + Dataflow Gen2
┌──────────────────────────────────────────┐
│ Data Pipeline                            │
│ ├── Dataflow Activity (prep data A)      │
│ ├── Dataflow Activity (prep data B)      │
│ └── Stored Procedure (aggregate)         │
└──────────────────────────────────────────┘

Pattern 3: Pipeline + Notebook + Dataflow Gen2
┌──────────────────────────────────────────┐
│ Data Pipeline (Master Orchestrator)      │
│ ├── Dataflow Gen2 (simple CRM extract)  │
│ ├── Notebook (complex IoT transform)    │
│ ├── Notebook (ML feature engineering)    │
│ └── Dataflow Gen2 (final report prep)   │
└──────────────────────────────────────────┘
```

### 2.6 Decision Matrix (Flowchart quyết định)

```
Cần orchestrate nhiều steps?
├── YES → Data Pipeline (+ Notebook/Dataflow bên trong)
└── NO → Chỉ cần transform?
    ├── YES → Dữ liệu lớn hoặc logic phức tạp?
    │   ├── YES → Notebook
    │   └── NO → Dataflow Gen2
    └── NO → Chỉ cần copy data?
        └── YES → Data Pipeline (Copy Activity)
```

---

## 3. DESIGN AND IMPLEMENT SCHEDULES AND EVENT-BASED TRIGGERS

### 3.1 Ba cách chính để chạy Pipeline

Fabric Data Factory hiện mô tả ba nhóm run:
1. **On-demand** — chạy thủ công/ngay lập tức.
2. **Scheduled** — chạy theo lịch.
3. **Event-based** — chạy khi event phù hợp xảy ra.

### 3.2 Fixed schedule

Khi tạo schedule, bạn chọn frequency, start/end date-time và time zone. **Fabric hiện yêu cầu cả start date và end date**; nếu muốn lịch dài hạn, đặt end date xa trong tương lai.

Một pipeline có thể có nhiều schedules (hiện Microsoft nêu tối đa 20 schedules/pipeline).

### 3.3 Interval-based schedule (Public Preview) — đừng gọi nhầm là Tumbling Window Trigger

**Interval-based schedule** tạo các fixed, non-overlapping windows và cung cấp trigger parameters **Window start time / Window end time**.

Nhưng tính đến 09/08/2026:
- đây là **Public Preview**;
- **time-slice monitoring và backfill chưa có**;
- không nên học nó như ADF **Tumbling Window Trigger** đầy đủ.

> [!WARNING]
> Nếu tài liệu/câu hỏi cũ nói Fabric hiện có Tumbling Window Trigger với built-in backfill và trigger dependency giống Azure Data Factory, đó là thông tin cũ/sai đối với Fabric hiện hành. Khi migrate ADF tumbling-window workload, watermark/backfill/dependency semantics có thể phải **redesign**.

### 3.4 Event-based runs

Fabric pipeline có thể được kích hoạt từ các event phù hợp như file/data events và Fabric events. Câu scenario thường tập trung vào ý nghĩa:
- “chạy ngay khi file tới” → **event-based** thay vì polling schedule;
- “chạy lúc 06:00 mỗi ngày” → **fixed schedule**;
- “xử lý theo non-overlapping time interval và cần window start/end” → **interval-based schedule (Preview)**.

Với storage event, dùng filter/path pattern để giảm false triggers. Trigger metadata có thể được truyền vào pipeline/dynamic expressions khi feature/event cung cấp các trigger parameters tương ứng.

### 3.5 Schedule với parameters và Variable library

Schedule có thể truyền giá trị cho pipeline parameters. Các parameter names trong schedule phải khớp với parameters khai báo trong pipeline. Fabric còn cho phép lấy value từ **Variable library**, rất hữu ích cho Dev/Test/Prod configuration.

### 3.6 Failure notifications

Scheduled pipeline runs có thể cấu hình failure notifications. Đừng nhầm notification với retry: retry xử lý transient failure của activity; notification báo cho người vận hành.

### 3.7 Notebook/Dataflow scheduling — nguyên tắc chọn

- Nếu chỉ cần chạy một Notebook theo lịch đơn giản, có thể schedule notebook trực tiếp.
- Nếu cần event trigger, dependency, nhiều bước, retry/error path, parameters giữa steps → bọc/điều phối bằng **Data Pipeline**.
- Dataflow Gen2 phù hợp cho low-code transformation; khi workflow cần orchestration nhiều activity, Pipeline là coordinator.

## 4. IMPLEMENT ORCHESTRATION PATTERNS WITH NOTEBOOKS AND PIPELINES

### 4.1 Pipeline Activities

#### Core Activities:

| Activity | Mô tả |
|----------|--------|
| **Copy Data** | Sao chép dữ liệu từ source tới destination |
| **Dataflow** | Chạy Dataflow Gen2 |
| **Notebook** | Chạy Fabric Notebook |
| **Stored Procedure** | Chạy stored procedure trong Warehouse/SQL Database |
| **Script** | Chạy SQL script |
| **KQL** | Chạy Kusto Query |
| **Delete Data** | Xóa dữ liệu từ storage |

#### Control Flow Activities:

| Activity | Mô tả |
|----------|--------|
| **If Condition** | Conditional branching (If/Else) |
| **Switch** | Multiple branching dựa trên expression |
| **ForEach** | Loop qua array/collection |
| **Until** | Loop cho đến khi condition = true |
| **Wait** | Pause execution |
| **Set Variable** | Gán giá trị cho variable |
| **Append Variable** | Thêm giá trị vào array variable |
| **Lookup** | Query data source và return results |
| **Get Metadata** | Lấy metadata (file size, exist, last modified) |
| **Web** | Call HTTP endpoint |
| **Fail** | Force fail pipeline |

### 4.2 Parameters trong Pipeline

#### Pipeline Parameters

```json
// Định nghĩa parameters trong Pipeline
{
    "parameters": {
        "SourceTable": {
            "type": "String",
            "defaultValue": "sales_data"
        },
        "LoadDate": {
            "type": "String",
            "defaultValue": "2026-07-21"
        },
        "IsFullLoad": {
            "type": "Bool",
            "defaultValue": false
        },
        "MaxRecords": {
            "type": "Int",
            "defaultValue": 1000000
        }
    }
}
```

#### Sử dụng Parameters trong Activities

```
// Tham chiếu parameter trong expression:
@pipeline().parameters.SourceTable
@pipeline().parameters.LoadDate
@pipeline().parameters.IsFullLoad
```

### 4.3 Dynamic Expressions

#### System Variables

| Variable | Mô tả | Ví dụ |
|----------|--------|-------|
| `@pipeline().RunId` | Unique ID của pipeline run | "abc123-def456" |
| `@pipeline().TriggerTime` | Thời gian trigger | "2026-07-21T06:00:00Z" |
| `@pipeline().TriggerType` | Loại trigger | "ScheduleTrigger" |
| `@pipeline().TriggerId` | ID của trigger | "trigger-789" |
| `@pipeline().GroupId` | Group ID | "group-456" |

#### Expression Functions

```
// String functions
@concat('Table_', pipeline().parameters.SourceTable)
@replace(pipeline().parameters.FileName, '.csv', '.parquet')
@toLower(pipeline().parameters.Region)

// Date/Time functions
@utcnow()
@formatDateTime(utcnow(), 'yyyy-MM-dd')
@adddays(utcnow(), -7)
@startOfDay(utcnow())

// Logical functions
@if(pipeline().parameters.IsFullLoad, 'FULL', 'INCREMENTAL')
@equals(pipeline().parameters.Region, 'US')
@and(greater(pipeline().parameters.MaxRecords, 0), not(empty(pipeline().parameters.SourceTable)))

// Conversion functions
@int(pipeline().parameters.MaxRecords)
@string(pipeline().parameters.RecordCount)
@json('{"key": "value"}')

// Collection functions
@length(activity('Lookup').output.value)
@first(activity('Lookup').output.value)
```

### 4.4 Truyền Parameters cho Notebook từ Pipeline

**Bước 1: Tạo parameter cell trong Notebook**
```python
# Đánh dấu cell này là "Parameters" cell (click Toggle parameter cell)
source_table = "default_table"
load_date = "2026-01-01"
is_full_load = False
```

**Bước 2: Cấu hình trong Pipeline Notebook Activity**
```json
// Notebook Activity → Settings → Base Parameters
{
    "source_table": {
        "value": "@pipeline().parameters.SourceTable",
        "type": "string"
    },
    "load_date": {
        "value": "@formatDateTime(utcnow(), 'yyyy-MM-dd')",
        "type": "string"
    },
    "is_full_load": {
        "value": "@pipeline().parameters.IsFullLoad",
        "type": "bool"
    }
}
```

**Bước 3: Sử dụng parameters trong Notebook**
```python
# Parameters đã được inject bởi Pipeline
print(f"Processing table: {source_table}")
print(f"Load date: {load_date}")
print(f"Full load: {is_full_load}")

if is_full_load:
    df = spark.sql(f"SELECT * FROM {source_table}")
else:
    df = spark.sql(f"SELECT * FROM {source_table} WHERE date = '{load_date}'")
```

### 4.5 Notebook Exit Value (Output từ Notebook)

> [!IMPORTANT]
> Microsoft đã đổi tên **MSSparkUtils → NotebookUtils**. Namespace cũ vẫn backward-compatible, nhưng nên dùng `notebookutils` cho code mới vì tính năng mới được cập nhật ở namespace này.

```python
# Trong Notebook - return value cho Pipeline
import json

result = {
    "records_processed": 15000,
    "status": "success",
    "output_path": "Tables/fact_sales"
}

# Sử dụng NotebookUtils để return exit value
notebookutils.notebook.exit(json.dumps(result))
```

**Lấy output trong Pipeline:**
```
// Tham chiếu output từ Notebook activity
@activity('TransformNotebook').output.result.exitValue

// Parse JSON exit value
@json(activity('TransformNotebook').output.result.exitValue).records_processed
@json(activity('TransformNotebook').output.result.exitValue).status
```

### 4.6 Orchestration Patterns

#### Pattern 1: Sequential Execution

```
Pipeline: Daily_ETL
├── [1] Copy Activity: Extract from SQL Server
├── [2] Notebook Activity: Transform (depends on [1])
├── [3] Notebook Activity: Data Quality Check (depends on [2])
└── [4] Dataflow Activity: Load to Report Tables (depends on [3])

Dependency: [1] → [2] → [3] → [4]
Condition: All "On Success"
```

#### Pattern 2: Parallel Execution

```
Pipeline: Parallel_Ingest
├── [1] Copy Activity: Extract Sales Data    ─┐
├── [2] Copy Activity: Extract Customer Data  ├── All run in parallel
├── [3] Copy Activity: Extract Product Data  ─┘
└── [4] Notebook Activity: Merge All (depends on [1]+[2]+[3])

Dependency: [1],[2],[3] → [4] (fan-in pattern)
```

#### Pattern 3: Conditional Execution (If/Else)

```
Pipeline: Conditional_Load
├── [1] Lookup Activity: Check if data exists
├── [2] If Condition: @greater(activity('Lookup').output.count, 0)
│   ├── TRUE:  Notebook Activity (Incremental Load)
│   └── FALSE: Notebook Activity (Full Load)
└── [3] Script Activity: Update metadata (depends on [2])
```

#### Pattern 4: ForEach Loop

```
Pipeline: Multi_Table_ETL
├── [1] Lookup Activity: Get list of tables
│   └── Returns: ["sales", "customer", "product", "inventory"]
├── [2] ForEach Activity: @activity('Lookup').output.value
│   ├── Sequential: false (parallel execution)
│   ├── Batch Count: 4 (max parallel)
│   └── Inner Activities:
│       ├── Copy Activity: @item().tableName
│       └── Notebook: Transform_@{item().tableName}
└── [3] Notification Activity: Send completion email
```

#### Pattern 5: Error Handling

```
Pipeline: ETL_With_ErrorHandling
├── [1] Notebook Activity: Transform Data
│   ├── On Success → [2] Update Success Status
│   ├── On Failure → [3] Log Error Details
│   │   └── [4] Send Alert Email
│   └── On Completion → [5] Cleanup Temp Files
```

#### Pattern 6: Master-Child Pipelines

```
Master Pipeline: Daily_Orchestrator
├── [1] Execute Pipeline: Ingest_Pipeline (parameters: date, source)
├── [2] Execute Pipeline: Transform_Pipeline (depends on [1])
├── [3] Execute Pipeline: Quality_Check_Pipeline (depends on [2])
│   ├── On Success → [4] Execute Pipeline: Load_Production
│   └── On Failure → [5] Execute Pipeline: Rollback_Pipeline
```

### 4.7 Truyền Parameters cho Dataflow Gen2 từ Pipeline

**Bước 1: Enable Public Parameters trong Dataflow Gen2**
1. Mở Dataflow Gen2
2. Click parameter icon hoặc Manage Parameters
3. Tạo parameters và set "Is Public" = Yes

**Bước 2: Sử dụng parameter trong Power Query/M**
```m
// M code sử dụng parameter
let
    Source = Sql.Database(ServerName, DatabaseName),
    FilteredTable = Table.SelectRows(Source{[Schema="dbo", Item=TableName]}[Data], 
        each [Date] >= LoadDate)
in
    FilteredTable
```

**Bước 3: Map parameters trong Pipeline Dataflow Activity**
```
Pipeline Dataflow Activity → Settings:
├── Parameter: ServerName → @pipeline().parameters.SqlServer
├── Parameter: DatabaseName → @pipeline().parameters.Database
├── Parameter: TableName → @pipeline().parameters.SourceTable
└── Parameter: LoadDate → @formatDateTime(utcnow(), 'yyyy-MM-dd')
```

> ⚠️ **Lưu ý thi:** Khi edit và save pipeline containing Dataflow activity, parameter mappings có thể revert to defaults. Luôn **verify parameter bindings** sau khi edit pipeline.

### 4.8 Notebook Chaining (Gọi Notebook từ Notebook)

```python
# Gọi notebook khác từ notebook hiện tại
# Sử dụng notebookutils.notebook.run()

# Chạy synchronous (blocking)
result = notebookutils.notebook.run("Transform_Sales", 600, {"load_date": "2026-07-21"})
# Parameters: notebook_name, timeout_seconds, parameters_dict

# Chạy multiple notebooks parallel
notebookutils.notebook.runMultiple([
    {"notebook": "Transform_Sales", "params": {"date": "2026-07-21"}},
    {"notebook": "Transform_Customers", "params": {"date": "2026-07-21"}},
    {"notebook": "Transform_Products", "params": {"date": "2026-07-21"}}
])
```

### 4.9 Ví dụ End-to-End Orchestration

**Tình huống:** E-commerce company cần ETL pipeline hàng ngày.

```
Pipeline: Ecommerce_Daily_ETL
│
├── [1] Set Variable: RunDate = @formatDateTime(utcnow(), 'yyyy-MM-dd')
│
├── [2] ForEach: Sources = ["orders", "customers", "products"]
│   └── Copy Activity: 
│       Source: SQL Server / @{item()} table
│       Dest: Lakehouse / Files/raw/@{item()}_@{variables('RunDate')}.parquet
│
├── [3] Notebook Activity: Bronze_to_Silver
│   Parameters:
│     run_date = @variables('RunDate')
│     tables = '["orders", "customers", "products"]'
│   
│   [Notebook Code:]
│   ┌──────────────────────────────────────────────┐
│   │ # Read raw parquet files                      │
│   │ for table in json.loads(tables):              │
│   │   df = spark.read.parquet(                    │
│   │     f"Files/raw/{table}_{run_date}.parquet")  │
│   │   # Clean, deduplicate, validate              │
│   │   df_clean = clean_data(df)                   │
│   │   # Write to Silver (Delta)                   │
│   │   df_clean.write.format("delta")              │
│   │     .mode("merge")                            │
│   │     .saveAsTable(f"silver_{table}")           │
│   │ notebookutils.notebook.exit("success")        │
│   └──────────────────────────────────────────────┘
│
├── [4] If Condition: 
│   @equals(json(activity('Bronze_to_Silver')
│     .output.result.exitValue), 'success')
│   ├── TRUE → [5] Notebook: Silver_to_Gold
│   └── FALSE → [6] Fail Activity: "Transform failed"
│
├── [5] Notebook Activity: Silver_to_Gold
│   [Creates dimensional model: dim_customer, 
│    dim_product, fact_orders]
│
└── [7] Script Activity: Log completion
    [INSERT INTO etl_log (run_date, status, end_time)
     VALUES (@variables('RunDate'), 'SUCCESS', GETDATE())]
```

---

## 5. CÂU HỎI MÔ PHỎNG BÀI THI

### Question 1

**A data engineering team needs to extract data from a REST API, perform complex transformations including SCD Type 2 updates on a 50GB dataset, and load results into a Lakehouse. Which combination of tools should they use?**

A. Dataflow Gen2 for extraction and transformation  
B. Data Pipeline with Copy Activity for extraction, Notebook for transformation  
C. Notebook for both extraction and transformation  
D. Data Pipeline with Dataflow Gen2 for extraction and transformation  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: B**

**Giải thích:**
- **Pipeline + Notebook** là combination tối ưu cho use case này:
  - **Copy Activity** trong Pipeline xử lý REST API extraction hiệu quả (built-in REST connector)
  - **Notebook** phù hợp với SCD Type 2/Delta `MERGE` phức tạp và cần full programmatic control; 50 GB chỉ là context, không phải hard threshold
- **(A)** Không loại Dataflow Gen2 chỉ vì “50 GB”; điểm quyết định ở đây là **SCD Type 2/stateful merge logic phức tạp + cần programmatic control**, nên Notebook/PySpark là lựa chọn mạnh hơn
- **(C)** Notebook có thể làm cả extraction nhưng Copy Activity có built-in REST connector tối ưu hơn
- **(D)** Không nên loại Dataflow chỉ bằng ngưỡng 50 GB; nhưng SCD2/logic stateful phức tạp thường phù hợp Notebook hơn khi cần full programmatic control

</details>

---

### Question 2

**You have a Data Pipeline that needs to process data for multiple tables. The list of tables is stored in a metadata table in a Warehouse. You want the pipeline to dynamically process each table. Which pipeline pattern should you use?**

A. Create a separate pipeline for each table  
B. Use a Lookup activity to get the table list, then a ForEach activity to process each table  
C. Use a Switch activity with a case for each table  
D. Use an Until activity to iterate through the tables  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: B**

**Giải thích:**
- **Lookup + ForEach** là pattern chuẩn cho dynamic table processing:
  - Lookup queries metadata table → returns array of table names
  - ForEach iterates qua array → mỗi iteration xử lý một table
- **(A)** Separate pipelines = không scalable, khó maintain
- **(C)** Switch cần predefined cases, không dynamic
- **(D)** Until dùng cho "loop until condition met", không phải "iterate through list"
- ForEach hỗ trợ parallel execution (batch count) để tăng throughput

</details>

---

### Question 3

**A pipeline contains a Notebook activity. The notebook processes data and needs to return the number of records processed back to the pipeline for a subsequent conditional check. How should the notebook return this value?**

A. Write the count to a Lakehouse table and use a Lookup activity  
B. Use `notebookutils.notebook.exit(str(record_count))` and reference the exit value in the pipeline  
C. Set a pipeline variable from within the notebook  
D. Use `print(record_count)` and capture stdout  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: B**

**Giải thích:**
- `notebookutils.notebook.exit(value)` là cách chính thức để return output từ Notebook cho Pipeline
- Pipeline tham chiếu bằng: `@activity('NotebookActivity').output.result.exitValue`
- **(A)** Hoạt động nhưng phức tạp và tốn resources không cần thiết
- **(C)** Notebook KHÔNG thể trực tiếp set pipeline variables
- **(D)** `print()` chỉ hiển thị trong notebook output, pipeline không capture stdout
- Best practice: Return JSON string qua exit value để truyền multiple values

</details>

---

### Question 4

**You need to ensure that a Data Pipeline runs automatically every day at 6:00 AM UTC, but ONLY on weekdays (Monday through Friday). Which trigger configuration should you use?**

A. Schedule trigger with daily recurrence, and add an If Condition activity to check the day of week  
B. Schedule trigger configured with weekly recurrence, selecting Monday through Friday  
C. Event-based trigger that checks the system clock  
D. Interval-based schedule with a 24-hour interval  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: B**

**Giải thích:**
- **Weekly recurrence** với selected days (Mon-Fri) là cách đúng để schedule weekday-only runs
- **(A)** If Condition hoạt động nhưng pipeline vẫn trigger mỗi ngày (kể cả weekends) → waste resources
- **(C)** Event-based triggers dựa trên storage events, không phải clock
- **(D)** Interval-based schedule xử lý non-overlapping intervals, không phải cách tối ưu để biểu diễn weekday-only calendar schedule
- Schedule trigger cho phép granular control: chọn ngày trong tuần, giờ, timezone

</details>

---

### Question 5

**You want a Data Pipeline to automatically start whenever a new CSV file is uploaded to an Azure Blob Storage container. Which trigger type should you use?**

A. Schedule trigger  
B. Interval-based schedule  
C. Event-based trigger (Blob Created)  
D. Manual trigger with a polling loop  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: C**

**Giải thích:**
- **Event-based trigger** với "Blob Created" event tự động detect khi file mới được upload → trigger pipeline
- **(A)** Schedule trigger chạy theo thời gian cố định, không react to file arrival
- **(B)** Interval-based schedule vẫn time-based, không phản ứng trực tiếp với file-arrival event
- **(D)** Polling loop waste resources và có latency
- Event trigger cấu hình: storage account, container path, blob path pattern (e.g., ends with .csv)

</details>

---

### Question 6

**In a Data Pipeline, you need to construct a file path dynamically using the current date. The desired format is "data/2026/07/21/sales.parquet". Which expression should you use?**

A. `@concat('data/', formatDateTime(utcnow(), 'yyyy'), '/', formatDateTime(utcnow(), 'MM'), '/', formatDateTime(utcnow(), 'dd'), '/sales.parquet')`  
B. `@concat('data/', utcnow(), '/sales.parquet')`  
C. `@string.Format('data/{0}/sales.parquet', utcnow())`  
D. `@formatDateTime(utcnow(), 'data/yyyy/MM/dd/sales.parquet')`  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: A**

**Giải thích:**
- **(A)** Sử dụng `concat()` kết hợp `formatDateTime()` cho từng phần (yyyy, MM, dd) → tạo path chính xác
- **(B)** `utcnow()` trả về full datetime string (bao gồm time), không đúng format
- **(C)** `string.Format` không phải Fabric pipeline expression syntax
- **(D)** `formatDateTime` với custom format string chứa literal characters có thể hoạt động, nhưng cách (A) rõ ràng và an toàn hơn
- Dynamic expressions là kỹ năng cốt lõi cho Fabric Pipeline development

</details>

---

### Question 7

**A Master Pipeline calls a Child Pipeline that performs data ingestion. The Master Pipeline needs to pass the source database name and load date to the Child Pipeline. How should you implement this?**

A. Use global variables that both pipelines can access  
B. Define parameters in the Child Pipeline and pass values from the Master Pipeline's Execute Pipeline activity  
C. Write the values to a shared configuration file  
D. Use environment variables in the Spark session  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: B**

**Giải thích:**
- **Execute Pipeline** activity trong Master Pipeline cho phép truyền parameter values cho Child Pipeline
- Child Pipeline phải có **defined parameters** (DatabaseName, LoadDate)
- Master Pipeline map values: `DatabaseName = @pipeline().parameters.SourceDB`, `LoadDate = @utcnow()`
- **(A)** Fabric pipelines không có "global variables" shared giữa pipelines
- **(C)** Shared config file phức tạp, race conditions possible
- **(D)** Environment variables là Spark concept, không phải Pipeline concept
- Master-Child pipeline pattern là best practice cho modular, reusable orchestration

</details>

---

### Question 8

**A Data Pipeline has three parallel Copy activities. You need all three to complete successfully before a downstream Notebook activity starts. How should you configure the dependencies?**

A. Add the three Copy activities in a ForEach loop, then add the Notebook after the ForEach  
B. Connect each Copy activity to the Notebook with "On Success" dependency  
C. Use a Wait activity between the Copy activities and the Notebook  
D. Place the Copy activities inside an If Condition before the Notebook  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: B**

**Giải thích:**
- Khi **multiple activities** (Copy1, Copy2, Copy3) đều connect tới downstream activity (Notebook) với "On Success" → Pipeline tự động wait cho **ALL upstream** hoàn thành trước khi start downstream
- Đây là **fan-in pattern**: nhiều → một, pipeline engine handles convergence
- **(A)** ForEach chạy sequential (hoặc parallel với limit), khác concept
- **(C)** Wait activity pause fixed time, không detect completion
- **(D)** If Condition dùng cho conditional logic, không phải synchronization
- Pipeline engine natively supports fan-in: downstream activity starts chỉ khi TẤT CẢ upstream dependencies satisfied

</details>

---

### Question 9

**You are designing an orchestration solution. A Dataflow Gen2 extracts data from a CRM system. You need the pipeline to pass a dynamic filter date to the Dataflow so it only extracts records modified after a certain date. What should you configure?**

A. Create a parameter in the Dataflow Gen2 marked as "Public" and map it from the Pipeline's Dataflow activity  
B. Use a Set Variable activity before the Dataflow activity  
C. Hard-code the date in the Dataflow Gen2 query  
D. Create a separate Dataflow for each date  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: A**

**Giải thích:**
- **Public Parameters** trong Dataflow Gen2 cho phép Pipeline truyền values **dynamically** tại runtime
- Workflow: Dataflow Gen2 → Manage Parameters → Create "FilterDate" → Set "Is Public" = Yes → Trong Pipeline Dataflow activity, map parameter = `@formatDateTime(utcnow(), 'yyyy-MM-dd')`
- **(B)** Set Variable chỉ tạo pipeline variable, không truyền cho Dataflow
- **(C)** Hard-code → không dynamic, phải sửa mỗi lần
- **(D)** Separate Dataflow per date = không scalable
- Public parameters cho phép **reuse** Dataflow cho nhiều scenarios mà không sửa Power Query logic

</details>

---

### Question 10

**A pipeline needs to handle a scenario where a Notebook activity may fail due to transient errors. You want the pipeline to retry the Notebook up to 3 times with a 5-minute delay between retries. If it still fails after 3 retries, the pipeline should send an alert email. How should you configure this?**

A. Add three separate Notebook activities chained with "On Failure" dependencies  
B. Configure the Notebook activity's retry policy (Retry count: 3, Retry interval: 300 seconds), and add a Web/Email activity on the "On Failure" path  
C. Use an Until loop with a counter variable  
D. Use a ForEach loop with 3 iterations  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: B**

**Giải thích:**
- **Retry policy** là built-in feature của Pipeline activities:
  - Retry count: Số lần retry (3)
  - Retry interval: Khoảng cách giữa retries tính bằng giây (300 = 5 phút)
- Kết hợp với **On Failure** dependency path → Web/Email activity để gửi alert
- **(A)** Three separate Notebook activities phức tạp, không cần thiết khi có built-in retry
- **(C)** Until loop hoạt động nhưng phức tạp hơn built-in retry policy
- **(D)** ForEach loop sai concept - dùng cho iterating collections, không retry
- Retry policy + On Failure path = clean, maintainable error handling pattern

</details>

---

## 📝 TÓM TẮT KIẾN THỨC CẦN NHỚ

| Chủ đề | Phản xạ khi làm bài |
|---|---|
| **Dataflow Gen2** | Low-code/Power Query; đánh giá query folding, logic, maintainability, SLA — không dùng ngưỡng GB cứng |
| **Data Pipeline** | Copy + orchestrate + dependency + parameters + loops + error paths + triggers |
| **Notebook** | Pro-code/distributed/complex transformation; parameter cell + exit value |
| **Fixed schedule** | Time-based; cần start/end; frequency/timezone |
| **Interval-based schedule** | Preview; non-overlapping windows; **chưa có backfill/time-slice monitoring** |
| **Event-based run** | Phản ứng với event; tránh polling khi event source đáp ứng được |
| **Variable library** | Centralize config/value sets; dùng được trong pipeline schedules/dynamic content |
| **Dynamic expressions** | `pipeline().parameters`, `activity().output`, `variables()`, date/string functions |
| **NotebookUtils** | `notebookutils.notebook.run/runMultiple/exit`; MSSparkUtils cũ backward-compatible |
| **ForEach** | Metadata-driven loop; kiểm soát parallelism/batch count theo capacity |
| **Error handling** | Retry transient errors + On Failure/Completion path + log/Fail activity |

---

## 6. BỔ SUNG ÔN THI 2026: PARAMETERS, DYNAMIC EXPRESSIONS VÀ VARIABLE LIBRARY

### 6.1 Pipeline expression mẫu

```text
@concat('Files/landing/', pipeline().parameters.entity, '/', formatDateTime(utcNow(),'yyyy/MM/dd'))
@activity('LookupWatermark').output.firstRow.LastWatermark
@if(equals(activity('Validate').output.status,'OK'),'continue','stop')
```

### 6.2 Notebook parameter cell mẫu

```python
# Parameter cell: pipeline ghi đè các giá trị này khi gọi notebook
entity = "orders"
load_date = "2026-08-04"
full_load = False

from pyspark.sql.functions import col
source = spark.table(f"bronze.{entity}")
if not full_load:
    source = source.filter(col("_ingestion_date") == load_date)
```

### 6.3 Variable library — config không hard-code

Dùng Variable library khi nhiều pipeline/notebook cần cùng configuration hoặc cần value khác nhau theo environment. Ví dụ `LandingPath`, `TargetWorkspaceId`, `WarehouseName`. Pipeline dynamic content có thể tham chiếu Library variables; NotebookUtils có module `notebookutils.variableLibrary`.

### 6.4 Câu hỏi tình huống bổ sung

**Bạn cần copy 40 bảng theo metadata, chạy tối đa 5 bảng song song và gọi cùng một notebook với tên bảng khác nhau. Pattern nào phù hợp nhất?**

A. 40 pipeline riêng  
B. Lookup metadata → ForEach batch count 5 → Copy/Notebook với dynamic parameters  
C. Dataflow Gen2 không tham số  
D. Một KQL materialized view

<details><summary>Đáp án</summary>

**B.** Đây là metadata-driven orchestration, một pattern rất sát cách DP-700 kiểm tra pipeline parameters, expressions và concurrency.
</details>


---

## NGUỒN CHÍNH THỨC ĐÃ ĐỐI CHIẾU

- [DP-700 Study Guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)
- [Pipeline runs, schedules and events](https://learn.microsoft.com/en-us/fabric/data-factory/pipeline-runs)
- [Fabric Data Factory limitations](https://learn.microsoft.com/en-us/fabric/data-factory/data-factory-limitations)
- [Pipeline expressions](https://learn.microsoft.com/en-us/fabric/data-factory/expression-language)
- [Notebook activity](https://learn.microsoft.com/en-us/fabric/data-factory/notebook-activity)
- [NotebookUtils](https://learn.microsoft.com/en-us/fabric/data-engineering/notebook-utilities)
- [Variable library integration with pipelines](https://learn.microsoft.com/en-us/fabric/data-factory/variable-library-integration-with-data-pipelines)
- [Dataflow Gen2 overview](https://learn.microsoft.com/en-us/fabric/data-factory/dataflows-gen2-overview)

---

## 7. CÂU HỎI SCENARIO — ORCHESTRATION

> Các câu hỏi dưới đây là câu tự soạn theo blueprint DP-700, **không phải câu hỏi thật hoặc exam dump**.

### Câu D1-O1 — Low-Code Transformation

**Yêu cầu: rename cột, filter dữ liệu, merge hai bảng, sử dụng các connector có sẵn, và cần query folding để đẩy xử lý về phía nguồn. Công cụ nào phù hợp nhất?**

A. Dataflow Gen2  
B. Notebook PySpark  
C. Deployment pipeline  
D. Activator  

<details><summary>Đáp án và giải thích</summary>

**A.** [Dataflow Gen2](https://learn.microsoft.com/en-us/fabric/data-factory/dataflows-gen2-overview) sử dụng Power Query — giao diện **low-code** với hơn 150 connectors và hỗ trợ **query folding** (đẩy xử lý về phía source database thay vì kéo toàn bộ dữ liệu về). Các thao tác rename/filter/merge đơn giản là use case điển hình của Dataflow Gen2.

</details>

---

### Câu D1-O2 — Multi-Step Orchestration

**Cần orchestrate theo thứ tự: Copy dữ liệu → Notebook transform → Validation → Gửi thông báo Teams khi có lỗi. Công cụ nào phù hợp nhất?**

A. Data Pipeline  
B. Dataflow Gen2  
C. KQL database  
D. Sensitivity label  

<details><summary>Đáp án và giải thích</summary>

**A.** [Data Pipeline](https://learn.microsoft.com/en-us/fabric/data-factory/data-factory-overview) cung cấp các **activity**: Copy, Notebook, Conditional (If/Else), Web/Teams notification, cùng các **dependency settings** (On Success, On Failure). Đây là công cụ duy nhất trong Fabric có thể orchestrate đa bước với error handling và alerting tích hợp.

</details>

---

### Câu D1-O3 — Metadata-Driven ForEach

**Cần xử lý 40 bảng theo danh sách metadata, chạy tối đa 5 bảng song song cùng lúc. Pattern pipeline nào tối ưu?**

A. Tạo 40 pipeline riêng biệt  
B. Lookup activity lấy danh sách → ForEach batch count = 5 → gọi Copy/Notebook với dynamic parameters  
C. Một notebook hardcode tên 40 bảng  
D. OneLake role cho từng bảng  

<details><summary>Đáp án và giải thích</summary>

**B.** [Metadata-driven ForEach](https://learn.microsoft.com/en-us/fabric/data-factory/tutorial-metadata-driven-pipeline) là pattern chuẩn:
1. **Lookup** query bảng control để lấy danh sách entities cần xử lý
2. **ForEach** với `batchCount = 5` cho phép chạy 5 iterations song song
3. Bên trong ForEach: Copy/Notebook activity nhận `@item().EntityName` làm dynamic parameter

Pattern này **scalable** — thêm entity chỉ cần insert vào bảng control, không sửa pipeline.

</details>

---

## 8. LAB THỰC HÀNH — METADATA-DRIVEN PIPELINE

> Lab này mô phỏng pattern metadata-driven thường gặp trong đề thi DP-700.

### Lab 8 — Xây Dựng Pipeline Theo Metadata

**Mục tiêu:** Pipeline tự động xử lý nhiều entity dựa trên bảng control, hỗ trợ cả Full Load và Incremental Load.

**Bước 1: Tạo bảng control (trong Lakehouse hoặc Warehouse)**

```sql
CREATE TABLE control.etl_config (
    EntityName      VARCHAR(100) NOT NULL,
    SourceQuery     VARCHAR(1000),
    TargetTable     VARCHAR(200) NOT NULL,
    LoadType        VARCHAR(20) NOT NULL,   -- 'Full' hoặc 'Incremental'
    WatermarkColumn VARCHAR(100),           -- Cột dùng cho incremental (ví dụ: ModifiedDate)
    LastWatermark   DATETIME2,             -- Giá trị watermark lần chạy trước
    IsEnabled       BIT DEFAULT 1
);

-- Ví dụ dữ liệu mẫu
INSERT INTO control.etl_config VALUES
  ('orders',    'SELECT * FROM src.orders',    'gold.fact_orders',    'Incremental', 'ModifiedDate', '2026-08-01', 1),
  ('customers', 'SELECT * FROM src.customers', 'gold.dim_customer',   'Full',        NULL,           NULL,         1),
  ('products',  'SELECT * FROM src.products',  'gold.dim_product',    'Full',        NULL,           NULL,         1);
```

**Bước 2: Cấu trúc pipeline**

```text
Pipeline: MetadataDriven_ETL
│
├── [1] Lookup: Lấy danh sách entities IsEnabled=1
│       Query: SELECT * FROM control.etl_config WHERE IsEnabled = 1
│
├── [2] ForEach: @activity('Lookup').output.value
│       Sequential: false
│       Batch Count: 4 đến 5  (chạy song song tối đa 5)
│       │
│       └── [2a] If Condition: @equals(item().LoadType, 'Full')
│               ├── TRUE (Full Load):
│               │     Copy Activity: overwrite target table
│               │
│               └── FALSE (Incremental Load):
│                     [2b] Lookup watermark từ control table
│                     [2c] Copy staging với filter WHERE @item().WatermarkColumn > @watermark
│                     [2d] Notebook MERGE staging → target
│                     [2e] Update LastWatermark trong control table (CHỈ sau khi success)
│
└── [3] On Failure → Fail Activity với message:
          "ETL failed for entity: @{item().EntityName}, RunId: @{pipeline().RunId}"
```

**Bước 3: Notebook MERGE (Python/PySpark)**

```python
# Parameter cell — pipeline sẽ ghi đè các giá trị này
entity_name   = "orders"
staging_table = "staging.orders_stg"
target_table  = "gold.fact_orders"
merge_key     = "OrderId"

from delta.tables import DeltaTable
from pyspark.sql import functions as F

staging = spark.table(staging_table)
target  = DeltaTable.forName(spark, target_table)

(target.alias("t")
 .merge(staging.alias("s"), f"t.{merge_key} = s.{merge_key}")
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute())

# Idempotent: rerun MERGE không tạo duplicate
# Watermark chỉ được update ở pipeline NGOÀI notebook này, sau khi MERGE thành công
```

**Điểm then chốt cần nhớ:**

| Nguyên tắc | Giải thích |
|-----------|-----------|
| **Watermark commit sau success** | Không update watermark trong notebook — chỉ update ở pipeline activity sau khi toàn bộ MERGE thành công |
| **Fail Activity với context** | Thông báo lỗi phải chứa entity name và run ID để dễ debug |
| **Idempotent MERGE** | Rerun pipeline vì lỗi không được tạo dữ liệu trùng |
| **Batch Count 4–5** | Cân bằng giữa throughput và resource contention trên capacity |

> [!TIP]
> Tham khảo: [Metadata-driven pipeline tutorial](https://learn.microsoft.com/en-us/fabric/data-factory/tutorial-metadata-driven-pipeline) và [Pipeline expressions](https://learn.microsoft.com/en-us/fabric/data-factory/expression-language)

