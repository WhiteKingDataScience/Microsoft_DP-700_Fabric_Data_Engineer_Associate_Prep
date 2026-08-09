# DP-700: Configure Microsoft Fabric Workspace Settings
## Implement and manage an analytics solution (30–35%)
### Skills measured as of July 21, 2026

> [!IMPORTANT]
> **Bản hiệu chỉnh ngày 04/08/2026.** Nội dung được đối chiếu với blueprint DP-700 có hiệu lực từ **21/07/2026**. Microsoft cho biết phần lớn câu hỏi tập trung vào tính năng GA, nhưng tính năng Preview phổ biến vẫn có thể xuất hiện. Với các bảng “supported items”, runtime, limits và trạng thái Preview/GA, hãy kiểm tra Microsoft Learn lần cuối trước ngày thi.


> **Tài liệu ôn thi DP-700: Fabric Data Engineer Associate**
> Phần này bao gồm toàn bộ kiến thức về cấu hình workspace settings trong Microsoft Fabric.

---

## 📋 MỤC LỤC

1. [Tổng quan về Fabric Workspace](#1-tổng-quan-về-fabric-workspace)
2. [Configure Spark Workspace Settings](#2-configure-spark-workspace-settings)
3. [Configure Domain Workspace Settings](#3-configure-domain-workspace-settings)
4. [Configure OneLake Workspace Settings](#4-configure-onelake-workspace-settings)
5. [Configure Apache Airflow Workspace Settings](#5-configure-apache-airflow-workspace-settings)
6. [Câu hỏi mô phỏng bài thi (Practice Questions)](#6-câu-hỏi-mô-phỏng-bài-thi)

---

## 1. TỔNG QUAN VỀ FABRIC WORKSPACE

### 1.1 Workspace là gì?

Workspace trong Microsoft Fabric là **đơn vị tổ chức cơ bản** (fundamental organizational unit) nơi người dùng tạo, quản lý và cộng tác trên các Fabric items (lakehouses, notebooks, pipelines, semantic models, reports, v.v.).

**Các đặc điểm chính:**
- Workspace hoạt động như một **container logic** cho các items
- Mỗi workspace được liên kết với một **Fabric capacity** (hoặc trial capacity)
- Workspace có thể thuộc về một **domain** để phục vụ quản trị phân quyền (federated governance)
- Workspace settings cho phép quản trị viên cấu hình Spark, OneLake, Airflow, Git, v.v.

### 1.2 Cách truy cập Workspace Settings

1. Mở **Fabric Portal** (app.fabric.microsoft.com)
2. Chọn workspace cần cấu hình
3. Click biểu tượng **Settings (⚙)** ở góc trên bên phải hoặc chọn **Workspace settings** từ menu "..."

### 1.3 Các vai trò (Workspace Roles)

| Vai trò | Quyền hạn chính |
|---------|-----------------|
| **Admin** | Toàn quyền: quản lý settings, thêm/xóa users, quản lý items, xóa workspace |
| **Member** | Thêm members (trừ Admin), tạo/chỉnh sửa/xóa items, publish apps |
| **Contributor** | Tạo/chỉnh sửa/xóa items nhưng KHÔNG thể share hoặc thay đổi permissions |
| **Viewer** | Chỉ xem items, KHÔNG thể chỉnh sửa |

> ⚠️ **Lưu ý thi:** Admin, Member, và Contributor đều có quyền **bypass OneLake security**. OneLake security chủ yếu áp dụng cho **Viewer** role.

---

## 2. CONFIGURE SPARK WORKSPACE SETTINGS

### 2.1 Tổng quan về Spark trong Fabric

Apache Spark trong Fabric cho phép xử lý dữ liệu lớn (big data processing) bằng PySpark, Scala, SQL, và R. Spark settings được quản lý tại:

**Đường dẫn:** `Workspace settings > Data Engineering/Science > Spark settings`

### 2.2 Spark Pools

#### Starter Pool (Mặc định)
- **Mục đích:** Dành cho development và testing nhanh
- **Thời gian khởi tạo:** thường tính bằng giây khi pre-warmed capacity sẵn có; đây là tối ưu best-effort, không phải SLA cố định
- **Session timeout mặc định:** 20 phút và có thể cấu hình ở Spark workspace settings; không nên nhầm session timeout với vòng đời của starter pool
- **Cấu hình:** dùng Medium nodes; autoscale và dynamic executor allocation được bật mặc định, giới hạn phụ thuộc capacity SKU
- Với workload cần thời gian khởi động dự đoán được, cân nhắc **custom live pool** (Preview) thay vì suy luận chỉ từ kích thước dữ liệu

#### Custom Spark Pool
- **Mục đích:** Cho production workloads hoặc khi cần kiểm soát tài nguyên
- **Cấu hình:**
  - **Node Family:** Memory Optimized
  - **Node Size:** Small, Medium, Large, XLarge, XXLarge
  - **Autoscale:** Bật/tắt, cấu hình min/max nodes
  - **Dynamic Allocation:** Tự động điều chỉnh executors dựa trên workload

```
Ví dụ cấu hình Custom Pool:
┌─────────────────────────────────────┐
│  Node Size: Large                   │
│  Min Nodes: 1                       │
│  Max Nodes: 10                      │
│  Autoscale: Enabled                 │
│  Dynamic Allocation: Enabled        │
└─────────────────────────────────────┘
```

### 2.3 Spark Runtime

| Thông số | Mô tả |
|----------|--------|
| **Runtime 1.3** | Runtime GA phổ biến cho workload cần tính tương thích đa trải nghiệm |
| **Runtime 2.0** | Spark 4.1, Delta Lake 4.2, Python 3.13 (**Public Preview**, cập nhật 07/2026) |
| **Default Runtime** | Workspace admin chọn runtime mặc định cho toàn workspace |

> 💡 **Mẹo thi:** Runtime 2.0 (Preview) cung cấp hiệu suất tốt hơn và tương thích Delta Lake mới nhất, nhưng câu hỏi thi thường tập trung vào features GA.

### 2.4 Environment và Libraries

- **Environment:** Là item trong workspace cho phép cấu hình custom Spark settings, libraries, và resources
- **Workspace Default Environment:** Admin có thể đặt một Environment làm mặc định cho tất cả notebooks và Spark jobs trong workspace
- **Library Management:**
  - **Public Libraries:** Cài đặt từ PyPI, Conda
  - **Custom Libraries:** Upload .whl, .jar, .tar.gz files
  - **Inline Installation:** Sử dụng `%pip install` trong notebook (chỉ áp dụng cho session hiện tại)

### 2.5 Spark Properties

Cấu hình các thuộc tính Spark ở cấp workspace:

```python
# Đặt Spark properties hợp lệ trong notebook/session
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Kiểm tra giá trị thực tế
print(spark.conf.get("spark.sql.adaptive.enabled"))
```

### 2.6 High Concurrency Mode

- Cho phép nhiều notebooks tương thích chia sẻ cùng một Spark session
- Session sharing nằm trong **ranh giới cùng một user**, không chia sẻ session giữa các user khác nhau
- Các notebook phải tương thích về workspace, Spark configuration, default lakehouse và libraries
- Giảm thời gian khởi động và tăng mức sử dụng compute cho development hoặc pipeline có nhiều notebook

### 2.7 Ví dụ minh họa thực tế

**Tình huống:** Công ty ABC có team Data Engineering gồm 5 người, cần xử lý 2TB dữ liệu hàng ngày.

**Giải pháp:**
1. Tạo **Custom Spark Pool** với Large nodes, autoscale 2-8 nodes
2. Chọn runtime theo yêu cầu tương thích; ưu tiên runtime GA nếu solution được nhiều Fabric engines cùng đọc
3. Tạo **Environment** với các libraries cần thiết (delta-spark, pandas, pyarrow)
4. Bật **High Concurrency** khi nhiều notebook của cùng user có cấu hình tương thích hoặc khi pipeline chạy chuỗi notebook với session tag
5. Không mặc định tắt High Concurrency ở production; đánh giá isolation, concurrency, session tags và khả năng debug theo workload

---

## 3. CONFIGURE DOMAIN WORKSPACE SETTINGS

### 3.1 Domain là gì?

**Domain** trong Fabric là một **lớp tổ chức logic** (logical organizational layer) giúp nhóm các workspaces theo đơn vị kinh doanh (business unit), phòng ban, hoặc chủ đề.

> ⚠️ **Quan trọng cho thi:** Domain KHÔNG phải là cơ chế kiểm soát truy cập (access control). Gán workspace vào domain KHÔNG tự động cấp quyền truy cập cho users trong domain đó. Quyền truy cập vẫn được quản lý qua workspace roles.

### 3.2 Hệ thống phân cấp vai trò Domain

| Vai trò | Phạm vi | Quyền hạn |
|---------|---------|-----------|
| **Fabric Admin** | Toàn tenant | Tạo/xóa domains, gán domain admins, override workspace assignments |
| **Domain Admin** | Domain cụ thể | Quản lý domain settings, cập nhật mô tả, delegate settings |
| **Domain Contributor** | Workspace | Gán workspace của mình vào domain (phải là workspace admin) |

### 3.3 Cấu hình Domain trong Admin Portal

**Đường dẫn:** `Settings (⚙) > Admin portal > Domains`

**Các bước:**
1. **Tạo Domain:** Đặt tên, mô tả, chọn hình ảnh đại diện
2. **Tạo Subdomain:** Tổ chức domains theo cấu trúc phân cấp
3. **Gán Domain Admins:** Chỉ định người quản lý domain
4. **Gán Domain Contributors:** Cho phép workspace admins tự gán workspace

### 3.4 Tenant Settings cho Domains

| Setting | Mô tả | Mặc định |
|---------|--------|----------|
| **Allow tenant/domain admins to override workspace assignments** | Cho phép admin gán lại workspace đã thuộc domain khác | Enabled |
| **Delegation of tenant settings** | Cho phép delegate một số tenant settings xuống domain level | Tùy setting |
| **Default sensitivity labels** | Delegate label mặc định cho domain | Disabled |

### 3.5 Lợi ích của Domains

1. **OneLake Data Hub:** Users có thể filter content theo domain trong Data Hub
2. **Federated Governance:** Domain admins quản lý phạm vi của mình mà không cần Fabric Admin
3. **Data Discovery:** Dễ dàng tìm kiếm và khám phá dữ liệu theo business unit
4. **Compliance:** Áp dụng chính sách tuân thủ riêng cho từng domain

### 3.6 Gán Workspace vào Domain

**Cách 1: Qua UI**
- Workspace settings > General > Domain > Chọn domain

**Cách 2: Qua REST API**
```http
POST https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/assignToDomain
```

**Cách 3: Qua Admin Portal**
- Admin portal > Domains > Chọn domain > Assign workspaces

### 3.7 Ví dụ minh họa thực tế

**Tình huống:** Tập đoàn XYZ có 3 bộ phận: Finance, Marketing, Operations. Mỗi bộ phận có team data riêng.

**Giải pháp:**
```
Tenant (XYZ Corporation)
├── Domain: Finance
│   ├── Subdomain: Accounting
│   │   ├── Workspace: Finance-DEV
│   │   └── Workspace: Finance-PROD
│   └── Subdomain: Treasury
│       └── Workspace: Treasury-Analytics
├── Domain: Marketing
│   ├── Workspace: Marketing-DEV
│   └── Workspace: Marketing-PROD
└── Domain: Operations
    ├── Workspace: Ops-DataLake
    └── Workspace: Ops-Reporting
```

- **Domain Admin (Finance):** CFO team lead → quản lý tất cả workspaces trong Finance domain
- **Domain Contributor:** Data engineers → có thể gán workspace của họ vào Finance domain
- **Tenant-level label:** "Confidential" → được delegate xuống Finance domain

---

## 4. CONFIGURE ONELAKE WORKSPACE SETTINGS

### 4.1 OneLake là gì?

**OneLake** là lớp lưu trữ thống nhất (unified storage layer) của Microsoft Fabric, dựa trên Azure Data Lake Storage (ADLS) Gen2.

**Đặc điểm chính:**
- **Một data lake cho toàn tổ chức** (one data lake per tenant)
- **Tự động đi kèm** với mỗi Fabric tenant, không cần provision riêng
- **Định dạng mở:** Delta Parquet là định dạng mặc định
- **Tích hợp với ADLS Gen2 APIs** → có thể truy cập qua Azure Storage Explorer, azcopy, v.v.

### 4.2 OneLake Workspace Settings

**Đường dẫn:** `Workspace settings > OneLake`

| Setting | Mô tả | Lưu ý |
|---------|--------|-------|
| **OneLake data access** | Bật/tắt truy cập OneLake qua ADLS Gen2 APIs | Mặc định: Enabled |
| **Allow users to access data stored in OneLake with apps external to Fabric** | Cho phép truy cập qua Azure Storage Explorer, custom apps | Controlled by tenant setting |

### 4.3 OneLake Shortcuts

**Shortcut** là con trỏ ảo (virtual pointer) tới dữ liệu, cho phép truy cập dữ liệu từ nhiều nguồn mà **KHÔNG sao chép dữ liệu**.

#### Các loại Shortcuts:

| Loại | Nguồn | Mô tả |
|------|-------|--------|
| **Internal** | OneLake (cùng/khác workspace) | Trỏ tới folder/table trong Lakehouse/Warehouse khác |
| **ADLS Gen2** | Azure Data Lake Storage Gen2 | Trỏ tới container/folder trong ADLS Gen2 |
| **Amazon S3** | AWS S3 | Trỏ tới S3 bucket/prefix |
| **Google Cloud Storage** | GCS | Trỏ tới GCS bucket |
| **Dataverse** | Microsoft Dataverse | Trỏ tới Dataverse tables |
| **S3 Compatible** | MinIO, Cloudflare R2, v.v. | Trỏ tới S3-compatible storage |

### 4.4 Shortcut Security

| Chế độ | Mô tả |
|--------|--------|
| **Passthrough Authentication** (Mặc định) | Identity của user truy cập được "pass through" tới target → user phải có quyền trên target |
| **Delegated Shortcuts** (Mới - 2026) | Sử dụng fixed credentials cho data distribution scenarios |

> 💡 **Mẹo thi:** Security được set trên target folder sẽ tự động được áp dụng xuyên qua internal shortcuts. Đây là cơ chế **security propagation**.

### 4.5 OneLake Data Access Roles (Workspace Level)

Cấu hình quyền truy cập dữ liệu OneLake ở cấp workspace:

- **AllowAll:** Tất cả users có workspace access đều đọc được tất cả OneLake data
- **RestrictAccess:** Chỉ users được gán explicit OneLake security roles mới đọc được data

### 4.6 OneLake File Explorer

- Ứng dụng Windows cho phép mount OneLake vào File Explorer
- Đồng bộ files giữa local machine và OneLake
- Hỗ trợ drag-and-drop files vào Lakehouse

### 4.7 Ví dụ minh họa thực tế

**Tình huống:** Team Analytics cần truy cập dữ liệu từ:
1. Lakehouse trong workspace khác (Finance)
2. Azure Data Lake Storage Gen2 (historical data)
3. Amazon S3 (partner data)

**Giải pháp:**
```
Lakehouse: Analytics-Lakehouse
├── Tables/
│   ├── dim_customer          (Delta table, native)
│   ├── dim_product           (Delta table, native)
│   └── fact_sales            (Delta table, native)
├── Files/
│   └── raw_data/
└── Shortcuts/
    ├── finance_data/         → Internal shortcut → Finance-Lakehouse/Tables/
    ├── historical_archive/   → ADLS Gen2 shortcut → storageaccount/archive/
    └── partner_feeds/        → S3 shortcut → s3://partner-bucket/feeds/
```

---

## 5. CONFIGURE APACHE AIRFLOW WORKSPACE SETTINGS

### 5.1 Apache Airflow trong Fabric là gì?

Apache Airflow trong Fabric là dịch vụ **managed orchestration** cho phép tạo, lập lịch và giám sát workflows (DAGs - Directed Acyclic Graphs) mà không cần quản lý infrastructure.

### 5.2 Workspace Settings cho Airflow

**Đường dẫn:** `Workspace settings > Data Factory > Apache Airflow Runtime Settings`

### 5.3 Pool Options

#### Starter Pool (Mặc định)
| Thuộc tính | Giá trị |
|-----------|---------|
| **Availability** | Instantaneous runtime |
| **Auto-deprovisioning** | Sau 20 phút không hoạt động |
| **Use case** | Development và testing |
| **Cost** | Pay-per-use, tiết kiệm khi không sử dụng |
| **Customization** | Không thể tùy chỉnh node size |

#### Custom Pool
| Thuộc tính | Giá trị |
|-----------|---------|
| **Availability** | Always-on cho đến khi manually paused |
| **Configuration** | Tùy chỉnh node size, extra nodes, autoscaling |
| **Use case** | Production workloads, long-running DAGs |
| **Cost** | Cao hơn (luôn chạy) |

### 5.4 Authentication - Workspace Identity

- **Workspace Identity** là managed identity cho phép Airflow DAGs authenticate tới Fabric services
- **Không cần** quản lý connection strings hoặc Service Principal thủ công
- **Cấu hình:** `Settings > Manage connections and gateways`

**Các services hỗ trợ Workspace Identity:**
- Lakehouse
- Notebooks  
- Pipelines
- Warehouse
- KQL Database

### 5.5 Variable Libraries

- Cho phép định nghĩa **nhiều bộ giá trị** (value sets) cho các biến
- Đánh dấu một bộ là **active** cho mỗi workspace
- **Lợi ích:** Thay đổi cấu hình giữa các môi trường (DEV/TEST/PROD) mà KHÔNG cần sửa DAG code

```python
# Ví dụ sử dụng Variable trong DAG
from airflow.models import Variable

# Giá trị này thay đổi tùy environment (DEV/PROD) nhờ Variable Libraries
storage_path = Variable.get("data_storage_path")
database_name = Variable.get("target_database")
```

### 5.6 DAG Authoring

- DAGs được viết bằng Python
- Lưu trữ trong thư mục `dags/` của Airflow item
- Hỗ trợ custom Python packages qua `requirements.txt`

### 5.7 Giới hạn hiện tại (Tính đến mid-2026)

> ⚠️ **Lưu ý:** Apache Airflow jobs trong Fabric hiện **KHÔNG hỗ trợ private networks hoặc virtual networks**. Feature này đang được phát triển.

### 5.8 Ví dụ minh họa thực tế

**Tình huống:** Team Data Engineering cần orchestrate pipeline ETL hàng ngày.

**DAG ví dụ:**
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'daily_etl_pipeline',
    default_args=default_args,
    schedule_interval='0 6 * * *',  # Chạy lúc 6 AM hàng ngày
    catchup=False,
) as dag:
    
    def extract_data():
        # Logic extract từ source
        pass
    
    def transform_data():
        # Logic transform
        pass
    
    def load_data():
        # Logic load vào Lakehouse
        pass
    
    extract = PythonOperator(task_id='extract', python_callable=extract_data)
    transform = PythonOperator(task_id='transform', python_callable=transform_data)
    load = PythonOperator(task_id='load', python_callable=load_data)
    
    extract >> transform >> load
```

---

## 6. CÂU HỎI MÔ PHỎNG BÀI THI

### Question 1

**You are configuring a Microsoft Fabric workspace for a data engineering team. The team needs Spark sessions to start within 5 seconds for interactive development. What should you configure?**

A. Create a Custom Spark Pool with Small nodes  
B. Enable the Starter Pool as the default pool  
C. Configure a Custom Spark Pool with autoscale enabled  
D. Create a new Environment with pre-loaded libraries  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: B**

**Giải thích:**
- **Starter Pool** được thiết kế để cung cấp **khởi tạo session nhanh (~5 giây)** vì nó sử dụng pre-warmed resources
- **Custom Spark Pool** (A, C) có thời gian khởi tạo lâu hơn vì cần provision resources
- **Environment** (D) là nơi quản lý libraries, không ảnh hưởng đến tốc độ khởi tạo session
- Starter Pool là lựa chọn tối ưu cho **development và testing** khi tốc độ khởi tạo là ưu tiên

</details>

---

### Question 2

**Your organization has a Microsoft Fabric tenant with three business units: Sales, HR, and IT. You need to organize workspaces so that each business unit's data team can manage their own governance policies without requiring Fabric Admin access. What should you implement?**

A. Create separate Fabric capacities for each business unit  
B. Create domains and assign Domain Admin roles to each business unit lead  
C. Create separate security groups and assign workspace Admin roles  
D. Implement row-level security across all shared workspaces  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: B**

**Giải thích:**
- **Domains** cho phép **federated governance** - mỗi Domain Admin quản lý phạm vi của mình mà KHÔNG cần Fabric Admin access
- **Separate capacities** (A) giúp isolate resources nhưng không giải quyết governance delegation
- **Security groups** (C) chỉ quản lý access, không delegate governance policies
- **RLS** (D) là data-level security, không liên quan đến workspace organization
- Domain là giải pháp đúng vì nó được thiết kế chính xác cho use case "**business unit-level governance without Fabric Admin**"

</details>

---

### Question 3

**You have data stored in an Amazon S3 bucket that needs to be accessed from a Fabric Lakehouse. The data should NOT be copied into OneLake. What should you create?**

A. A data pipeline with a Copy activity  
B. An ADLS Gen2 shortcut  
C. An Amazon S3 shortcut  
D. A Dataflow Gen2 with S3 connector  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: C**

**Giải thích:**
- **S3 shortcut** tạo virtual pointer tới S3 bucket **mà không sao chép dữ liệu** (zero-copy)
- **Copy activity** (A) sẽ sao chép dữ liệu vào OneLake → vi phạm yêu cầu "should NOT be copied"
- **ADLS Gen2 shortcut** (B) chỉ dành cho Azure Data Lake Storage Gen2, không phải S3
- **Dataflow Gen2** (D) cũng sẽ extract và load dữ liệu → tạo bản sao
- Shortcuts là tính năng chính của OneLake cho phép truy cập dữ liệu multi-cloud mà không di chuyển dữ liệu

</details>

---

### Question 4

**You need to configure Apache Airflow in Microsoft Fabric for a production ETL pipeline that runs every 4 hours. The pipeline must be highly available. Which pool type should you use?**

A. Starter Pool with auto-deprovisioning disabled  
B. Custom Pool with always-on availability  
C. Starter Pool with increased timeout  
D. Default Spark Pool  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: B**

**Giải thích:**
- **Custom Pool** cung cấp **always-on availability** cho đến khi manually paused → đảm bảo high availability cho production
- **Starter Pool** (A, C) tự động de-provision sau 20 phút không hoạt động, KHÔNG thể disable auto-deprovisioning hoặc tăng timeout → không phù hợp cho production
- **Default Spark Pool** (D) là Spark compute, không phải Airflow runtime
- Với pipeline chạy mỗi 4 giờ, Custom Pool đảm bảo runtime luôn sẵn sàng khi DAG cần chạy

</details>

---

### Question 5

**A workspace admin assigns a workspace to the "Finance" domain. Which statement is TRUE about the users who have the Domain Contributor role for the "Finance" domain?**

A. They automatically gain Viewer access to all workspaces in the Finance domain  
B. They can access the Domains tab in the Admin portal  
C. They can assign their own workspaces to the Finance domain but cannot access the Domains tab in the Admin portal  
D. They can modify the domain settings including description and image  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: C**

**Giải thích:**
- **Domain Contributor** có quyền gán workspace (mà họ là admin) vào domain, nhưng **KHÔNG có quyền truy cập tab Domains** trong Admin portal
- (A) Sai - Domain KHÔNG phải là cơ chế access control, không tự động cấp quyền truy cập workspace
- (B) Sai - Chỉ Fabric Admin và Domain Admin mới truy cập được Domains tab
- (D) Sai - Đây là quyền của Domain Admin, không phải Domain Contributor
- Điểm mấu chốt: Domain Contributor = workspace admin được phép tự gán workspace vào domain

</details>

---

### Question 6

**You are setting up a development environment for a data engineering team. Multiple data engineers need to work on separate notebooks simultaneously within the same workspace. They want to share the same Spark session to reduce startup times. What feature should you enable?**

A. Workspace Identity  
B. Spark Autoscale  
C. High Concurrency Mode  
D. Dynamic Allocation  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: C**

**Giải thích:**
- **High Concurrency Mode** cho phép nhiều notebooks chia sẻ cùng một Spark session → giảm thời gian khởi động
- **Workspace Identity** (A) là managed identity cho authentication, không liên quan đến session sharing
- **Autoscale** (B) tự động điều chỉnh số lượng nodes, không phải session sharing
- **Dynamic Allocation** (D) tự động điều chỉnh executors, không phải session sharing
- High Concurrency đặc biệt hữu ích cho development environment khi nhiều engineers cần interactive notebooks

</details>

---

### Question 7

**You need to ensure that users can access OneLake data from Azure Storage Explorer. Which workspace setting must be enabled?**

A. OneLake data access  
B. Allow users to access data stored in OneLake with apps external to Fabric  
C. Workspace Identity  
D. ADLS Gen2 compatibility mode  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: B**

**Giải thích:**
- Setting "**Allow users to access data stored in OneLake with apps external to Fabric**" phải được enabled để cho phép truy cập qua Azure Storage Explorer, custom apps, và các công cụ external khác
- **OneLake data access** (A) bật/tắt ADLS Gen2 API access nhưng không đủ - cần setting cụ thể cho external apps
- **Workspace Identity** (C) là managed identity, không liên quan
- **ADLS Gen2 compatibility mode** (D) không phải là setting thực trong Fabric
- Lưu ý: Setting này cũng có thể được controlled ở **tenant level** bởi Fabric Admin

</details>

---

### Question 8

**You are configuring Spark settings for a workspace. You need to ensure that all notebooks in the workspace use a specific set of Python libraries by default. What should you do?**

A. Install libraries using %pip install in each notebook  
B. Create an Environment item with the required libraries and set it as the workspace default  
C. Add the libraries to the Starter Pool configuration  
D. Configure the libraries in the Spark Properties section  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: B**

**Giải thích:**
- **Environment item** cho phép cài đặt libraries (public từ PyPI/Conda hoặc custom .whl/.jar) và khi set làm **workspace default**, TẤT CẢ notebooks sẽ sử dụng libraries đó
- `%pip install` (A) chỉ cài đặt cho **session hiện tại** của một notebook, không persistent
- **Starter Pool** (C) không cho phép tùy chỉnh library configuration
- **Spark Properties** (D) dùng để cấu hình Spark runtime parameters, không phải library management
- Workflow đúng: Tạo Environment → Cài libraries → Set workspace default → Publish

</details>

---

### Question 9

**Your organization uses Microsoft Fabric. A tenant admin wants to allow domain admins to set default sensitivity labels for items within their own domains, rather than applying a single policy tenant-wide. What should the tenant admin do?**

A. Create a separate capacity for each domain  
B. Delegate the sensitivity label tenant setting to the domain level  
C. Assign the Fabric Admin role to each domain admin  
D. Configure Microsoft Purview at the workspace level  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: B**

**Giải thích:**
- Fabric cho phép **delegation** của một số tenant-level settings xuống domain level, bao gồm default sensitivity labels
- **Separate capacities** (A) liên quan đến compute isolation, không phải governance delegation
- **Fabric Admin role** (C) quá nhiều quyền, vi phạm principle of least privilege
- **Microsoft Purview at workspace level** (D) không phải cách cấu hình đúng trong Fabric
- Delegation cho phép domain admins tùy chỉnh policies cho business unit của họ mà vẫn tuân thủ framework chung

</details>

---

### Question 10

**You are using OneLake shortcuts to access data from an ADLS Gen2 storage account. A user reports that they can see the shortcut in the Lakehouse but cannot read the data. What is the MOST likely cause?**

A. The shortcut is configured with the wrong file format  
B. The user does not have the required permissions on the target ADLS Gen2 storage account (passthrough authentication)  
C. The OneLake data access setting is disabled for the workspace  
D. The user needs to be added as a Domain Contributor  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: B**

**Giải thích:**
- Với **Passthrough Authentication** (mặc định cho hầu hết shortcuts), identity của user được "pass through" tới target. User **phải có quyền trên target ADLS Gen2** để đọc dữ liệu
- **File format** (A) không ảnh hưởng đến khả năng truy cập
- **OneLake data access** (C) ảnh hưởng đến external API access, user vẫn thấy shortcut trong Lakehouse
- **Domain Contributor** (D) liên quan đến domain assignment, không phải data access
- Đây là vấn đề phổ biến: user có workspace access nhưng thiếu permissions trên nguồn external → cần GRANT quyền trên ADLS Gen2

</details>

---

## 📝 TÓM TẮT KIẾN THỨC CẦN NHỚ

| Chủ đề | Điểm then chốt |
|--------|----------------|
| **Spark Pools** | Starter (~5s start) vs Custom (configurable, production) |
| **Spark Runtime** | Runtime GA cho tương thích; Runtime 2.0 = Spark 4.1/Delta 4.2 Preview (07/2026) |
| **Environment** | Quản lý libraries, set workspace default |
| **Domain** | Organizational layer, NOT access control |
| **Domain Roles** | Admin > Domain Admin > Domain Contributor |
| **OneLake** | Unified storage, Delta Parquet format |
| **Shortcuts** | Virtual pointers, zero-copy, multi-cloud |
| **Shortcut Auth** | Passthrough (default) vs Delegated |
| **Airflow Pools** | Starter (20min timeout) vs Custom (always-on) |
| **Airflow Identity** | Workspace Identity cho managed authentication |
| **Variable Libraries** | Multiple value sets cho multi-environment config |

---

> 📖 **Tài liệu tham khảo chính thức:**
> - [Microsoft Fabric Documentation](https://learn.microsoft.com/en-us/fabric/)
> - [DP-700 Study Guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)
> - [OneLake Documentation](https://learn.microsoft.com/en-us/fabric/onelake/)
> - [Apache Spark in Fabric](https://learn.microsoft.com/en-us/fabric/data-engineering/)


---

## 7. BỔ SUNG ÔN THI 2026: SPARK SETTINGS VÀ QUYẾT ĐỊNH KIẾN TRÚC

### 7.1 Những bẫy dễ gặp

- Starter pool nhanh nhờ pre-warmed capacity nhưng không bảo đảm mọi session đều khởi động trong đúng 5 giây.
- High concurrency không có nghĩa nhiều người dùng chia sẻ một session; session sharing bị giới hạn theo user và các notebook phải tương thích.
- Runtime mới nhất không tự động là lựa chọn tốt nhất. Runtime 2.0 vẫn Preview và Delta 4.x features có thể không interoperable với mọi Fabric workload.
- Chọn pool dựa trên SLA khởi động, concurrency, libraries, runtime và resource profile; không chọn chỉ dựa trên “2 TB/ngày”.

### 7.2 Lab nhanh

```python
# Kiểm tra cấu hình session đang chạy
keys = [
    "spark.sql.adaptive.enabled",
    "spark.sql.shuffle.partitions",
    "spark.sql.parquet.vorder.default",
]
for key in keys:
    print(key, spark.conf.get(key, "<not set>"))
```

### 7.3 Câu hỏi tình huống bổ sung

**Một pipeline chạy ba notebook liên tiếp. Các notebook cùng workspace, cùng default lakehouse và libraries. Mục tiêu là giảm thời gian tạo session nhưng vẫn tách log theo notebook. Chọn giải pháp nào?**

A. Tạo ba custom pools khác nhau  
B. Bật high concurrency cho notebook trong pipeline và dùng cùng session tag  
C. Dùng Viewer role cho pipeline identity  
D. Tăng `spark.sql.shuffle.partitions`

<details><summary>Đáp án</summary>

**B.** High concurrency + session tag cho phép các notebook tương thích trong pipeline tái sử dụng session. `shuffle.partitions` không xử lý startup latency.
</details>


---

## NGUỒN CHÍNH THỨC ĐÃ ĐỐI CHIẾU

- [DP-700 study guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)
- [Spark starter pools](https://learn.microsoft.com/en-us/fabric/data-engineering/configure-starter-pools)
- [Runtime 2.0](https://learn.microsoft.com/en-us/fabric/data-engineering/runtime-2-0)
- [High concurrency notebooks](https://learn.microsoft.com/en-us/fabric/data-engineering/configure-high-concurrency-session-notebooks)
- [Airflow workspace settings](https://learn.microsoft.com/en-us/fabric/data-factory/apache-airflow-jobs-workspace-settings)

---

## 8. CÂU HỎI SCENARIO — WORKSPACE SETTINGS

> Các câu hỏi dưới đây là câu tự soạn theo blueprint DP-700, **không phải câu hỏi thật hoặc exam dump**.

### Câu D1-W1 — Custom Pool vs Starter Pool

**Một workload định kỳ chạy vào khung 02:00–04:00 mỗi ngày và cần Environment có nhiều thư viện wheel (.whl). Compute nào phù hợp nhất?**

A. Starter pool (mặc định)  
B. Custom live pool với active window được đặt vào khung 01:45–04:15  
C. High concurrency giữa nhiều người dùng khác nhau  
D. Airflow starter pool  

<details><summary>Đáp án và giải thích</summary>

**B.** [Custom live pool](https://learn.microsoft.com/en-us/fabric/data-engineering/create-custom-spark-pools) (Preview) cho phép **pre-warm compute và tải trước thư viện** trong active window đã đặt trước; starter pool là best-effort và không đảm bảo startup time. Airflow starter pool là pool riêng của Airflow, không dùng cho Spark notebook/job.

</details>

---

### Câu D1-W2 — High Concurrency Boundary

**Hai notebook trong cùng workspace, nhưng do hai người dùng khác nhau chạy. High Concurrency có chia sẻ cùng một Spark session cho hai notebook này không?**

A. Có, vì cùng workspace  
B. Có, nếu cùng default lakehouse  
C. Không — session sharing trong High Concurrency bị giới hạn theo cùng một user  
D. Chỉ khi cả hai user đều là Admin  

<details><summary>Đáp án và giải thích</summary>

**C.** [High Concurrency mode](https://learn.microsoft.com/en-us/fabric/data-engineering/configure-high-concurrency-session-notebooks) cho phép nhiều notebook **của cùng một user** chia sẻ session, không chia sẻ qua ranh giới người dùng khác. Đây là điểm bẫy thường gặp trong đề thi.

</details>

---

### Câu D1-W3 — Runtime Interoperability

**Solution sử dụng Runtime 2.0 với Delta 4.2 feature experimental, nhưng Warehouse và Direct Lake cũng cần đọc cùng table đó. Hành động tốt nhất là gì?**

A. Bật ngay vì đây là runtime mới nhất  
B. Kiểm tra khả năng tương thích (interoperability) và thử trên bản sao trước  
C. Chạy `VACUUM 0 HOURS` để reset  
D. Chuyển table sang CSV để tránh vấn đề  

<details><summary>Đáp án và giải thích</summary>

**B.** Các tính năng Delta 4.x-specific (như liquid clustering nâng cao) có thể chỉ hoạt động ở Spark engine; [Warehouse, Direct Lake, Power BI](https://learn.microsoft.com/en-us/fabric/data-engineering/runtime-2-0) và các engine khác cần đọc phải hỗ trợ format đó. Luôn kiểm tra interoperability trên môi trường test trước khi áp dụng production.

</details>

---

## 9. KẾ HOẠCH 14 NGÀY VÀ ĐÁNH GIÁ SẴN SÀNG

> Phần này tổng hợp từ **DP-700 Readiness Assessment** (đánh giá ngày 04/08/2026, blueprint 21/07/2026).

### 9.1 Đánh Giá Mức Sẵn Sàng

| Tiêu chí | Sau cập nhật | Nhận xét |
|---|---:|---|
| Khớp blueprint | 98/100 | Đủ toàn bộ nhóm kỹ năng chính |
| Tính chính xác/currentness | 92/100 | Đã sửa facts thay đổi nhanh và overclaim |
| SQL/PySpark/KQL | 90/100 | Code hợp lệ và decision context |
| Hands-on readiness | 86/100 | Cần tự chạy lab trong tenant Fabric |
| Câu hỏi scenario | 90/100 | Trade-off, least privilege, troubleshooting |
| Exam strategy | 91/100 | Checklist và kế hoạch 14 ngày |

> [!IMPORTANT]
> Đạt ≥80% ổn định trên ba lần liên tiếp mock exam + làm được lab thực tế → khoảng **88–92/100 mức sẵn sàng**. Điểm thi là **scaled score**, cần **700 trở lên** để đạt.

### 9.2 Kế Hoạch 14 Ngày

#### Ngày 1–2: Domain 1 — Workspace, CI/CD, Security

- Tạo workspace Dev/Test, xem Spark settings, Environment, starter/custom pool.
- Kết nối workspace test với [Git](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/intro-to-git-integration) và thực hiện commit/update/conflict.
- Tạo deployment pipeline; phân biệt paired/unpaired items.
- Tạo Viewer/item access và OneLake role; kiểm tra khác biệt với Contributor.

#### Ngày 3–5: Domain 2 — Batch và Loading

- Full load bằng `CREATE OR REPLACE` hoặc overwrite.
- Incremental load với control watermark và `MERGE`.
- SCD Type 1/2, dedup, missing/late-arriving dimension.
- So sánh Lakehouse, Warehouse, Eventhouse, SQL Database bằng 10 scenarios.

#### Ngày 6–7: Domain 2 — Streaming

- Eventstream → Eventhouse/Lakehouse.
- KQL `where`, `project`, `summarize`, `bin`, `join`, materialized view/update policy.
- Spark Structured Streaming với checkpoint, watermark, window, output mode.
- Standard shortcut vs query acceleration vs native table.

#### Ngày 8–9: Domain 3 — Monitor và Troubleshoot

- [Monitoring Hub](https://learn.microsoft.com/en-us/fabric/admin/monitoring-hub): pipeline, notebook, Dataflow run.
- [Capacity Metrics app](https://learn.microsoft.com/en-us/fabric/enterprise/metrics-app): identify overload, top operations, smoothing/throttling.
- Activator alert trên Job events hoặc KQL query.
- Thực hành 5 lỗi: credential, schema drift, gateway, OOM/skew, shortcut permission.

#### Ngày 10–11: Optimization

- Tạo small files, chạy `OPTIMIZE`/V-Order/`VACUUM` trên dữ liệu lab.
- So sánh partitioning, Z-Order và liquid clustering bằng physical plan/file count.
- Tune Copy activity bằng partitioning/parallelism/throughput option.
- KQL filter early, hot cache; Spark broadcast/AQE/skew.

#### Ngày 12–13: Mock Exam

- Làm 2 lượt 50 câu scenario, giới hạn thời gian 90–100 phút.
- Ghi error log theo blueprint bullet, không chỉ theo tên file.
- Mọi câu sai phải viết lại: requirement → rejected options → correct trade-off.

#### Ngày 14: Final Review

- Đọc lại bảng "bẫy thi" và các decision matrix trong từng file domain.
- Làm [Microsoft Practice Assessment](https://learn.microsoft.com/en-us/credentials/certifications/fabric-data-engineer-associate/practice/assessment) và Exam Sandbox.
- Chỉ đặt lịch khi đạt ≥80% ổn định và giải thích được vì sao ba đáp án còn lại sai.

### 9.3 Mẹo Làm Bài Trong Phòng Thi

- Đọc **câu cuối câu hỏi trước** để biết họ hỏi **BEST**, **MINIMUM**, **FIRST** hay **MOST likely**.
- Gạch nhanh yêu cầu quan trọng: latency, no-copy, least privilege, low-code/pro-code, environment promotion, retry/transient, cost, compatibility.
- Không chọn công cụ vì "mới nhất"; chọn vì nó đáp ứng ràng buộc với ít vận hành/rủi ro nhất.
- Với security → xác định workspace role **trước** rồi mới suy luận RLS/CLS/item permissions.
- Với data movement → phân biệt **copy**, **transform**, **orchestrate**.
- Với monitoring → phân biệt **operation status** (Monitoring Hub) và **capacity resource health** (Capacity Metrics).
- Với errors → retry chỉ cho **transient**; credential/schema/permission phải sửa nguyên nhân gốc.
- Đánh dấu câu dài để quay lại, không tiêu quá 2–3 phút ở lượt đầu.

### 9.4 Ngưỡng "Đủ Để Thi"

> [!WARNING]
> Nên hoãn thi nếu vẫn chưa làm được **độc lập** các việc sau:

- Viết PySpark `MERGE`, dedup và Structured Streaming checkpoint/watermark.
- Viết KQL filter/project/summarize/bin/join và giải thích native vs shortcut vs acceleration.
- Chọn Dataflow Gen2/Pipeline/Notebook theo scenario cụ thể.
- Giải thích Git integration vs deployment pipeline và permission model.
- Giải thích OneLake security precedence và bypass pattern.
- Chẩn đoán pipeline/notebook/capacity từ đúng monitoring surface.
- Chọn OPTIMIZE/VACUUM/V-Order/Z-Order/liquid clustering đúng mục đích.

### 9.5 Nguồn Học Chính Thức

| Tài nguyên | Link |
|-----------|------|
| DP-700 Study Guide | [learn.microsoft.com](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700) |
| Fabric Data Engineer Associate | [Certification page](https://learn.microsoft.com/en-us/credentials/certifications/fabric-data-engineer-associate/) |
| Khóa học DP-700T00 | [Training course](https://learn.microsoft.com/en-us/training/courses/dp-700t00) |
| Practice Assessment | [Free practice test](https://learn.microsoft.com/en-us/credentials/certifications/fabric-data-engineer-associate/practice/assessment) |
| Exam Sandbox | [aka.ms/examdemo](https://aka.ms/examdemo) |

