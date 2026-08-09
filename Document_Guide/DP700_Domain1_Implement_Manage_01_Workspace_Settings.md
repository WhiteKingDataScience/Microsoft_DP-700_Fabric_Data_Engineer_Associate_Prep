# DP-700: Configure Microsoft Fabric Workspace Settings
## Implement and manage an analytics solution (30–35%)
### Skills measured as of July 21, 2026

> [!IMPORTANT]
> **Bản hiệu chỉnh ngày 09/08/2026.** Nội dung được đối chiếu với blueprint DP-700 có hiệu lực từ **21/07/2026**. Microsoft cho biết phần lớn câu hỏi tập trung vào tính năng GA, nhưng tính năng Preview phổ biến vẫn có thể xuất hiện. Với các bảng “supported items”, runtime, limits và trạng thái Preview/GA, hãy kiểm tra Microsoft Learn lần cuối trước ngày thi.


> [!CAUTION]
> **Không có tài liệu nào có thể bảo đảm “trúng mọi câu hỏi”.** Microsoft nêu rõ danh sách skills là minh họa và đề có thể hỏi các chủ đề liên quan. Mục tiêu của bản này là: phủ đủ blueprint hiện hành, sửa các facts đã thay đổi, và luyện cách chọn đáp án theo **scenario + constraint + least privilege/least operations**.

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

### 4.1 Học đúng scope: Workspace setting khác Tenant setting

Đây là bẫy rất quan trọng. Khi câu hỏi nói **Workspace settings > OneLake**, hãy nghĩ đến các cấu hình lưu trữ/vận hành của OneLake ở **workspace**. Khi câu hỏi nói người dùng có được truy cập OneLake bằng ứng dụng ngoài Fabric hay không, đó là **tenant setting** trong Admin portal, không phải một công tắc riêng của workspace.

### 4.2 Các OneLake workspace settings cần biết (cập nhật 08/2026)

| Nhóm setting | Ý nghĩa dễ hiểu | Điểm thi cần nhớ |
|---|---|---|
| **Lifecycle management** | Tự động chuyển file giữa storage tier theo tuổi hoặc mức độ truy cập | `Workspace settings > OneLake > Lifecycle management`; mỗi workspace có một policy, tối đa 10 rules |
| **Default storage tier** | Tier mặc định của dữ liệu OneLake trong workspace | Hiện có **Hot, Cool, Cold**; chọn theo access pattern và chi phí |
| **Diagnostics** | Ghi OneLake diagnostic logs sang Lakehouse đích | Dùng cho điều tra/troubleshooting; khác Microsoft 365/Purview audit log |
| **Immutability policy** | Giữ diagnostic logs không bị thay đổi trong retention period | Hiện phạm vi hỗ trợ chủ yếu là **DiagnosticLogs** |

### 4.3 OneLake Lifecycle Management — hiểu bằng ví dụ

Giả sử workspace có dữ liệu lịch sử ít được đọc:

- File mới và thường xuyên truy cập → **Hot**.
- Sau 30 ngày không sửa → có thể chuyển sang **Cool**.
- Sau 90 ngày không sửa/truy cập → có thể chuyển sang **Cold**.
- Rule có thể dựa trên **created time**, **last modified**, hoặc **last accessed**.
- Policy cố gắng chạy theo chu kỳ hằng ngày; rule mới/cập nhật có thể cần đến khoảng 24 giờ mới có hiệu lực.

> [!IMPORTANT]
> Lifecycle management hiện là **tiering**, không nên học theo kiểu “rule sẽ tự xóa file” như Azure Storage lifecycle cũ. Khi đề hỏi giảm chi phí lưu trữ dữ liệu ít truy cập nhưng vẫn giữ dữ liệu, hãy nghĩ đến **Cool/Cold tier**.

### 4.4 Tenant settings liên quan OneLake — đừng nhầm với workspace settings

Các setting như **Users can access data stored in OneLake with apps external to Fabric** được cấu hình ở **Admin portal > Tenant settings > OneLake**. Chúng kiểm soát khả năng dùng ADLS APIs/custom apps/OneLake File Explorer từ ngoài Fabric.

Ngoài ra, OneLake tenant settings hiện còn có các tùy chọn về **OneLake File Explorer**, **user-delegated SAS tokens** và việc ghi end-user identifiers vào diagnostic logs. Không học thuộc từng toggle; hãy nhớ **scope**: tenant policy quyết định khả năng chung của tổ chức, workspace policy quyết định cách workspace đó lưu trữ/ghi diagnostics/tiering.

### 4.5 OneLake Shortcuts — kiến thức liên quan, nhưng không phải trọng tâm của skill “workspace settings”

**Shortcut** là con trỏ tới dữ liệu ở nơi khác để tránh sao chép. Đây là kiến thức quan trọng của DP-700 nhưng chủ yếu xuất hiện trong phần ingest/manage data. Với Domain 1, chỉ cần nối nó với hai quyết định:

1. **Need zero-copy / minimize data movement** → cân nhắc Shortcut.
2. **Need to change workspace storage lifecycle/diagnostics/default tier** → đây mới là OneLake workspace settings.

> 💡 **Mẹo thi:** Nếu đáp án nói “enable external app access trong Workspace settings > OneLake”, hãy cảnh giác. External-app access là tenant setting; lifecycle/diagnostics/default tier mới là các cấu hình workspace OneLake hiện hành.

## 5. CONFIGURE APACHE AIRFLOW WORKSPACE SETTINGS

### 5.1 Apache Airflow Job trong Fabric là gì?

Fabric cung cấp **Apache Airflow Job** để chạy DAG (Directed Acyclic Graph) mà không phải tự dựng Airflow infrastructure. Blueprint DP-700 hiện yêu cầu biết **configure Apache Airflow workspace settings**.

**Đường dẫn chính:** `Workspace settings > Data Factory > Apache Airflow Runtime Settings`

### 5.2 Hai loại pool phải phân biệt

| Tiêu chí | Starter pool | Custom pool |
|---|---|---|
| Mục tiêu | Dev/test, workload chạy không thường xuyên | Production, scheduler cần sẵn sàng liên tục |
| Startup | Nhanh khi runtime sẵn | Pool giữ trạng thái chạy |
| Idle behavior | Tự deprovision sau khoảng 20 phút không hoạt động | **Always-on** đến khi pause thủ công |
| Tùy chỉnh | Ít | Node size, extra nodes, autoscale |
| Chi phí | Tốt cho workload gián đoạn | Trả CU trong thời gian pool chạy |

**Cách suy luận:**
- “Runs occasionally / minimize idle cost” → **Starter pool**.
- “Production / scheduler 24x7 / predictable readiness” → **Custom pool**.
- Custom pool không có nghĩa lúc nào cũng phải chọn node lớn; chọn node/extra nodes/autoscale theo concurrency và capacity.

### 5.3 Setting “customize compute configurations for items”

Nếu workspace admin **không cho phép item tự tùy biến compute**, các Airflow environments dùng Starter pool/default configuration của workspace. Nếu cho phép, item có thể dùng custom compute phù hợp hơn.

### 5.4 Network limitation cần biết

Tính đến 09/08/2026, Microsoft vẫn ghi rõ **private networks và virtual networks chưa được hỗ trợ cho Fabric Apache Airflow jobs**. Nếu scenario bắt buộc private/VNet connectivity, không được giả định Fabric Airflow hiện đáp ứng trực tiếp yêu cầu đó.

### 5.5 Đừng trộn các khái niệm

- **Airflow Variable** (`airflow.models.Variable`) là biến của Airflow.
- **Fabric Variable library** là Fabric item dùng chia sẻ configuration/value sets giữa pipelines, notebooks, shortcuts và CI/CD.
- **Workspace identity** là identity phục vụ authentication ở các tính năng hỗ trợ; nó **không phải** là “Airflow pool setting”.

### 5.6 Mini scenario

**Yêu cầu:** DAG chạy mỗi 4 giờ nhưng đội vận hành yêu cầu scheduler luôn sẵn sàng, không chờ resume runtime.

→ Chọn **Custom pool**. Nếu yêu cầu ngược lại là workload chỉ chạy 1–2 lần/ngày và ưu tiên tiết kiệm idle cost, Starter pool có thể phù hợp hơn.

## 6. CÂU HỎI MÔ PHỎNG BÀI THI

### Question 1

**Bạn cần cấu hình Fabric Spark cho interactive development và muốn thời gian khởi tạo session nhanh nhất trong các lựa chọn chuẩn, không yêu cầu SLA cố định. Nên chọn gì?**

A. Create a Custom Spark Pool with Small nodes  
B. Enable the Starter Pool as the default pool  
C. Configure a Custom Spark Pool with autoscale enabled  
D. Create a new Environment with pre-loaded libraries  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: B**

**Giải thích:**
- **Starter Pool** dùng prehydrated/pre-warmed capacity và Microsoft mô tả thời gian khởi tạo thường khoảng **5–10 giây** trong common scenarios; đây không phải SLA cố định
- **Custom Spark Pool** (A, C) có thời gian khởi tạo lâu hơn vì cần provision resources
- **Environment** (D) quản lý runtime/libraries/compute configuration; bản thân việc tạo Environment không phải đáp án trực tiếp cho yêu cầu startup nhanh nhất
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

**Hai notebook do hai người dùng khác nhau chạy trong cùng workspace. Họ muốn dùng High Concurrency để chia sẻ cùng một Spark session. Điều nào đúng?**

A. Có, chỉ cần cùng workspace  
B. Có, nếu cùng default lakehouse  
C. Không; High Concurrency session sharing được giới hạn theo cùng một user  
D. Chỉ cần cả hai là Contributor

<details><summary>✅ Đáp án</summary>

**C.** High Concurrency giúp nhiều notebook tương thích **của cùng một user** chia sẻ session; không nên suy luận thành cross-user session sharing.
</details>

---

### Question 7

**Một tổ chức muốn cho phép người dùng truy cập OneLake bằng Azure Storage Explorer. Setting nào cần kiểm tra trước?**

A. Workspace > OneLake > Lifecycle management  
B. Tenant setting **Users can access data stored in OneLake with apps external to Fabric**  
C. Spark session timeout  
D. Domain certification setting

<details><summary>✅ Đáp án</summary>

**B.** Đây là **tenant setting**. Lifecycle management là workspace setting phục vụ storage tiering, không phải external-app authorization.
</details>

---

### Question 8

**Workspace chứa file lịch sử phải giữ lại nhiều năm. File ít được truy cập sau 90 ngày và mục tiêu là giảm storage cost mà không xóa dữ liệu. Nên cấu hình gì?**

A. OneLake lifecycle rule chuyển file sang Cold tier  
B. OneLake shortcut  
C. Domain Contributor  
D. Spark High Concurrency

<details><summary>✅ Đáp án</summary>

**A.** OneLake lifecycle management tự động tier dữ liệu theo created/modified/accessed time. Đây là đúng scope “OneLake workspace settings”.
</details>

---

### Question 9

**Spark jobs trong workspace thường bị cạnh tranh capacity. Admin muốn Fabric cho phép job bắt đầu dựa trên minimum core requirement và scale dần khi capacity còn tài nguyên. Setting/behavior nào phù hợp?**

A. Optimistic Job Admission  
B. OneLake immutability  
C. Deployment rule  
D. Domain default label

<details><summary>✅ Đáp án</summary>

**A.** Fabric Spark workspaces mặc định bật **Optimistic Job Admission**. Admin có thể chọn reserve maximum cores nếu cần cách admission bảo thủ hơn.
</details>

---

### Question 10

**Production Airflow scheduler phải luôn sẵn sàng 24x7. Lựa chọn nào phù hợp nhất?**

A. Starter pool  
B. Custom pool  
C. Spark starter pool  
D. OneLake default tier

<details><summary>✅ Đáp án</summary>

**B.** Custom Airflow pool là always-on đến khi pause thủ công; Starter pool deprovision sau thời gian idle.
</details>

---

## 📝 TÓM TẮT KIẾN THỨC CẦN NHỚ

| Chủ đề | Phản xạ khi làm bài |
|---|---|
| **Spark Starter vs Custom** | Starter = fast/common workloads; Custom = kiểm soát node/autoscale/dynamic allocation |
| **Spark Environment** | Runtime + libraries + compute settings; có thể đặt workspace default |
| **Spark Jobs settings** | Optimistic Job Admission, reserve maximum cores, session timeout |
| **Domain** | Logical governance/data mesh; **không tự cấp data access** |
| **Delegated domain settings** | Có thể gồm default sensitivity label và certification; feature/state có thể Preview |
| **OneLake workspace settings** | Lifecycle management, default tier, diagnostics, immutability |
| **OneLake tenant settings** | External apps, File Explorer, SAS, diagnostic identifiers — đừng nhầm scope |
| **Airflow Starter vs Custom** | Starter = intermittent/dev; Custom = always-on/production |
| **Preview vs GA** | Không chọn feature chỉ vì mới; ưu tiên đúng requirement và compatibility |

> **Điểm cá nhân cần khóa:** “Configure Fabric workspace settings” nằm trong Top 3 weakness của score report của bạn. Hãy luyện câu hỏi theo **scope**: Tenant → Domain → Capacity → Workspace → Item → Data, vì rất nhiều đáp án nhiễu chỉ khác nhau ở cấp cấu hình.

---

## 7. BỔ SUNG ÔN THI 2026: SPARK SETTINGS VÀ QUYẾT ĐỊNH KIẾN TRÚC

### 7.1 Spark Runtime hiện hành

- **Runtime 1.3 (GA):** Spark 3.5.5, Python 3.11, Delta Lake 3.2; Microsoft hiện khuyến nghị dùng GA runtime cho production khi cần độ ổn định/tương thích.
- **Runtime 2.0 (Public Preview):** Spark 4.1, Python 3.13, Delta Lake 4.2. Tính năng Preview có thể xuất hiện trong đề nếu phổ biến, nhưng production decision phải kiểm tra interoperability.

### 7.2 Jobs settings — phần dễ bị bỏ sót

Workspace Admin có thể cấu hình **Optimistic Job Admission**, **Reserve maximum cores for active Spark jobs**, và **Spark session timeout**. Session expiry mặc định cho interactive session là khoảng 20 phút.

- Optimistic Job Admission: job bắt đầu theo **minimum core requirement**, rồi autoscale nếu capacity cho phép.
- Reserve maximum cores: phù hợp khi muốn admission dựa trên peak/max resource reservation, đổi lại concurrency có thể thấp hơn.

### 7.3 Những bẫy dễ gặp

- Starter pool nhanh nhờ pre-warmed capacity nhưng không bảo đảm mọi session đều khởi động trong đúng 5 giây.
- High concurrency không có nghĩa nhiều người dùng chia sẻ một session; session sharing bị giới hạn theo user và các notebook phải tương thích.
- Runtime mới nhất không tự động là lựa chọn tốt nhất. Runtime 2.0 vẫn Preview và Delta 4.x features có thể không interoperable với mọi Fabric workload.
- Chọn pool dựa trên SLA khởi động, concurrency, libraries, runtime và resource profile; không chọn chỉ dựa trên “2 TB/ngày”.

### 7.4 Lab nhanh

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

### 7.5 Câu hỏi tình huống bổ sung

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

- [DP-700 Study Guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)
- [Spark workspace administration settings](https://learn.microsoft.com/en-us/fabric/data-engineering/workspace-admin-settings)
- [Spark job admission](https://learn.microsoft.com/en-us/fabric/data-engineering/job-admission-management)
- [Fabric domains](https://learn.microsoft.com/en-us/fabric/governance/domains)
- [Domain-level default sensitivity labels](https://learn.microsoft.com/en-us/fabric/governance/domain-default-sensitivity-label)
- [OneLake lifecycle management](https://learn.microsoft.com/en-us/fabric/onelake/onelake-lifecycle-management)
- [OneLake tenant settings](https://learn.microsoft.com/en-us/fabric/admin/service-admin-portal-onelake)
- [OneLake Settings REST API](https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-settings)
- [Apache Airflow Job workspace settings](https://learn.microsoft.com/en-us/fabric/data-factory/apache-airflow-jobs-workspace-settings)
- [Apache Airflow compute](https://learn.microsoft.com/en-us/fabric/data-factory/apache-airflow-compute)

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

## 9. READINESS CHECK — WORKSPACE SETTINGS

Bạn chưa nên coi phần này là “nắm chắc” nếu chưa tự trả lời được, không nhìn tài liệu:

1. Khi nào dùng Starter pool, Custom Spark pool, Custom Airflow pool?
2. Environment khác Spark pool và runtime như thế nào?
3. Optimistic Job Admission giải quyết vấn đề gì?
4. Domain có cấp quyền truy cập dữ liệu không?
5. Default sensitivity label ở domain khác sensitivity label/protection policy ra sao?
6. OneLake **workspace settings** khác OneLake **tenant settings** ở đâu?
7. Lifecycle management dùng Hot/Cool/Cold như thế nào?
8. Diagnostics/immutability khác Microsoft Purview audit log ra sao?
9. High Concurrency có chia sẻ session giữa hai user khác nhau không?
10. Khi feature còn Preview, tiêu chí nào quyết định có nên dùng production?

**Ngưỡng khuyến nghị:** làm đúng ≥ 85% câu scenario mới, và tự cấu hình/lab được ít nhất Spark settings + OneLake lifecycle + Airflow pool decision trong tenant test.
