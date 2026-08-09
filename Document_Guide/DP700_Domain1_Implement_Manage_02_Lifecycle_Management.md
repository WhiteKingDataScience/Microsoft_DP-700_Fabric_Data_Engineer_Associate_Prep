# DP-700: Implement Lifecycle Management in Fabric
## Implement and manage an analytics solution (30–35%)
### Skills measured as of July 21, 2026

> [!IMPORTANT]
> **Bản hiệu chỉnh ngày 09/08/2026.** Nội dung được đối chiếu với blueprint DP-700 có hiệu lực từ **21/07/2026**. Microsoft cho biết phần lớn câu hỏi tập trung vào tính năng GA, nhưng tính năng Preview phổ biến vẫn có thể xuất hiện. Với các bảng “supported items”, runtime, limits và trạng thái Preview/GA, hãy kiểm tra Microsoft Learn lần cuối trước ngày thi.


> [!CAUTION]
> **Không có tài liệu nào có thể bảo đảm “trúng mọi câu hỏi”.** Microsoft nêu rõ danh sách skills là minh họa và đề có thể hỏi các chủ đề liên quan. Mục tiêu của bản này là: phủ đủ blueprint hiện hành, sửa các facts đã thay đổi, và luyện cách chọn đáp án theo **scenario + constraint + least privilege/least operations**.

> **Tài liệu ôn thi DP-700: Fabric Data Engineer Associate**
> Phần này bao gồm toàn bộ kiến thức về quản lý vòng đời (lifecycle management) trong Microsoft Fabric.

> **Tham khảo chính:** [DP-700 Study Guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700) và [Plan CI/CD for Microsoft Fabric solutions](https://learn.microsoft.com/en-us/fabric/fundamentals/understand-best-practices-fabric-cicd).

---

## 📋 MỤC LỤC

1. [Tổng quan về Lifecycle Management](#1-tổng-quan-về-lifecycle-management)
2. [Configure Version Control (Git Integration)](#2-configure-version-control-git-integration)
3. [Implement Database Projects](#3-implement-database-projects)
4. [Create and Configure Deployment Pipelines](#4-create-and-configure-deployment-pipelines)
5. [Câu hỏi mô phỏng bài thi (Practice Questions)](#5-câu-hỏi-mô-phỏng-bài-thi)

---

## 1. TỔNG QUAN VỀ LIFECYCLE MANAGEMENT

### 1.1 Application Lifecycle Management (ALM) trong Fabric

ALM trong Microsoft Fabric bao gồm hai trụ cột chính. Xem thêm [tổng quan CI/CD của Fabric](https://learn.microsoft.com/en-us/fabric/fundamentals/understand-best-practices-fabric-cicd) để hiểu cách Git, deployment pipelines và các lựa chọn tự động hóa kết hợp với nhau:

```
┌─────────────────────────────────────────────────────────┐
│              Fabric ALM Framework                        │
├──────────────────────┬──────────────────────────────────┤
│   SOURCE CONTROL     │    RELEASE MANAGEMENT            │
│   (Git Integration)  │    (Deployment Pipelines)        │
├──────────────────────┼──────────────────────────────────┤
│ • Version history    │ • Dev → Test → Prod promotion    │
│ • Branching          │ • Deployment rules               │
│ • Collaboration      │ • Comparison & diff              │
│ • Code review        │ • Automated deployment           │
│ • Rollback           │ • Content cloning                │
└──────────────────────┴──────────────────────────────────┘
```

### 1.2 Tại sao ALM quan trọng?

1. **Traceability:** Theo dõi ai thay đổi gì, khi nào
2. **Collaboration:** Nhiều developers làm việc song song
3. **Quality Assurance:** Test trước khi deploy production
4. **Rollback:** Dễ dàng quay lại phiên bản trước khi có lỗi
5. **Compliance:** Đáp ứng yêu cầu audit và governance

---

## 2. CONFIGURE VERSION CONTROL (GIT INTEGRATION)

### 2.1 Git Integration là gì?

Git Integration trong Fabric cho phép kết nối workspace với **Git repository** để quản lý version control cho các Fabric items. Xem [Fabric Git integration overview](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/intro-to-git-integration) và [Git integration process/permissions](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-integration-process).

### 2.2 Git Providers được hỗ trợ (cập nhật 08/2026)

| Provider | Trạng thái |
|---|---|
| [**Azure DevOps**](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-integration-process#azure-devops-limitations) | Hỗ trợ, cloud-based only |
| [**GitHub**](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-integration-process#github-limitations) | Hỗ trợ, cloud-based only |
| [**GitHub Enterprise**](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-integration-process#github-enterprise-limitations) | Hỗ trợ theo các giới hạn riêng |
| Bitbucket / GitLab | Không có native Fabric Git integration trong support matrix hiện tại |

### 2.3 Supported items: học nguyên tắc, không học thuộc bảng tĩnh

Support matrix thay đổi nhanh. Các nhóm Data Engineering/Data Factory hiện hỗ trợ rộng hơn trước, gồm Lakehouse, Notebook, Environment, Spark Job Definition, Pipeline, Dataflow Gen2, Copy Job và nhiều item mới/Preview. Hãy kiểm tra [giới hạn và supported items của Git integration](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-integration-process) trước khi học thuộc một trường hợp cụ thể.

**Nguyên tắc bền vững để làm bài:**
- Git lưu **item definition/metadata/source**, không phải raw data trong OneLake.
- Workspace kết nối tới **một branch** của repo tại một thời điểm.
- Initial connect/sync/disconnect cần Workspace Admin; các thao tác commit/update sau khi đã kết nối còn phụ thuộc Fabric Write + Git permissions.
- Luôn kiểm tra live supported-item matrix nếu câu hỏi phụ thuộc một item mới/Preview.

### 2.3.1 Auto-binding và dependencies — kiến thức 2026 đáng học

Khi Git update/deploy tạo item ở workspace đích, Fabric cố **auto-bind** các tham chiếu dependency trong cùng workspace để item mới trỏ tới counterpart phù hợp. Với dependency xuyên workspace hoặc item chưa hỗ trợ auto-binding, có thể cần cấu hình sau deploy; **[Variable library](https://learn.microsoft.com/en-us/fabric/cicd/variable-library/tutorial-variable-library)/item reference** giúp tránh hard-code ID theo môi trường. Xem bảng chi tiết trong [Understand dependency binding in cross-workspace deployment](https://learn.microsoft.com/en-us/fabric/cicd/cross-workspace-dependency-binding).

### 2.4 Cách kết nối Workspace với Git

**Bước 1: Prerequisites** — đối chiếu [ma trận quyền Git Integration](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-integration-process#required-fabric-permissions-for-popular-actions) vì quyền phụ thuộc thao tác (connect, update, commit, switch branch).
- **Chỉ Workspace Admin** có thể kết nối, đồng bộ ban đầu hoặc ngắt kết nối workspace với Git. Sau khi kết nối, Member/Contributor có đủ Write và Git permissions có thể commit/update; branch switching cần workspace setting tương ứng.
- Git repository phải tồn tại trước khi kết nối
- Tenant setting "Users can sync workspace items with their Git repositories" phải được **enabled**

**Bước 2: Cấu hình kết nối**

```
Workspace settings > Git integration
├── Git provider: Azure DevOps / GitHub
├── Account/Organization: <your-org>
├── Repository: <repo-name>
├── Branch: main (hoặc branch khác)
└── Git folder: / (root) hoặc /fabric-workspace/
```

**Bước 3: Initial Sync**
- **Connect and sync:** Đồng bộ items giữa workspace và Git
- Nếu workspace có items → chọn commit workspace changes to Git
- Nếu Git có items → chọn update workspace from Git

### 2.5 Bi-directional Sync

Git Integration hỗ trợ **đồng bộ hai chiều**. Quy trình initial sync, commit và update được mô tả trong [Connect and sync](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-integration-process#connect-and-sync):

```
                    ┌──────────────┐
        COMMIT      │              │      PULL/UPDATE
    ──────────────> │  Git Repo    │ <──────────────
    Workspace → Git │  (Source of  │ Git → Workspace
                    │   Truth)     │
                    └──────────────┘
```

#### Commit (Workspace → Git)
- Phát hiện thay đổi trong workspace (modified, new, deleted items)
- User review changes và commit với message
- Tạo version history trong Git

#### Update (Git → Workspace)
- Pull thay đổi từ Git vào workspace
- Phát hiện conflicts nếu cùng item bị sửa ở cả hai nơi
- User phải resolve conflicts trước khi update

### 2.6 Branching Strategy

**Recommended Pattern:** Tham khảo [Development process in Microsoft Fabric](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/manage-branches) để chọn branch/workspace phù hợp; mỗi workspace chỉ kết nối trực tiếp với một branch tại một thời điểm.

```
main (Production)
├── release/v1.0 (Staging/Testing)
├── develop (Development integration)
│   ├── feature/add-sales-pipeline
│   ├── feature/update-customer-model
│   └── bugfix/fix-null-handling
```

**Mỗi branch có thể kết nối với một workspace riêng:**

| Branch | Workspace | Mục đích |
|--------|-----------|----------|
| `main` | Production-WS | Production environment |
| `develop` | Development-WS | Integration testing |
| `feature/*` | Feature-WS | Individual feature development |

### 2.7 Conflict Resolution

Khi có conflict (thay đổi ở cả workspace và Git), xem các trạng thái `Conflict`, `Uncommitted changes` và quy trình [Git status/sync](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-integration-process#git-status):

1. **Workspace wins:** Giữ phiên bản workspace, commit đè lên Git
2. **Git wins:** Giữ phiên bản Git, update workspace từ Git
3. **Manual merge:** (Thường thực hiện bên ngoài Fabric, sử dụng Git tools)

### 2.8 Best Practices cho Git Integration

1. **Sử dụng branching strategy** rõ ràng (GitFlow hoặc trunk-based); xem thêm [Fabric CI/CD best practices](https://learn.microsoft.com/en-us/fabric/fundamentals/understand-best-practices-fabric-cicd)
2. **Commit thường xuyên** với messages có ý nghĩa
3. **KHÔNG commit** sensitive data (credentials, connection strings)
4. **Git là source of truth** - workspace chỉ là môi trường làm việc
5. **Review changes** trước khi commit (code review qua Pull Requests)

---

## 3. IMPLEMENT DATABASE PROJECTS

### 3.1 Database Projects là gì?

**Database Projects** (SQL Database Projects) trong Fabric cho phép quản lý schema của SQL databases dưới dạng code (schema-as-code), hỗ trợ offline development và CI/CD. Đọc [Fabric SQL database source control](https://learn.microsoft.com/en-us/fabric/database/sql/source-control) để biết chính xác object definition nào được serialize vào Git.

### 3.2 Công cụ và Extensions cần thiết

| Công cụ | Mục đích |
|---------|----------|
| [**Visual Studio Code**](https://code.visualstudio.com/) | IDE chính cho database development |
| [**MSSQL Extension**](https://learn.microsoft.com/en-us/sql/tools/visual-studio-code-extensions/mssql/mssql-extension-visual-studio-code?view=sql-server-ver17) | Kết nối và query SQL databases |
| [**SQL Database Projects Extension**](https://learn.microsoft.com/en-us/sql/tools/visual-studio-code-extensions/sql-database-projects/getting-started-sql-database-projects-extension?view=sql-server-ver17) | Tạo và quản lý `.sqlproj` files |
| [**DacFx (Data-Tier Application Framework)**](https://learn.microsoft.com/en-us/sql/tools/sql-database-projects/sql-projects-tools?view=sql-server-ver17) | Build và deploy database packages |
| [**SqlPackage**](https://learn.microsoft.com/en-us/sql/tools/sqlpackage/sqlpackage?view=sql-server-ver17) | CLI tool cho database deployment |

### 3.3 Workflow của Database Projects

```
┌─────────────────────────────────────────────────────────────┐
│                  Database Project Workflow                    │
│                                                              │
│  1. CREATE         2. DEVELOP        3. COMMIT               │
│  ┌──────────┐     ┌──────────┐     ┌──────────┐            │
│  │ .sqlproj │ ──> │ T-SQL    │ ──> │   Git    │            │
│  │  file    │     │ scripts  │     │  Repo    │            │
│  └──────────┘     └──────────┘     └──────────┘            │
│                                         │                    │
│  6. VALIDATE      5. DEPLOY        4. BUILD                  │
│  ┌──────────┐     ┌──────────┐     ┌──────────┐            │
│  │  Test    │ <── │ Fabric   │ <── │ .dacpac  │            │
│  │  Results │     │ SQL DB   │     │ package  │            │
│  └──────────┘     └──────────┘     └──────────┘            │
└─────────────────────────────────────────────────────────────┘
```

### 3.4 Tạo Database Project

**Bước 1: Khởi tạo project**
```bash
# Sử dụng VS Code Command Palette
# > SQL Database Projects: Create New Project
# Hoặc sử dụng dotnet CLI:
dotnet new sqlproj -n MyDatabaseProject
```

**Bước 2: Cấu trúc project**
```
MyDatabaseProject/
├── MyDatabaseProject.sqlproj    # Project file
├── Tables/
│   ├── dbo.Customer.sql
│   ├── dbo.Product.sql
│   └── dbo.Sales.sql
├── Views/
│   └── dbo.vw_SalesSummary.sql
├── StoredProcedures/
│   └── dbo.usp_LoadSales.sql
└── Security/
    └── Roles.sql
```

**Bước 3: Định nghĩa objects**
```sql
-- Tables/dbo.Customer.sql
CREATE TABLE [dbo].[Customer] (
    [CustomerID]    INT            NOT NULL PRIMARY KEY,
    [CustomerName]  NVARCHAR(100)  NOT NULL,
    [Email]         NVARCHAR(255)  NULL,
    [Region]        NVARCHAR(50)   NOT NULL,
    [CreatedDate]   DATETIME2      NOT NULL DEFAULT GETDATE()
);
GO
```

### 3.5 Tích hợp với Git (Source Control)

**Convert live database to code:** Xem [Add the Fabric SQL database to source control](https://learn.microsoft.com/en-us/fabric/database/sql/source-control#add-the-fabric-sql-database-to-source-control) để thực hành serialize object definitions:
1. Kết nối Fabric SQL Database với Git repository
2. Fabric tự động serialize database objects thành SQL scripts
3. Mỗi object (table, view, procedure) = một file .sql trong Git

**Update database from source control:**
1. Sửa đổi SQL scripts trong Git (thông qua PR)
2. Merge vào branch chính
3. Fabric detect changes và apply schema diff vào live database

### 3.6 Build và Deploy

**Build .dacpac package:** Theo [SQL Database Projects extension](https://learn.microsoft.com/en-us/sql/tools/visual-studio-code-extensions/sql-database-projects/getting-started-sql-database-projects-extension?view=sql-server-ver17#build-and-publish), Build vừa validate project vừa tạo file `.dacpac`:
```bash
# Sử dụng dotnet CLI
dotnet build MyDatabaseProject.sqlproj

# Output: bin/Debug/MyDatabaseProject.dacpac
```

**Deploy với SqlPackage:** Xem [SqlPackage Publish](https://learn.microsoft.com/en-us/sql/tools/sqlpackage/sqlpackage-publish?view=sql-server-ver17) để biết các tham số `/Action:Publish`, `/SourceFile` và publish profile:
```bash
# Deploy dacpac tới Fabric SQL Database
SqlPackage /Action:Publish \
    /SourceFile:MyDatabaseProject.dacpac \
    /TargetConnectionString:"Server=<server>;Database=<db>;Authentication=Active Directory Default"
```

### 3.7 CI/CD Integration

**Azure DevOps Pipeline example:** Quy trình build/publish tương tự được minh họa trong [Create and deploy a SQL project](https://learn.microsoft.com/en-us/sql/tools/sql-database-projects/tutorials/create-deploy-sql-project?view=sql-server-ver17):
```yaml
# azure-pipelines.yml
trigger:
  branches:
    include:
      - main

pool:
  vmImage: 'ubuntu-latest'

steps:
  - task: UseDotNet@2
    inputs:
      packageType: 'sdk'
      version: '8.x'
  
  - script: |
      dotnet build MyDatabaseProject.sqlproj
    displayName: 'Build Database Project'
  
  - task: SqlAzureDacpacDeployment@1
    inputs:
      azureSubscription: 'MyAzureSubscription'
      AuthenticationType: 'servicePrincipal'
      DacpacFile: '$(Build.SourcesDirectory)/bin/Debug/MyDatabaseProject.dacpac'
      ServerName: '<fabric-sql-endpoint>'
      DatabaseName: '<database-name>'
    displayName: 'Deploy to Fabric SQL Database'
```

### 3.8 Ví dụ minh họa thực tế

**Tình huống:** Team cần quản lý schema cho Fabric SQL Database dùng trong data warehouse.

**Quy trình:**
1. Dev tạo feature branch từ `develop`
2. Thêm table mới `dbo.FactOrders` trong VS Code
3. Build local để verify syntax
4. Commit và tạo Pull Request
5. Code review bởi senior engineer
6. Merge → CI pipeline tự động build .dacpac
7. CD pipeline deploy tới Test environment
8. QA verify → approve → deploy tới Production

---

### 3.9 Database Projects cho Fabric Warehouse (Preview)

Đừng giới hạn khái niệm **database project** vào Fabric SQL Database. [Fabric Warehouse source control](https://learn.microsoft.com/en-us/fabric/data-warehouse/source-control) hiện cũng có thể serialize schema thành **SQL database project** (Preview):

- mỗi table/view/procedure/function definition được lưu thành source `.sql`;
- **table data không được đưa vào Git/project**;
- **SQL security features không được đưa vào warehouse project** và cần script-based/post-deployment approach nếu phải migrate;
- với VS Code, Fabric Warehouse dùng SDK-style SQL project và target platform phù hợp cho Synapse Data Warehouse in Microsoft Fabric.

**Exam rule:** “version/control schema definitions” → database project; “move/copy data” → không phải nhiệm vụ của database project.

---

## 4. CREATE AND CONFIGURE DEPLOYMENT PIPELINES

### 4.1 Deployment pipeline dùng để làm gì?

**[Deployment pipelines](https://learn.microsoft.com/en-us/fabric/cicd/deployment-pipelines/intro-to-deployment-pipelines)** là Fabric-native release management: so sánh và promote item giữa các environment/stage như Dev → Test → Prod. Đừng nhầm với Git:

- **Git integration** = version control, branch, PR, source of truth.
- **Deployment pipeline** = promote item giữa workspace/stage, compare/diff, selected deployment.
- Hai cơ chế thường được dùng **kết hợp**, nhưng không bắt buộc phải có cả hai.

### 4.2 Stage và workspace

Dev/Test/Prod là pattern phổ biến, nhưng không nên học thuộc một số lượng stage cố định như một “fact thi”. Theo [Get started with deployment pipelines](https://learn.microsoft.com/en-us/fabric/cicd/deployment-pipelines/get-started-with-deployment-pipelines), pipeline có thể có từ 2 đến 10 stage. Hãy tập trung vào quan hệ:

`Source stage/workspace → compare → select items → deploy → target stage/workspace`

Khi deploy selected items (xem [Selective deployment và paired items](https://learn.microsoft.com/en-us/fabric/cicd/deployment-pipelines/get-started-with-deployment-pipelines#step-6-deploy-content-from-one-stage-to-another)):
- Paired item ở target có thể được **ghi đè/cập nhật** theo source.
- Item chỉ tồn tại ở target **không tự động bị xóa** chỉ vì deploy selected content.
- Với SQL Database, Fabric tính schema diff/T-SQL cần thiết; data hiện hữu được giữ nếu thay đổi an toàn. Thay đổi có nguy cơ data loss có thể yêu cầu xử lý thủ công.

### 4.3 Permissions — tránh overclaim “phải Admin mọi stage”

Permission phụ thuộc thao tác và trạng thái target. Hãy đối chiếu [Get started with deployment pipelines – prerequisites và permissions](https://learn.microsoft.com/en-us/fabric/cicd/deployment-pipelines/get-started-with-deployment-pipelines) trước khi trả lời câu hỏi quyền. Người deploy cũng cần quyền phù hợp ở target; Admin cần cho các thao tác quản trị workspace/pipeline nhất định, nhưng **không học quy tắc sai rằng mọi deployment đều bắt buộc Workspace Admin trên tất cả stages**.

### 4.4 Deployment rules — support matrix hiện tại

Deployment rules **không phải generic connection-rewrite engine cho mọi Fabric item**. Xem [Create deployment rules](https://learn.microsoft.com/en-us/fabric/cicd/deployment-pipelines/create-rules) để kiểm tra item/rule combination đang được hỗ trợ.

| Rule | Item hiện được hỗ trợ điển hình |
|---|---|
| **Data source rule** | Dataflow Gen1, semantic model, paginated report, mirrored database |
| **Parameter rule** | Dataflow Gen1, semantic model |
| **Default lakehouse rule** | Notebook |

> [!WARNING]
> Không chọn “Connection rule cho Pipeline/Dataflow Gen2” chỉ vì nghe hợp lý. Với pipelines/notebooks hiện đại, **Variable library** thường là cách bền vững hơn để quản lý environment-specific IDs, paths, connection/config values.

### 4.5 Variable library — bổ sung rất quan trọng cho CI/CD 2026

Variable library là một Fabric item chứa variables và nhiều **value sets** (ví dụ `dev`, `test`, `prod`). Mỗi environment/stage có thể active value set khác nhau. Xem [tutorial Variable libraries](https://learn.microsoft.com/en-us/fabric/cicd/variable-library/tutorial-variable-library) và [Variable library integration with pipelines](https://learn.microsoft.com/en-us/fabric/data-factory/variable-library-integration-with-data-pipelines).

Use case điển hình:
- `SourceWorkspaceId`
- `LakehouseId`
- `ServerName`
- `LandingPath`
- Feature flags / configuration strings

Pipeline có thể dùng Library variables trong dynamic content; notebook có thể đọc qua `notebookutils.variableLibrary`. Đây là cách giảm hard-code và đơn giản hóa promotion giữa môi trường.

### 4.6 SQL Database trong deployment pipeline

Fabric SQL Database được quản lý theo **schema-as-code**; xem [SQL Database deployment pipelines](https://learn.microsoft.com/en-us/fabric/database/sql/deployment-pipelines):
1. Source control tạo/duy trì SQL project.
2. Update/deploy build project → `.dacpac`/SqlPackage logic → tính differential schema change.
3. Database-level settings như collation/compatibility level hiện không nằm trong source control/deployment pipeline integration.
4. **Pre-deployment/Post-deployment scripts** có thể xử lý tác vụ trước/sau schema deployment, ví dụ seed/static data.

### 4.7 Warehouse và SQL database projects

Fabric Warehouse source control hiện hỗ trợ mô hình **SQL database project** (Preview); tài liệu [Warehouse source control](https://learn.microsoft.com/en-us/fabric/data-warehouse/source-control) nêu rõ mỗi SQL object thành file `.sql`, còn **table data và SQL security features không nằm trong project**. Vì vậy, nếu scenario hỏi “version schema vs version data”, câu trả lời là version **schema/code**, không phải warehouse data.

### 4.8 Git + Deployment Pipeline + Variable Library: mental model

```text
Git repo (xem [CI/CD workflow options in Fabric](https://learn.microsoft.com/en-us/fabric/cicd/manage-deployment))
  └─ version item definitions / SQL projects
       ↓ update/sync
Dev workspace
  └─ deploy selected items
       ↓
Test workspace  -- active value set = test
  └─ validate
       ↓
Prod workspace  -- active value set = prod
```

### 4.9 Bẫy thi

- Git không copy OneLake data.
- Deployment pipeline không thay thế Git history/PR.
- Deployment rules chỉ áp dụng cho **supported combinations**; đừng suy rộng.
- Environment-specific configuration → ưu tiên **Variable library** khi item hỗ trợ.
- SQL Database/Warehouse project = schema/code; dữ liệu vật lý không phải Git artifact.
- Preview support matrix thay đổi nhanh; nếu đề phụ thuộc một item cụ thể, xác định “supported item” trước rồi mới chọn workflow.

## 5. CÂU HỎI MÔ PHỎNG BÀI THI

### Question 1

**You are setting up version control for a Microsoft Fabric workspace. The organization uses GitHub for source control. You need to connect the workspace to a GitHub repository. What is the MINIMUM workspace role required to configure Git integration?**

A. Viewer  
B. Contributor  
C. Member  
D. Admin  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: D**

**Giải thích:**
- Theo mặc định, **Admin** role là minimum role cần thiết để cấu hình Git integration cho workspace
- Tuy nhiên, tenant setting có thể cho phép **Contributor** role thực hiện Git operations (commit/update), nhưng việc **initial setup** (kết nối workspace với Git) yêu cầu **Admin**
- **Viewer** (A) và **Member** (C) nếu không có tenant setting đặc biệt thì không đủ quyền
- **Contributor** (B) có thể commit/update sau khi admin đã setup, nhưng không thể initial configure

> **Cập nhật 2026:** Fabric đã mở rộng cho phép Contributor role thực hiện một số Git operations, nhưng initial connection vẫn yêu cầu Admin.

</details>

---

### Question 2

**You connect a Fabric workspace to an Azure DevOps Git repository. After making changes to a notebook in the workspace, you notice the notebook shows a "Modified" status. What should you do to save the changes to Git?**

A. Use the Sync button to automatically push changes  
B. Click Commit, review the changes, enter a commit message, and commit  
C. Use the Deploy button to promote changes to the next stage  
D. Navigate to Azure DevOps and pull the changes from the workspace  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: B**

**Giải thích:**
- Workflow đúng: **Commit** workspace changes to Git = review changes → enter commit message → commit
- **Sync** (A) không tự động push - sync bao gồm cả commit và update, nhưng bạn vẫn cần review và confirm
- **Deploy** (C) là Deployment Pipelines feature, không liên quan đến Git
- **Pull from Azure DevOps** (D) ngược chiều - pull là Git → Workspace, không phải Workspace → Git
- Git Integration sử dụng explicit commit model: user chủ động chọn khi nào commit thay đổi

</details>

---

### Question 3

**Your team uses Fabric deployment pipelines with three stages: Dev, Test, and Production. The Dev semantic model connects to a development SQL database. When deploying to Test, it should connect to a test SQL database. What should you configure?**

A. Manually change the data source in the Test workspace after deployment  
B. Configure a data source deployment rule for the Test stage  
C. Create a separate semantic model for each stage  
D. Use Git integration to manage different connection strings per branch  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: B**

**Giải thích:**
- **Data source deployment rules** tự động thay đổi connection khi deploy giữa các stages → đúng purpose
- **Manually change** (A) không scalable, dễ quên, không automated
- **Separate semantic models** (C) tạo duplication, khó maintain consistency
- **Git integration** (D) quản lý source control, không phải deployment rules
- Deployment rules là feature được thiết kế chính xác cho use case này: "cùng item, khác environment"

</details>

---

### Question 4

**You are implementing a database project for a Fabric SQL Database. You need to manage the database schema using an offline development workflow. Which combination of tools should you use? (Choose TWO)**

A. Visual Studio Code with SQL Database Projects extension  
B. SQL Server Management Studio (SSMS)  
C. DacFx / SqlPackage for building and deploying  
D. Power BI Desktop  
E. Azure Data Studio without any extensions  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: A và C**

**Giải thích:**
- **VS Code + SQL Database Projects extension** (A) cho phép tạo .sqlproj, chỉnh sửa schema offline, và quản lý database objects dưới dạng SQL scripts
- **DacFx / SqlPackage** (C) cho phép build .sqlproj thành .dacpac package và deploy tới Fabric SQL Database
- **SSMS** (B) là công cụ query và management, không hỗ trợ SQL Database Projects workflow
- **Power BI Desktop** (D) dùng cho semantic models/reports, không phải database schema management
- **Azure Data Studio** (E) có thể hỗ trợ nhưng cần extensions, và câu hỏi nói "without any extensions"

</details>

---

### Question 5

**A development team uses Git integration with three workspaces connected to different branches: feature/new-pipeline (Feature-WS), develop (Dev-WS), and main (Prod-WS). A developer finishes a feature and needs to promote it to production. What is the correct sequence of steps?**

A. Commit from Feature-WS → Merge feature branch to develop → Update Dev-WS from Git → Merge develop to main → Update Prod-WS from Git  
B. Deploy from Feature-WS directly to Prod-WS using deployment pipelines  
C. Copy items from Feature-WS to Prod-WS manually  
D. Commit from Feature-WS → Merge feature branch directly to main → Update Prod-WS from Git  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: A**

**Giải thích:**
- Quy trình đúng tuân thủ **GitFlow/branching strategy**:
  1. Developer commit changes từ Feature-WS vào feature branch
  2. Tạo PR để merge feature → develop (code review)
  3. Dev-WS update từ Git (pull develop branch)
  4. Test/verify trong Dev-WS
  5. Merge develop → main (hoặc qua release branch)
  6. Prod-WS update từ Git (pull main branch)
- **(B)** Deployment pipelines là cách khác, nhưng câu hỏi hỏi về Git workflow
- **(C)** Manual copy không có version tracking, không professional
- **(D)** Bỏ qua develop/testing stage → risky, skip QA

</details>

---

### Question 6

**You need to deploy items from a Development stage to a Test stage in a deployment pipeline. Some items exist in the Test stage but NOT in the Development stage. What happens to those items in the Test stage after deployment?**

A. They are automatically deleted from the Test stage  
B. They remain unchanged in the Test stage  
C. They are moved to a backup folder  
D. The deployment fails with a conflict error  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: B**

**Giải thích:**
- Deployment Pipelines chỉ deploy items **từ source stage** → items chỉ tồn tại ở target stage **KHÔNG bị ảnh hưởng**
- **(A)** Fabric KHÔNG tự động xóa items ở target stage
- **(C)** Không có cơ chế backup folder
- **(D)** Deployment không fail vì items extra ở target stage
- Đây là thiết kế "additive" - deployment thêm/cập nhật items, không xóa items existing chỉ ở target

</details>

---

### Question 7

**Your organization wants to use both Git integration AND deployment pipelines together. Which workflow represents the BEST PRACTICE?**

A. Use Git integration only; deployment pipelines are redundant  
B. Use deployment pipelines only; Git integration adds unnecessary complexity  
C. Use Git for source control and collaboration, and deployment pipelines for promoting content across stages  
D. Use Git for production deployment and deployment pipelines for development  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: C**

**Giải thích:**
- **Best practice:** Sử dụng Git cho **source control** (version tracking, branching, collaboration, code review) VÀ deployment pipelines cho **release management** (promote content, deployment rules)
- **(A)** Git không có deployment rules (thay đổi connections giữa environments)
- **(B)** Deployment pipelines không có version control, branching, code review
- **(D)** Ngược vai trò: Git dùng cho source control, pipelines dùng cho deployment
- Hai công cụ **bổ sung** cho nhau, không thay thế

</details>

---

### Question 8

**You have a Fabric SQL Database connected to Git. A developer modifies a table definition directly in the Fabric portal. What is the CORRECT way to synchronize this change with the Git repository?**

A. The change is automatically synced to Git in real-time  
B. The developer must commit the change from the workspace to Git  
C. Run SqlPackage to extract the schema and push to Git  
D. The developer must recreate the table definition in the .sqlproj file manually  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: B**

**Giải thích:**
- Khi database connected to Git, thay đổi trong Fabric portal tạo **uncommitted changes**
- Developer phải **explicitly commit** từ workspace to Git (review → commit message → commit)
- **(A)** Thay đổi KHÔNG tự động sync - Git Integration sử dụng explicit commit model
- **(C)** SqlPackage dùng cho deploy, không phải extract-and-push workflow trong Fabric
- **(D)** Fabric tự động serialize database objects - không cần manual recreation
- Key point: Git Integration = **bi-directional BUT manual** (user quyết định khi nào commit/update)

</details>

---

### Question 9

**You are creating a deployment pipeline with custom stages. The business requires the following environments: Development → Quality Assurance → User Acceptance Testing → Pre-Production → Production. How many stages do you need to configure?**

A. 3 (the default stages are sufficient)  
B. 5  
C. You cannot create more than 3 stages in a deployment pipeline  
D. 4 (UAT and Pre-Production can be combined)  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: B**

**Giải thích:**
- Deployment pipelines hỗ trợ từ **2 đến 10 stages** (không giới hạn ở 3)
- Yêu cầu: Dev + QA + UAT + Pre-Prod + Prod = **5 stages**
- **(A)** 3 stages mặc định không đủ cho yêu cầu 5 environments
- **(C)** SAI - Fabric cho phép tới 10 stages, không giới hạn ở 3
- **(D)** Kết hợp stages vi phạm yêu cầu business "separate environments"
- Đây là câu hỏi "trap" - nhiều người nhầm rằng deployment pipelines chỉ có 3 stages

</details>

---

### Question 10

**You are working on a Fabric workspace that is connected to a Git repository. A colleague has made changes in the same workspace and committed them. You then make changes to the same notebook. When you try to commit, you encounter a conflict. What is the RECOMMENDED approach to resolve this?**

A. Force overwrite the Git version with your changes  
B. Undo your changes and update the workspace from Git first, then reapply your changes  
C. Delete the notebook and recreate it from scratch  
D. Disconnect the workspace from Git, make changes, then reconnect  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: B**

**Giải thích:**
- **Best practice:** Pull latest changes first (update from Git) → resolve conflicts → reapply your changes → commit
- **(A)** Force overwrite sẽ mất colleague's changes → data loss
- **(C)** Xóa và tạo lại mất version history, quá extreme
- **(D)** Disconnect/reconnect không giải quyết conflict, có thể gây thêm vấn đề
- Standard Git workflow: **Always pull before push** → resolve conflicts locally → then commit

</details>

---

## 📝 TÓM TẮT KIẾN THỨC CẦN NHỚ

| Chủ đề | Điểm then chốt |
|---|---|
| **Git providers** | Azure DevOps, GitHub, GitHub Enterprise — cloud-based |
| **Git scope** | Version item definition/metadata; không version raw OneLake data |
| **Git permissions** | Admin cho initial connect/sync/disconnect; ongoing commit/update cần Fabric + repo permission phù hợp |
| **Auto-binding** | Fabric cố bind dependencies sau sync/deploy; cross-workspace refs cần thiết kế cẩn thận |
| **Database projects** | Schema-as-code → build/validate → dacpac/SqlPackage style deployment |
| **SQL Database** | Git + deployment pipeline; pre/post-deploy scripts; database settings không phải mọi thứ đều versioned |
| **Warehouse** | Source control/database project hiện Preview; không chứa table data/SQL security |
| **Deployment pipeline** | Release/promotion; selected deployment; compare source/target |
| **Deployment rules** | Chỉ một số item/rule combination; không có generic pipeline connection rule |
| **Variable library** | Quản lý config/value sets cho Dev/Test/Prod; giảm hard-code |
| **Git vs deployment** | Git = source control; deployment pipeline = release management |

---

## 6. BỔ SUNG ÔN THI 2026: CI/CD DECISION POINTS

### 6.1 Ma trận quyết định

| Yêu cầu | Chọn |
|---|---|
| Version history, branch, PR, code review | Git integration |
| Promote Dev → Test → Prod bằng Fabric-native UI/API | Deployment pipelines |
| Tham số môi trường dùng chung giữa nhiều item | Variable library trước; deployment rules khi phù hợp |
| Tự động triển khai từ repo và biến đổi item definition | Fabric Git/Items APIs hoặc `fabric-cicd` |

### 6.2 Bẫy thi

- Admin kết nối workspace với Git; Contributor không tự làm initial connection.
- Git sync định nghĩa item, không sao chép dữ liệu Lakehouse/OneLake.
- Deploy sang stage đích ghi đè paired items; item chỉ có ở stage đích không tự bị xóa.
- Supported-item matrix thay đổi nhanh và có item Preview; đề thi thường hỏi nguyên tắc hơn là bắt thuộc toàn bộ danh sách.

### 6.3 Câu hỏi tình huống bổ sung

**Một Contributor có Write trên mọi item và Write trong GitHub repo. Workspace đã được Admin kết nối Git. Contributor cần commit thay đổi notebook. Có cần nâng lên Admin không?**

A. Có, mọi Git operation đều cần Admin  
B. Không, Contributor có đủ Fabric và Git permissions có thể commit/update  
C. Có, vì GitHub chỉ hỗ trợ Viewer  
D. Không, nhưng chỉ khi deployment pipeline có 10 stages

<details><summary>Đáp án</summary>

**B.** Admin cần cho connect/sync/disconnect. Sau khi đã kết nối, Contributor có đủ quyền có thể commit/update.
</details>


---

## NGUỒN CHÍNH THỨC ĐÃ ĐỐI CHIẾU

- [DP-700 Study Guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)
- [Fabric Git integration overview](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/intro-to-git-integration)
- [Git integration process, permissions and limitations](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-integration-process)
- [Development process and branch/workspace strategy](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/manage-branches)
- [Cross-workspace dependency binding](https://learn.microsoft.com/en-us/fabric/cicd/cross-workspace-dependency-binding)
- [Fabric CI/CD best practices](https://learn.microsoft.com/en-us/fabric/fundamentals/understand-best-practices-fabric-cicd)
- [Deployment pipelines](https://learn.microsoft.com/en-us/fabric/cicd/deployment-pipelines/intro-to-deployment-pipelines)
- [Get started with deployment pipelines](https://learn.microsoft.com/en-us/fabric/cicd/deployment-pipelines/get-started-with-deployment-pipelines)
- [Deployment rules](https://learn.microsoft.com/en-us/fabric/cicd/deployment-pipelines/create-rules)
- [Variable libraries](https://learn.microsoft.com/en-us/fabric/cicd/variable-library/get-started-variable-libraries)
- [Variable libraries tutorial](https://learn.microsoft.com/en-us/fabric/cicd/variable-library/tutorial-variable-library)
- [SQL Database source control](https://learn.microsoft.com/en-us/fabric/database/sql/source-control)
- [SQL Database deployment pipelines](https://learn.microsoft.com/en-us/fabric/database/sql/deployment-pipelines)
- [SQL Database Projects extension for VS Code](https://learn.microsoft.com/en-us/sql/tools/visual-studio-code-extensions/sql-database-projects/getting-started-sql-database-projects-extension?view=sql-server-ver17)
- [SQL projects tools: DacFx, SqlPackage and VS Code](https://learn.microsoft.com/en-us/sql/tools/sql-database-projects/sql-projects-tools?view=sql-server-ver17)
- [SqlPackage Publish](https://learn.microsoft.com/en-us/sql/tools/sqlpackage/sqlpackage-publish?view=sql-server-ver17)
- [Create and deploy a SQL project](https://learn.microsoft.com/en-us/sql/tools/sql-database-projects/tutorials/create-deploy-sql-project?view=sql-server-ver17)
- [Warehouse source control (Preview)](https://learn.microsoft.com/en-us/fabric/data-warehouse/source-control)
- [CI/CD workflow options in Fabric](https://learn.microsoft.com/en-us/fabric/cicd/manage-deployment)

---

## 7. CÂU HỎI SCENARIO — LIFECYCLE MANAGEMENT

> Các câu hỏi dưới đây là câu tự soạn theo blueprint DP-700, **không phải câu hỏi thật hoặc exam dump**.

### Câu D1-L1 — Quyền Kết Nối Git

**Ai có quyền **tối thiểu** để kết nối (connect) workspace với Git repo lần đầu tiên?**

A. Viewer  
B. Contributor  
C. Member  
D. Admin  

<details><summary>Đáp án và giải thích</summary>

**D.** [Chỉ workspace Admin](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-integration-process#permissions) có quyền thực hiện thao tác initial connect, sync ban đầu, và disconnect workspace với Git. Sau khi đã kết nối, Contributor có đủ Fabric Write và Git permissions có thể commit/update các thay đổi tiếp theo.

</details>

---

### Câu D1-L2 — Contributor Và Git Operations

**Một Contributor có Write trên mọi item và có quyền Write trong Git repo. Workspace đã được Admin kết nối với Git trước đó. Contributor muốn cập nhật workspace từ branch Git mới nhất. Họ có thể làm điều này không?**

A. Không — mọi Git operation đều yêu cầu Admin  
B. Có — Contributor có đủ Fabric và Git permissions có thể thực hiện commit/update sau khi đã kết nối  
C. Không — vì GitHub chỉ hỗ trợ Admin  
D. Có — nhưng chỉ khi deployment pipeline có từ 10 stage trở lên  

<details><summary>Đáp án và giải thích</summary>

**B.** Sau khi Admin đã thiết lập kết nối, [Contributor có đủ quyền](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-integration-process#permissions) (Write trên Fabric item + quyền Git repo) có thể **commit thay đổi lên Git** và **update workspace từ Git** mà không cần nâng lên Admin. Đây là điểm bẫy phổ biến — tách biệt initial connect với ongoing operations.

</details>

---

### Câu D1-L3 — Deployment Pipeline Vs Git

**Mục tiêu là promote nội dung từ Dev → Test → Prod và **đổi connection string** tự động theo từng môi trường. Giải pháp nào phù hợp nhất?**

A. Chỉ dùng Git integration  
B. Deployment pipeline + Variable library (hoặc deployment rule nếu item/rule combination được hỗ trợ)  
C. OneLake shortcut  
D. Monitoring Hub  

<details><summary>Đáp án và giải thích</summary>

**B.** Deployment pipeline xử lý promotion giữa các stage. Với pipeline/configuration hiện đại, **Variable library** là lựa chọn chính để giữ value sets theo Dev/Test/Prod; deployment rules chỉ dùng khi đúng item/rule combination được support. Git quản lý version history/branches, không phải environment promotion.

</details>
