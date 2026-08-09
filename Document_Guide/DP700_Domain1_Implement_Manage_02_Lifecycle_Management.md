# DP-700: Implement Lifecycle Management in Fabric
## Implement and manage an analytics solution (30–35%)
### Skills measured as of July 21, 2026

> [!IMPORTANT]
> **Bản hiệu chỉnh ngày 04/08/2026.** Nội dung được đối chiếu với blueprint DP-700 có hiệu lực từ **21/07/2026**. Microsoft cho biết phần lớn câu hỏi tập trung vào tính năng GA, nhưng tính năng Preview phổ biến vẫn có thể xuất hiện. Với các bảng “supported items”, runtime, limits và trạng thái Preview/GA, hãy kiểm tra Microsoft Learn lần cuối trước ngày thi.


> **Tài liệu ôn thi DP-700: Fabric Data Engineer Associate**
> Phần này bao gồm toàn bộ kiến thức về quản lý vòng đời (lifecycle management) trong Microsoft Fabric.

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

ALM trong Microsoft Fabric bao gồm hai trụ cột chính:

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

Git Integration trong Fabric cho phép kết nối workspace với **Git repository** để quản lý version control cho các Fabric items.

### 2.2 Git Providers được hỗ trợ

| Provider | Hỗ trợ | Ghi chú |
|----------|--------|---------|
| **Azure DevOps (Azure Repos)** | ✅ GA | Fully supported |
| **GitHub / GitHub Enterprise cloud** | ✅ | Hỗ trợ cloud-based; quyền repo/token vẫn phải đáp ứng yêu cầu |
| **Bitbucket** | ❌ | Không hỗ trợ trực tiếp |
| **GitLab** | ❌ | Không hỗ trợ |

### 2.3 Fabric Items hỗ trợ Git Integration

| Item | Hỗ trợ | Ghi chú |
|------|--------|---------|
| **Notebooks** | ✅ | Full support |
| **Semantic Models (Datasets)** | ✅ | Full support |
| **Reports** | ✅ | Full support |
| **SQL Database** | ✅ | Full support |
| **KQL Database** | ✅ | Full support |
| **Lakehouse** | ✅ | Metadata only (không sync data files) |
| **Data Pipeline** | ✅ | Full support |
| **Dataflow Gen2** | ✅ | Full support |
| **Warehouse** | ✅ | Full support |
| **Environment** | ✅ | Full support |
| **Spark Job Definition** | ✅ | Full support |
| **ML Model** | ✅ | Full support |

> ⚠️ **Lưu ý thi:** Danh sách và mức độ hỗ trợ Git thay đổi nhanh, và một số item vẫn ở Preview. Tính đến 08/2026, provider hỗ trợ gồm Azure DevOps cloud, GitHub cloud và GitHub Enterprise cloud. Hãy học **nguyên tắc**: Git đồng bộ định nghĩa/metadata của item, không đồng bộ dữ liệu vật lý trong OneLake; kiểm tra live support matrix trước ngày thi.

### 2.4 Cách kết nối Workspace với Git

**Bước 1: Prerequisites**
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

Git Integration hỗ trợ **đồng bộ hai chiều**:

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

**Recommended Pattern:**

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

Khi có conflict (thay đổi ở cả workspace và Git):

1. **Workspace wins:** Giữ phiên bản workspace, commit đè lên Git
2. **Git wins:** Giữ phiên bản Git, update workspace từ Git
3. **Manual merge:** (Thường thực hiện bên ngoài Fabric, sử dụng Git tools)

### 2.8 Best Practices cho Git Integration

1. **Sử dụng branching strategy** rõ ràng (GitFlow hoặc trunk-based)
2. **Commit thường xuyên** với messages có ý nghĩa
3. **KHÔNG commit** sensitive data (credentials, connection strings)
4. **Git là source of truth** - workspace chỉ là môi trường làm việc
5. **Review changes** trước khi commit (code review qua Pull Requests)

---

## 3. IMPLEMENT DATABASE PROJECTS

### 3.1 Database Projects là gì?

**Database Projects** (SQL Database Projects) trong Fabric cho phép quản lý schema của SQL databases dưới dạng code (schema-as-code), hỗ trợ offline development và CI/CD.

### 3.2 Công cụ và Extensions cần thiết

| Công cụ | Mục đích |
|---------|----------|
| **Visual Studio Code** | IDE chính cho database development |
| **MSSQL Extension** | Kết nối và query SQL databases |
| **SQL Database Projects Extension** | Tạo và quản lý .sqlproj files |
| **DacFx (Data-Tier Application Framework)** | Build và deploy database packages |
| **SqlPackage** | CLI tool cho database deployment |

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

**Convert live database to code:**
1. Kết nối Fabric SQL Database với Git repository
2. Fabric tự động serialize database objects thành SQL scripts
3. Mỗi object (table, view, procedure) = một file .sql trong Git

**Update database from source control:**
1. Sửa đổi SQL scripts trong Git (thông qua PR)
2. Merge vào branch chính
3. Fabric detect changes và apply schema diff vào live database

### 3.6 Build và Deploy

**Build .dacpac package:**
```bash
# Sử dụng dotnet CLI
dotnet build MyDatabaseProject.sqlproj

# Output: bin/Debug/MyDatabaseProject.dacpac
```

**Deploy với SqlPackage:**
```bash
# Deploy dacpac tới Fabric SQL Database
SqlPackage /Action:Publish \
    /SourceFile:MyDatabaseProject.dacpac \
    /TargetConnectionString:"Server=<server>;Database=<db>;Authentication=Active Directory Default"
```

### 3.7 CI/CD Integration

**Azure DevOps Pipeline example:**
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

## 4. CREATE AND CONFIGURE DEPLOYMENT PIPELINES

### 4.1 Deployment Pipelines là gì?

**Deployment Pipelines** là công cụ release management trong Fabric cho phép promote content qua các stages (Development → Test → Production).

### 4.2 Cấu trúc Deployment Pipeline

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│              │     │              │     │              │
│ DEVELOPMENT  │ ──> │    TEST      │ ──> │ PRODUCTION   │
│              │     │              │     │              │
│ Dev-WS       │     │ Test-WS      │     │ Prod-WS      │
│              │     │              │     │              │
│ • Notebooks  │     │ • Notebooks  │     │ • Notebooks  │
│ • Pipelines  │     │ • Pipelines  │     │ • Pipelines  │
│ • Lakehouses │     │ • Lakehouses │     │ • Lakehouses │
│ • Reports    │     │ • Reports    │     │ • Reports    │
└──────────────┘     └──────────────┘     └──────────────┘
```

### 4.3 Yêu cầu để tạo Deployment Pipeline

| Yêu cầu | Chi tiết |
|----------|----------|
| **License** | Fabric capacity (F64+) hoặc Premium (P1+) |
| **Role** | Workspace Admin cho TẤT CẢ stages |
| **Tenant setting** | "Users can create and use deployment pipelines" phải enabled |
| **Stages** | 2-10 stages (mặc định 3: Dev, Test, Prod) |

### 4.4 Tạo Deployment Pipeline

**Bước 1: Tạo pipeline**
1. Fabric Portal → chọn workspace
2. Click **"Deployment pipelines"** từ menu bên trái
3. Đặt tên cho pipeline
4. Chọn số lượng stages (mặc định 3)

**Bước 2: Assign workspaces**
1. Gán workspace hiện có cho mỗi stage
2. Hoặc tạo workspace mới cho stage

**Bước 3: Deploy content**
1. So sánh nội dung giữa các stages
2. Chọn items cần deploy
3. Click Deploy

### 4.5 Deployment Rules

**Deployment Rules** cho phép thay đổi cấu hình khi deploy từ stage này sang stage khác, ví dụ: kết nối tới database khác nhau cho mỗi environment.

#### Các loại Deployment Rules:

| Loại Rule | Áp dụng cho | Mô tả |
|-----------|-------------|--------|
| **Data source rules** | Semantic models, Dataflows | Thay đổi data source connection (server, database name) |
| **Parameter rules** | Semantic models | Thay đổi parameter values |
| **Connection rules** | Pipelines, Notebooks | Thay đổi connection endpoints |

**Ví dụ Data Source Rule:**
```
Dev Stage:   Server = dev-server.database.fabric.microsoft.com
Test Stage:  Server = test-server.database.fabric.microsoft.com
Prod Stage:  Server = prod-server.database.fabric.microsoft.com
```

### 4.6 Comparison và Diff

Trước khi deploy, bạn có thể so sánh content giữa hai stages:

| Status Icon | Ý nghĩa |
|-------------|---------|
| 🟢 **Same** | Item giống nhau ở cả hai stages |
| 🟡 **Different** | Item đã thay đổi, cần deploy |
| 🔴 **New** | Item chỉ tồn tại ở source stage |
| ⚪ **Missing** | Item chỉ tồn tại ở target stage |

### 4.7 Backward Deployment

- Deployment pipelines cho phép deploy **ngược** (backward): Test → Dev, Prod → Test
- Hữu ích cho **hotfix scenarios**: sửa lỗi ở Production → deploy ngược về Test → Dev
- Cần cẩn thận để tránh ghi đè work-in-progress ở lower stages

### 4.8 Automate Deployment với APIs

```python
# Sử dụng Fabric REST API để automate deployment
import requests

# Get deployment pipeline stages
url = "https://api.fabric.microsoft.com/v1/deploymentPipelines/{pipelineId}/stages"
headers = {"Authorization": f"Bearer {access_token}"}

response = requests.get(url, headers=headers)
stages = response.json()

# Deploy from Dev to Test
deploy_url = f"https://api.fabric.microsoft.com/v1/deploymentPipelines/{pipelineId}/deploy"
deploy_body = {
    "sourceStageOrder": 0,  # Dev = 0
    "targetStageOrder": 1,  # Test = 1
    "items": [
        {"itemId": "item-id-1"},
        {"itemId": "item-id-2"}
    ],
    "options": {
        "allowCreateItem": True,
        "allowOverwriteItem": True
    }
}

response = requests.post(deploy_url, headers=headers, json=deploy_body)
```

### 4.9 Deployment Pipelines vs Git Integration

| Tiêu chí | Deployment Pipelines | Git Integration |
|----------|---------------------|-----------------|
| **Mục đích** | Release management (promote content) | Source control (version tracking) |
| **Workflow** | Stage-based (Dev → Test → Prod) | Branch-based (feature → develop → main) |
| **Content** | Clone items giữa workspaces | Serialize items thành code files |
| **Rules** | Deployment rules (change connections) | Không có built-in rules |
| **Collaboration** | Limited (stage-based approval) | Full (PRs, code review, branching) |
| **Best for** | Content promotion | Version control, collaboration |

> 💡 **Mẹo thi:** Git Integration và Deployment Pipelines **bổ sung** cho nhau. Best practice: Sử dụng Git cho source control + Deployment Pipelines cho release management.

### 4.10 Ví dụ minh họa thực tế

**Tình huống:** Công ty cần triển khai quy trình CI/CD cho data engineering team.

**Giải pháp hoàn chỉnh:**

```
Developer Workflow:
1. Clone Git repo → create feature branch
2. Develop in feature workspace (VS Code / Fabric Portal)
3. Commit changes to Git
4. Create Pull Request → Code Review
5. Merge to 'develop' branch
6. Dev workspace syncs from Git

Deployment Workflow:
1. Dev workspace (Stage 1) → Compare with Test
2. Deploy selected items to Test workspace (Stage 2)
3. Apply deployment rules (change connections to test DB)
4. QA team verifies in Test workspace
5. Deploy from Test to Production (Stage 3)
6. Apply production deployment rules
7. Monitor production
```

### 4.11 Items hỗ trợ Deployment Pipelines

| Item | Hỗ trợ | Deployment Rules |
|------|--------|-----------------|
| **Semantic Model** | ✅ | Data source, Parameters |
| **Report** | ✅ | N/A (inherits from semantic model) |
| **Dashboard** | ✅ | N/A |
| **Paginated Report** | ✅ | Data source |
| **Dataflow Gen2** | ✅ | Data source |
| **Data Pipeline** | ✅ | Connection |
| **Notebook** | ✅ | N/A |
| **Lakehouse** | ✅ | N/A (metadata only) |
| **Warehouse** | ✅ | N/A |
| **SQL Database** | ✅ | Connection |
| **KQL Database** | ✅ | N/A |

---

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
|--------|----------------|
| **Git Providers** | Azure DevOps + GitHub (supported); Bitbucket/GitLab (NOT supported) |
| **Git Sync** | Bi-directional: Commit (WS→Git) + Update (Git→WS) |
| **Git Role** | Admin để connect/sync/disconnect; Contributor có đủ Write + Git permissions để commit/update sau khi đã kết nối |
| **Database Projects** | VS Code + SQL Projects ext + DacFx/SqlPackage |
| **Database Workflow** | .sqlproj → T-SQL scripts → Git → Build .dacpac → Deploy |
| **Deployment Pipelines** | 2-10 stages, default 3 (Dev/Test/Prod) |
| **Deployment Rules** | Data source, Parameters, Connection rules |
| **Pipeline Role** | Workspace Admin cho TẤT CẢ stages |
| **Pipeline vs Git** | Bổ sung nhau: Git = source control, Pipeline = release management |
| **Backward Deploy** | Có thể deploy ngược (Prod→Test, Test→Dev) |

---

> 📖 **Tài liệu tham khảo chính thức:**
> - [Git Integration in Fabric](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/intro-to-git-integration)
> - [Deployment Pipelines](https://learn.microsoft.com/en-us/fabric/cicd/deployment-pipelines/intro-to-deployment-pipelines)
> - [SQL Database Projects](https://learn.microsoft.com/en-us/fabric/database/sql/source-control)
> - [Lifecycle Management Overview](https://learn.microsoft.com/en-us/fabric/cicd/cicd-overview)


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

- [DP-700 study guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)
- [Git integration process and permissions](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-integration-process)
- [Git supported items/providers](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/intro-to-git-integration)
- [Deployment pipelines](https://learn.microsoft.com/en-us/fabric/cicd/deployment-pipelines/intro-to-deployment-pipelines)
- [Fabric CI/CD best practices](https://learn.microsoft.com/en-us/fabric/fundamentals/understand-best-practices-fabric-cicd)
