# DP-700: Configure Security and Governance
## Implement and manage an analytics solution (30–35%)
### Skills measured as of July 21, 2026

> [!IMPORTANT]
> **Bản hiệu chỉnh ngày 04/08/2026.** Nội dung được đối chiếu với blueprint DP-700 có hiệu lực từ **21/07/2026**. Microsoft cho biết phần lớn câu hỏi tập trung vào tính năng GA, nhưng tính năng Preview phổ biến vẫn có thể xuất hiện. Với các bảng “supported items”, runtime, limits và trạng thái Preview/GA, hãy kiểm tra Microsoft Learn lần cuối trước ngày thi.


> **Tài liệu ôn thi DP-700: Fabric Data Engineer Associate**
> Phần này bao gồm toàn bộ kiến thức về bảo mật và quản trị (security & governance) trong Microsoft Fabric.

---

## 📋 MỤC LỤC

1. [Tổng quan mô hình bảo mật Fabric](#1-tổng-quan-mô-hình-bảo-mật-fabric)
2. [Implement Workspace-Level Access Controls](#2-implement-workspace-level-access-controls)
3. [Implement Item-Level Access Controls](#3-implement-item-level-access-controls)
4. [Implement Row-Level, Column-Level, Object-Level, and Folder/File-Level Access Controls](#4-implement-granular-access-controls)
5. [Implement Dynamic Data Masking](#5-implement-dynamic-data-masking)
6. [Apply Sensitivity Labels to Items](#6-apply-sensitivity-labels-to-items)
7. [Endorse Items](#7-endorse-items)
8. [Implement and Use Microsoft Fabric Audit Logs](#8-implement-and-use-microsoft-fabric-audit-logs)
9. [Configure and Implement OneLake Security](#9-configure-and-implement-onelake-security)
10. [Câu hỏi mô phỏng bài thi (Practice Questions)](#10-câu-hỏi-mô-phỏng-bài-thi)

---

## 1. TỔNG QUAN MÔ HÌNH BẢO MẬT FABRIC

### 1.1 Security Layers (Các lớp bảo mật)

Microsoft Fabric sử dụng mô hình bảo mật **đa lớp** (layered security model):

```
┌─────────────────────────────────────────────────────┐
│  Layer 1: TENANT LEVEL                               │
│  • Tenant settings (Admin Portal)                    │
│  • Microsoft Entra ID authentication                 │
├─────────────────────────────────────────────────────┤
│  Layer 2: WORKSPACE LEVEL                            │
│  • Workspace roles (Admin, Member, Contributor,      │
│    Viewer)                                           │
├─────────────────────────────────────────────────────┤
│  Layer 3: ITEM LEVEL                                 │
│  • Share items, Per-item permissions                 │
│  • Read, ReadAll, Write permissions                  │
├─────────────────────────────────────────────────────┤
│  Layer 4: DATA LEVEL (OneLake Security)              │
│  • Row-Level Security (RLS)                          │
│  • Column-Level Security (CLS)                       │
│  • Object-Level Security (OLS)                       │
│  • Folder/File-Level Security                        │
│  • Dynamic Data Masking (DDM)                        │
├─────────────────────────────────────────────────────┤
│  Layer 5: INFORMATION PROTECTION                     │
│  • Sensitivity labels (Microsoft Purview)            │
│  • Endorsement (Certified, Promoted)                 │
└─────────────────────────────────────────────────────┘
```

### 1.2 Nguyên tắc cơ bản

- **Principle of Least Privilege:** Cấp quyền tối thiểu cần thiết
- **Defense in Depth:** Nhiều lớp bảo mật chồng lên nhau
- **Centralized Governance:** OneLake Security = "define once, enforce everywhere"

---

## 2. IMPLEMENT WORKSPACE-LEVEL ACCESS CONTROLS

### 2.1 Workspace Roles chi tiết

| Quyền | Admin | Member | Contributor | Viewer |
|--------|:-----:|:------:|:-----------:|:------:|
| Xem items | ✅ | ✅ | ✅ | ✅ |
| Đọc dữ liệu | ✅ | ✅ | ✅ | ✅ |
| Tạo items | ✅ | ✅ | ✅ | ❌ |
| Chỉnh sửa items | ✅ | ✅ | ✅ | ❌ |
| Xóa items | ✅ | ✅ | ✅ | ❌ |
| Share items | ✅ | ✅ | ❌ | ❌ |
| Thêm members (không phải Admin) | ✅ | ✅ | ❌ | ❌ |
| Publish apps | ✅ | ✅ | ❌ | ❌ |
| Quản lý permissions | ✅ | ❌ | ❌ | ❌ |
| Thêm/xóa Admins | ✅ | ❌ | ❌ | ❌ |
| Thay đổi workspace settings | ✅ | ❌ | ❌ | ❌ |
| Xóa workspace | ✅ | ❌ | ❌ | ❌ |

### 2.2 Gán Workspace Roles

**Qua UI:**
1. Workspace > Manage access
2. Click "Add people or groups"
3. Nhập email/group name
4. Chọn role
5. Click Add

**Qua REST API:**
```http
POST https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/roleAssignments
Content-Type: application/json

{
    "principal": {
        "id": "user-or-group-object-id",
        "type": "User"  // hoặc "Group", "ServicePrincipal"
    },
    "role": "Contributor"  // Admin, Member, Contributor, Viewer
}
```

### 2.3 Best Practices

1. **Sử dụng Security Groups** thay vì individual users → dễ quản lý
2. **Viewer role** cho end users chỉ cần xem reports
3. **Contributor role** cho developers cần chỉnh sửa items
4. **Admin role** chỉ cho workspace administrators

> ⚠️ **Quan trọng cho thi:** Admin, Member, Contributor roles **bypass OneLake Security**. Nếu bạn muốn áp dụng granular security (RLS, CLS), user phải có **Viewer** role hoặc chỉ có specific item-level permissions.

---

## 3. IMPLEMENT ITEM-LEVEL ACCESS CONTROLS

### 3.1 Item-Level Sharing

Cho phép share individual items (reports, semantic models, lakehouses) mà KHÔNG cần cấp workspace role.

**Cách share:**
1. Right-click item → Share
2. Nhập email/group name
3. Chọn permissions:
   - **Read:** Xem item
   - **ReadAll:** Đọc dữ liệu qua SQL endpoint (cho Lakehouse/Warehouse)
   - **Write:** Chỉnh sửa item
   - **Reshare:** Cho phép user share tiếp cho người khác

### 3.2 Permission Types theo Item

| Item | Read | ReadAll | Write | Build | Reshare |
|------|:----:|:-------:|:-----:|:-----:|:-------:|
| **Report** | ✅ | N/A | ✅ | N/A | ✅ |
| **Semantic Model** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Lakehouse** | ✅ | ✅ | ✅ | N/A | ✅ |
| **Warehouse** | ✅ | ✅ | ✅ | N/A | ✅ |
| **Notebook** | ✅ | N/A | ✅ | N/A | ✅ |
| **Pipeline** | ✅ | N/A | ✅ | N/A | ✅ |

### 3.3 ReadAll Permission

- **Đặc biệt quan trọng** cho Lakehouse và Warehouse
- Cho phép user kết nối qua **SQL analytics endpoint** để query dữ liệu
- Nếu chỉ có Read (không có ReadAll) → user chỉ thấy metadata, không thể query data
- ReadAll + OneLake Security = kiểm soát dữ liệu nào user thực sự thấy

### 3.4 Build Permission (Semantic Models)

- Cho phép user tạo reports dựa trên semantic model
- User không cần workspace access
- Quan trọng cho **self-service BI** scenarios

### 3.5 Ví dụ minh họa

**Tình huống:** Finance team cần xem Sales Report nhưng KHÔNG nên truy cập workspace.

**Giải pháp:**
1. Share Sales Report với Finance group → Grant "Read" permission
2. Tự động grant "Read" trên underlying Semantic Model
3. KHÔNG cấp workspace role → Finance team chỉ thấy shared items

---

## 4. IMPLEMENT GRANULAR ACCESS CONTROLS

### 4.1 Row-Level Security (RLS)

**RLS** giới hạn hàng dữ liệu mà user có thể xem dựa trên identity/role.

#### Triển khai RLS trong Semantic Model (DAX)

**Bước 1: Tạo Role**
```dax
-- Trong Power BI / Semantic Model, tạo role:
-- Role name: RegionalSalesRole
-- Table: DimSalesTerritory
-- Filter expression:
[Region] = USERPRINCIPALNAME()
```

**Bước 2: Sử dụng DAX functions**
```dax
-- USERNAME() - trả về domain\username
-- USERPRINCIPALNAME() - trả về email (user@domain.com)
-- CUSTOMDATA() - trả về custom data string

-- Ví dụ: Dynamic RLS
[SalesRegion] = LOOKUPVALUE(
    UserMapping[Region],
    UserMapping[UserEmail],
    USERPRINCIPALNAME()
)
```

**Bước 3: Assign users to roles**
- Fabric Portal → Semantic Model → Security → Manage roles
- Thêm users/groups vào roles

#### Triển khai RLS trong SQL (Lakehouse SQL Endpoint / Warehouse)

```sql
-- Bước 1: Tạo function
CREATE FUNCTION dbo.fn_SecurityPredicate(@Region AS NVARCHAR(50))
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN SELECT 1 AS result
WHERE @Region = SESSION_CONTEXT(N'user_region')
   OR IS_MEMBER('db_owner') = 1;

-- Bước 2: Tạo Security Policy
CREATE SECURITY POLICY dbo.RegionSecurityPolicy
ADD FILTER PREDICATE dbo.fn_SecurityPredicate(Region)
ON dbo.FactSales
WITH (STATE = ON);
```

#### Triển khai RLS trong OneLake Security

```
OneLake Security Role:
├── Name: SalesViewerRole
├── Type: Row-Level Security
├── Table: fact_sales
├── Filter: [region] = @user_attribute.region
└── Members: SalesTeam@company.com
```

### 4.2 Column-Level Security (CLS)

**CLS** giới hạn cột nào user có thể xem.

#### Triển khai CLS trong SQL

```sql
-- Cấp quyền SELECT chỉ trên specific columns
GRANT SELECT ON dbo.EmployeeData 
    (EmployeeID, FirstName, LastName, Department) 
TO [AnalystRole];

-- AnalystRole KHÔNG thể xem: Salary, SSN, BankAccount
-- Nếu query SELECT * FROM EmployeeData → ERROR cho restricted columns
```

#### Triển khai CLS trong OneLake Security

```
OneLake Security Role:
├── Name: RestrictedAnalyst
├── Type: Column-Level Security
├── Table: dim_employee
├── Allowed Columns: [employee_id, name, department, hire_date]
├── Restricted Columns: [salary, ssn, bank_account]
└── Members: Analysts@company.com
```

### 4.3 Object-Level Security (OLS)

**OLS** giới hạn quyền truy cập vào toàn bộ objects (tables, views).

#### Triển khai OLS trong Semantic Model (Tabular Editor)

```json
// Trong Tabular Editor:
// Table: ConfidentialFinanceData
// Object-Level Security: None / Read / Default

// Bước 1: Tạo Role
{
    "name": "LimitedAnalyst",
    "tablePermissions": [
        {
            "name": "ConfidentialFinanceData",
            "metadataPermission": "None"  
            // None = table bị ẩn hoàn toàn
        }
    ]
}
```

#### Triển khai OLS trong SQL

```sql
-- DENY SELECT trên entire table
DENY SELECT ON OBJECT::dbo.SensitiveData TO [LimitedRole];

-- Hoặc GRANT SELECT chỉ cho specific tables
GRANT SELECT ON OBJECT::dbo.PublicData TO [LimitedRole];
```

### 4.4 Folder/File-Level Security

**Folder/File-Level Security** kiểm soát truy cập tới folders và files trong OneLake.

#### Triển khai trong OneLake Security

```
OneLake Security Role:
├── Name: DataScienceTeam
├── Type: Folder-Level Security
├── Lakehouse: Sales-Lakehouse
├── Allowed Folders:
│   ├── Tables/fact_sales
│   ├── Tables/dim_customer
│   └── Files/ml_features/
├── Denied Folders:
│   ├── Tables/sensitive_finance
│   └── Files/raw_pii/
└── Members: DataScience@company.com
```

**Cách cấu hình qua UI:**
1. Mở Lakehouse → Manage OneLake data access
2. Tạo new role
3. Chọn folders/tables để grant access
4. Assign users/groups
5. Apply

### 4.5 So sánh các loại Access Controls

| Loại | Phạm vi | Áp dụng tại | Engine |
|------|---------|-------------|--------|
| **RLS** | Hàng dữ liệu | Semantic Model / SQL / OneLake | Tất cả |
| **CLS** | Cột dữ liệu | SQL / OneLake | SQL endpoint, Spark, Direct Lake |
| **OLS** | Table/Object | Semantic Model / SQL | Tất cả |
| **Folder/File** | Folders trong OneLake | OneLake Security | Spark, SQL endpoint, Direct Lake |

> 💡 **Mẹo thi:** OneLake Security theo mô hình **"define once, enforce consistently"**. Chính sách được thực thi bởi các Fabric engines được hỗ trợ và authorized engines; engine không được ủy quyền có thể bị chặn. Không nên diễn giải là mọi engine bên ngoài đều tự động hiểu RLS/CLS.

---

## 5. IMPLEMENT DYNAMIC DATA MASKING

### 5.1 Dynamic Data Masking (DDM) là gì?

DDM **che giấu dữ liệu nhạy cảm** trong kết quả query cho non-privileged users mà KHÔNG thay đổi dữ liệu gốc.

### 5.2 Các loại Masking Functions

| Function | Mô tả | Input | Output |
|----------|--------|-------|--------|
| **Default** | Che hoàn toàn | "Hello" | "XXXX" |
| **Email** | Che email | "user@domain.com" | "uXXX@XXXX.com" |
| **Random** | Số ngẫu nhiên trong range | 123 | 87 (random) |
| **Partial** | Hiện ký tự đầu/cuối | "0123456789" | "01XXXXX89" |
| **Custom String** | Thay bằng chuỗi custom | "Sensitive" | "****" |

### 5.3 Triển khai DDM trong Fabric SQL

#### Tạo mask trên column mới:
```sql
CREATE TABLE dbo.CustomerData (
    CustomerID    INT NOT NULL PRIMARY KEY,
    FirstName     NVARCHAR(100) MASKED WITH (FUNCTION = 'partial(2, "XXX", 0)') NOT NULL,
    LastName      NVARCHAR(100) NOT NULL,
    Email         NVARCHAR(255) MASKED WITH (FUNCTION = 'email()') NOT NULL,
    Phone         NVARCHAR(20)  MASKED WITH (FUNCTION = 'default()') NULL,
    SSN           CHAR(11)      MASKED WITH (FUNCTION = 'partial(0, "XXX-XX-", 4)') NOT NULL,
    CreditScore   INT           MASKED WITH (FUNCTION = 'random(100, 999)') NULL
);
```

#### Thêm mask vào column existing:
```sql
ALTER TABLE dbo.CustomerData
ALTER COLUMN Phone ADD MASKED WITH (FUNCTION = 'default()');
```

#### Xóa mask:
```sql
ALTER TABLE dbo.CustomerData
ALTER COLUMN Phone DROP MASKED;
```

### 5.4 UNMASK Permission

```sql
-- Cấp quyền UNMASK cho specific user
GRANT UNMASK ON dbo.CustomerData TO [TrustedAnalyst];

-- Cấp UNMASK cho toàn bộ database
GRANT UNMASK TO [DataAdmin];

-- Revoke UNMASK
REVOKE UNMASK ON dbo.CustomerData FROM [TrustedAnalyst];
```

### 5.5 DDM và Workspace Roles

| Role | Thấy masked data? |
|------|:------------------:|
| **Admin** | ❌ (thấy dữ liệu gốc) |
| **Member** | ❌ (thấy dữ liệu gốc) |
| **Contributor** | ❌ (thấy dữ liệu gốc) |
| **Viewer** (không có UNMASK) | ✅ (thấy data masked) |
| **Viewer** (có UNMASK) | ❌ (thấy dữ liệu gốc) |

> ⚠️ **Quan trọng cho thi:** DDM chỉ effective cho users KHÔNG CÓ elevated permissions (Admin/Member/Contributor bypass DDM). Chỉ Viewer hoặc users với limited SQL permissions mới bị mask.

### 5.6 DDM và Power BI

- Khi sử dụng **Direct Lake** mode với DDM → Power BI có thể fallback sang **DirectQuery** mode để enforce masking
- **Import Mode** → DDM KHÔNG được enforce vì data đã được imported vào memory
- Best practice: Sử dụng Direct Lake hoặc DirectQuery khi cần DDM enforcement

### 5.7 Ví dụ minh họa

```
Query bởi User KHÔNG có UNMASK permission:

SELECT FirstName, Email, Phone, SSN FROM dbo.CustomerData;

Kết quả:
| FirstName | Email            | Phone          | SSN            |
|-----------|------------------|----------------|----------------|
| JoXXX     | jXXX@XXXX.com   | xxxxxxxxxxxx   | XXX-XX-4532    |
| MaXXX     | mXXX@XXXX.com   | xxxxxxxxxxxx   | XXX-XX-7891    |

Query bởi User CÓ UNMASK permission:

| FirstName | Email              | Phone          | SSN            |
|-----------|--------------------|----------------|----------------|
| John      | john@company.com   | 555-123-4567   | 123-45-4532    |
| Mary      | mary@company.com   | 555-987-6543   | 456-78-7891    |
```

---

## 6. APPLY SENSITIVITY LABELS TO ITEMS

### 6.1 Sensitivity Labels là gì?

**Sensitivity Labels** từ **Microsoft Purview Information Protection** được áp dụng lên Fabric items để phân loại và bảo vệ dữ liệu theo chính sách tổ chức.

### 6.2 Các mức Sensitivity Labels phổ biến

| Label | Mô tả | Ví dụ áp dụng |
|-------|--------|----------------|
| **Public** | Dữ liệu công khai | Annual reports, marketing materials |
| **General** | Dữ liệu nội bộ | Internal communications, training docs |
| **Confidential** | Dữ liệu bí mật | Financial data, employee records |
| **Highly Confidential** | Tối mật | PII, trade secrets, M&A data |

### 6.3 Cách áp dụng Sensitivity Labels

**Thủ công:**
1. Right-click item → Settings
2. Trong Sensitivity label section → Chọn label
3. Save

**Tự động:**
- Auto-labeling policies trong Microsoft Purview
- Downstream inheritance (label tự động truyền từ data source → downstream items)
- Default labels (tenant hoặc domain level)

### 6.4 Sensitivity Label Inheritance

```
Upstream (Data Source)              Downstream (Reports)
┌───────────────┐                  ┌───────────────┐
│ Lakehouse     │                  │ Report        │
│ Label: Highly │  ──inherits──>   │ Label: Highly │
│ Confidential  │                  │ Confidential  │
└───────────────┘                  └───────────────┘
        │
        v
┌───────────────┐
│ Semantic Model│
│ Label: Highly │
│ Confidential  │
└───────────────┘
```

> 💡 **Mẹo thi:** Sensitivity labels **downstream inheritance** tự động lan truyền labels từ data source xuống các items phụ thuộc. Đây là cơ chế "label flows with data".

### 6.5 Export Controls

Sensitivity labels có thể kiểm soát **export behavior**:
- Chặn export tới Excel/PDF nếu label là "Highly Confidential"
- Yêu cầu encryption khi export
- Áp dụng watermark trên exported documents

### 6.6 Prerequisites cho Sensitivity Labels

1. **Microsoft Purview Information Protection** license
2. Tenant setting: "Allow users to apply sensitivity labels for Fabric content" = **Enabled**
3. Labels phải được **published** từ Microsoft Purview Compliance Portal
4. Users phải có label published tới họ (through label policy)

---

## 7. ENDORSE ITEMS

### 7.1 Endorsement là gì?

**Endorsement** giúp users xác định items đáng tin cậy và chất lượng cao trong tổ chức.

### 7.2 Các mức Endorsement

| Mức | Icon | Ai có thể đặt? | Mục đích |
|-----|------|-----------------|----------|
| **Promoted** | 🔵 Badge | Workspace Member, Contributor, Admin | Items đã sẵn sàng cho wider use |
| **Certified** | 🟢 Badge | Chỉ users được Fabric Admin authorize | Items đã qua review chất lượng chính thức |
| **No endorsement** | Không có | Mặc định | Items chưa được review |

### 7.3 Cách Endorse Items

**Promote:**
1. Right-click item → Settings → Endorsement
2. Chọn "Promoted"
3. (Tùy chọn) Thêm description
4. Apply

**Certify:**
1. Phải được Fabric Admin authorize (tenant setting)
2. Right-click item → Settings → Endorsement
3. Chọn "Certified"
4. Apply

### 7.4 Cấu hình Certification Authorization

**Đường dẫn:** Admin Portal → Tenant settings → Certification

| Setting | Mô tả |
|---------|--------|
| **Enable certification** | Bật/tắt tính năng certification |
| **Specify users or groups** | Chỉ định ai có thể certify items |
| **Certification documentation URL** | Link tới quy trình certification nội bộ |

### 7.5 Endorsement trong Data Discovery

- **OneLake Data Hub** hiển thị badges endorsement
- Users có thể **filter** items theo endorsement level
- Certified items xuất hiện **đầu tiên** trong search results
- Endorsement giúp **build trust** trong self-service BI environment

### 7.6 Ví dụ minh họa

```
OneLake Data Hub:
├── 🟢 Sales Performance Report [CERTIFIED]
│   └── Reviewed and approved by Data Governance Team
├── 🔵 Customer Segmentation Model [PROMOTED]
│   └── Ready for wider team use
├──    Revenue Forecast v3 [NO ENDORSEMENT]
│   └── Work in progress
└── 🔵 Inventory Dashboard [PROMOTED]
    └── Shared by Operations team
```

---

## 8. IMPLEMENT AND USE MICROSOFT FABRIC AUDIT LOGS

### 8.1 Audit Logs là gì?

**Fabric Audit Logs** theo dõi tất cả hoạt động trong tenant, bao gồm ai làm gì, khi nào, và trên item nào.

### 8.2 Nơi xem Audit Logs

| Nơi | Cách truy cập |
|-----|---------------|
| **Microsoft Purview Compliance Portal** | purview.microsoft.com → Audit |
| **Microsoft 365 Admin Center** | admin.microsoft.com → Audit |
| **PowerShell** | Search-UnifiedAuditLog cmdlet |
| **Office 365 Management API** | REST API calls |
| **Fabric Monitoring Hub** | Fabric Portal → Monitoring Hub |

### 8.3 Các hoạt động được audit (Activities)

#### Workspace Activities:
| Activity | Operation Name |
|----------|---------------|
| Tạo workspace | CreateFolder |
| Xóa workspace | DeleteFolder |
| Thay đổi workspace access | UpdateFolderAccess |
| Gán workspace vào domain | UpdateDataDomainFoldersRelationsAsAdmin |

#### Data Activities:
| Activity | Operation Name |
|----------|---------------|
| Xem report | ViewReport |
| Export report | ExportReport |
| Share item | ShareReport |
| Download data | DownloadReport |
| Refresh dataset | RefreshDataset |

#### Admin Activities:
| Activity | Operation Name |
|----------|---------------|
| Thay đổi tenant settings | SetTenantSettings |
| Thay đổi capacity settings | SetCapacitySettings |
| Thêm/xóa admin | AddAdmin, DeleteAdmin |

### 8.4 Truy xuất Audit Logs bằng PowerShell

```powershell
# Kết nối Exchange Online
Connect-ExchangeOnline -UserPrincipalName admin@company.com

# Tìm kiếm audit logs (ví dụ: tìm tất cả report views trong 7 ngày qua)
$startDate = (Get-Date).AddDays(-7)
$endDate = Get-Date

Search-UnifiedAuditLog `
    -StartDate $startDate `
    -EndDate $endDate `
    -RecordType PowerBIAudit `
    -Operations "ViewReport" `
    -ResultSize 5000 |
    Select-Object CreationDate, UserIds, Operations, AuditData |
    Export-Csv -Path "FabricAuditLogs.csv" -NoTypeInformation
```

### 8.5 Truy xuất qua REST API

```python
# Office 365 Management API
import requests

url = "https://manage.office.com/api/v1.0/{tenant_id}/activity/feed/subscriptions/content"
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}
params = {
    "contentType": "Audit.General",
    "startTime": "2026-07-01T00:00:00",
    "endTime": "2026-07-31T23:59:59"
}

response = requests.get(url, headers=headers, params=params)
audit_data = response.json()
```

### 8.6 Monitoring Hub

**Fabric Monitoring Hub** cung cấp real-time monitoring:
- Xem activities hiện tại và gần đây
- Filter theo item type, user, status
- Drill vào chi tiết từng activity
- KHÔNG thay thế cho Audit Logs (Monitoring Hub = operational, Audit Logs = compliance)

### 8.7 Best Practices cho Audit Logs

1. **Enable Audit Logging** ở tenant level (mặc định đã bật cho Microsoft 365)
2. **Set up automatic export** tới Azure Log Analytics hoặc Event Hub cho long-term retention
3. **Create alerts** cho suspicious activities (nhiều export trong thời gian ngắn, unauthorized access attempts)
4. **Regular review** audit logs theo schedule (weekly/monthly)
5. **Retention policy:** Mặc định 90 ngày, có thể extend tới 10 năm với proper license

---

## 9. CONFIGURE AND IMPLEMENT ONELAKE SECURITY

### 9.1 OneLake Security Overview

**OneLake Security** là hệ thống centralized security cho phép định nghĩa access controls một lần và thực thi trên các Fabric engines được hỗ trợ hoặc authorized engines. Phạm vi item/engine và limitations phải được kiểm tra theo tài liệu hiện hành.

### 9.2 Security Modes

| Mode | Mô tả | Access Control |
|------|--------|----------------|
| **Delegated Identity Mode** | SQL endpoint connect to OneLake bằng workspace/item owner identity | SQL permissions (GRANT, REVOKE, RLS, CLS, DDM) |
| **User Identity Mode** | SQL endpoint pass through user's identity to OneLake | OneLake roles và policies |

### 9.3 OneLake Security Roles

**Tạo OneLake Security Role:**

```
OneLake Security → Create Role:
├── Role Name: "SalesAnalyst"
├── Security Type: Folder-Level
├── Permissions:
│   ├── Tables/
│   │   ├── fact_sales → ✅ Read
│   │   ├── dim_customer → ✅ Read
│   │   ├── dim_product → ✅ Read
│   │   └── fact_hr_payroll → ❌ No Access
│   └── Files/
│       ├── reports/ → ✅ Read
│       └── raw_sensitive/ → ❌ No Access
├── Row Filters:
│   └── fact_sales: [region] = 'North America'
├── Column Restrictions:
│   └── dim_customer: HIDE [ssn], [credit_card]
└── Members:
    ├── SalesTeam@company.com
    └── RegionalManagers@company.com
```

### 9.4 Enforcement Across Engines

OneLake Security được enforce nhất quán trên:

| Engine | Enforcement |
|--------|-------------|
| **Spark** (Notebooks, Spark Jobs) | ✅ |
| **SQL Analytics Endpoint** | ✅ |
| **Power BI Direct Lake** | ✅ |
| **OneLake APIs** (ADLS Gen2) | ✅ |
| **Shortcuts** (Internal) | ✅ |

### 9.5 OneLake Security và Workspace Roles

```
                    ┌─────────────────────────────┐
                    │        OneLake Security       │
                    │     ┌─────────────────┐      │
                    │     │  Security Roles  │      │
                    │     │  (RLS/CLS/FLS)  │      │
                    │     └────────┬────────┘      │
                    │              │                │
                    │     Enforced for:             │
                    │     • Viewer role             │
                    │     • Item-level shared users │
                    │              │                │
                    │     BYPASSED by:              │
                    │     • Admin role              │
                    │     • Member role             │
                    │     • Contributor role         │
                    └─────────────────────────────┘
```

### 9.6 Security Propagation qua Shortcuts

```
Source Lakehouse (OneLake Security: RLS applied)
    │
    ├── Internal Shortcut ──> Target Lakehouse
    │                         (RLS automatically enforced)
    │
    └── External Shortcut ──> ADLS Gen2
                              (Passthrough auth: user needs
                               ADLS permissions)
```

### 9.7 Ví dụ triển khai hoàn chỉnh

**Tình huống:** Công ty cần áp dụng security model phức tạp cho Sales Lakehouse.

```
Sales-Lakehouse Security Configuration:
┌─────────────────────────────────────────────────────┐
│ Role 1: SalesManager_NorthAmerica                    │
│ • Folders: fact_sales ✅, dim_customer ✅,            │
│           dim_product ✅                              │
│ • RLS: fact_sales WHERE region = 'NA'               │
│ • CLS: dim_customer HIDE [ssn, phone]               │
│ • Members: NAManagers@company.com                    │
├─────────────────────────────────────────────────────┤
│ Role 2: SalesManager_EMEA                            │
│ • Folders: fact_sales ✅, dim_customer ✅             │
│ • RLS: fact_sales WHERE region = 'EMEA'             │
│ • CLS: dim_customer HIDE [ssn, phone]               │
│ • Members: EMEAManagers@company.com                  │
├─────────────────────────────────────────────────────┤
│ Role 3: DataScientist                                │
│ • Folders: fact_sales ✅, dim_customer ✅,            │
│           dim_product ✅, ml_features/ ✅             │
│ • RLS: None (see all regions)                        │
│ • CLS: dim_customer HIDE [ssn] (can see phone)     │
│ • Members: DataScience@company.com                   │
├─────────────────────────────────────────────────────┤
│ Role 4: Executive                                    │
│ • Folders: ALL ✅                                     │
│ • RLS: None                                          │
│ • CLS: None                                          │
│ • Members: CLevel@company.com                        │
└─────────────────────────────────────────────────────┘
```

---

## 10. CÂU HỎI MÔ PHỎNG BÀI THI

### Question 1

**A user with the Contributor role in a Fabric workspace can view all data in a Lakehouse, bypassing OneLake Security restrictions. A security administrator wants to prevent this user from seeing sensitive salary data in the fact_payroll table. What should the administrator do?**

A. Apply row-level security on the fact_payroll table  
B. Change the user's workspace role from Contributor to Viewer, then apply OneLake Security  
C. Apply dynamic data masking on salary columns  
D. Remove the user's workspace access entirely  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: B**

**Giải thích:**
- **Contributor** role **bypass** OneLake Security → bất kỳ security nào applied ở data level đều bị ignored
- Giải pháp đúng: **Downgrade** role xuống **Viewer** → OneLake Security được enforce → apply folder/table-level security để ẩn fact_payroll
- **(A)** RLS sẽ bị bypass vì user là Contributor
- **(C)** DDM cũng bị bypass bởi Contributor
- **(D)** Quá extreme, user cần truy cập workspace cho các items khác
- Key principle: OneLake Security chỉ effective cho Viewer role hoặc item-level shared users

</details>

---

### Question 2

**You need to mask the Email column in a Fabric Warehouse table so that non-privileged users see a partially masked version. Which T-SQL statement should you use?**

A. `ALTER TABLE dbo.Customers ALTER COLUMN Email ADD MASKED WITH (FUNCTION = 'default()');`  
B. `ALTER TABLE dbo.Customers ALTER COLUMN Email ADD MASKED WITH (FUNCTION = 'email()');`  
C. `ALTER TABLE dbo.Customers ALTER COLUMN Email ADD ENCRYPTED WITH (FUNCTION = 'email()');`  
D. `ALTER TABLE dbo.Customers ALTER COLUMN Email ADD MASKED WITH (FUNCTION = 'partial(1,"XXX",1)');`  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: B**

**Giải thích:**
- `email()` masking function được thiết kế đặc biệt cho email → hiển thị dạng `uXXX@XXXX.com`
- **(A)** `default()` che hoàn toàn → "XXXX", không phải "partially masked"
- **(C)** `ENCRYPTED` không phải DDM syntax, đây là Always Encrypted syntax
- **(D)** `partial(1,"XXX",1)` hoạt động nhưng không optimal cho email format
- `email()` là purpose-built function cho email masking và cho output dễ nhận biết là email

</details>

---

### Question 3

**Your organization requires that all items containing financial data must be labeled as "Confidential" and that this label should automatically propagate to downstream reports. Which feature enables this?**

A. OneLake Security with folder-level restrictions  
B. Sensitivity labels with downstream inheritance  
C. Dynamic data masking with UNMASK permissions  
D. Endorsement with Certified status  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: B**

**Giải thích:**
- **Sensitivity labels** với **downstream inheritance** tự động propagate labels từ data source (Lakehouse/Semantic Model) xuống downstream items (Reports/Dashboards)
- **(A)** OneLake Security kiểm soát data access, không phải data classification/labeling
- **(C)** DDM che giấu dữ liệu, không phân loại/label
- **(D)** Endorsement xác nhận quality/trust, không phải security classification
- Downstream inheritance = "label flows with data" → khi Lakehouse được label "Confidential", reports build on top cũng tự động nhận label

</details>

---

### Question 4

**A Fabric administrator needs to determine which users have exported reports containing sensitive data over the past 30 days. Where should the administrator look?**

A. Fabric Monitoring Hub  
B. Microsoft Purview Compliance Portal - Audit logs  
C. OneLake Security activity feed  
D. Workspace settings - Activity log  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: B**

**Giải thích:**
- **Microsoft Purview Compliance Portal Audit logs** là nơi chính thức để tra cứu compliance-related activities như export operations
- **(A)** Monitoring Hub hiển thị operational activities (real-time), không phù hợp cho historical compliance audit 30 ngày
- **(C)** OneLake Security không có activity feed riêng
- **(D)** Workspace settings không có detailed activity log cho export tracking
- Sử dụng operation name "ExportReport" trong audit log search với date range 30 ngày

</details>

---

### Question 5

**You want to allow a group of analysts to build their own Power BI reports using a certified semantic model, without giving them access to the workspace containing the model. Which permission should you grant?**

A. Workspace Viewer role  
B. Read permission on the semantic model  
C. Build permission on the semantic model  
D. ReadAll permission on the semantic model  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: C**

**Giải thích:**
- **Build permission** cho phép user tạo reports dựa trên semantic model **mà không cần workspace access**
- **(A)** Viewer role cấp quyền xem TẤT CẢ items trong workspace, quá rộng
- **(B)** Read chỉ cho phép xem model metadata, không thể build reports
- **(D)** ReadAll cho phép query underlying data nhưng không specifically cho phép build reports
- Build permission là key cho **self-service BI**: analysts có thể tạo reports riêng từ trusted semantic models

</details>

---

### Question 6

**Which of the following combinations correctly describes the behavior of OneLake Security enforcement?**

A. OneLake Security enforces rules for Admin, Member, and Contributor roles  
B. OneLake Security enforces rules only for Viewer role and item-level shared users  
C. OneLake Security enforces rules for all workspace roles equally  
D. OneLake Security only works with SQL analytics endpoint, not Spark  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: B**

**Giải thích:**
- OneLake Security chỉ enforce cho **Viewer role** và users được grant **item-level permissions** (không có workspace role)
- **(A)** Sai - Admin, Member, Contributor **bypass** OneLake Security
- **(C)** Sai - Không enforce equally, elevated roles bypass
- **(D)** Sai - OneLake Security enforce xuyên qua TẤT CẢ engines: Spark, SQL endpoint, Direct Lake, ADLS Gen2 APIs
- Đây là concept cực kỳ quan trọng: Design security model với assumption rằng elevated workspace roles bypass data-level security

</details>

---

### Question 7

**A company wants to mark their "Monthly Revenue Report" as having been officially reviewed and approved by the data governance team. What should they do?**

A. Apply a "Confidential" sensitivity label  
B. Promote the report  
C. Certify the report  
D. Enable audit logging for the report  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: C**

**Giải thích:**
- **Certification** = official review và approval bởi authorized individuals (set by Fabric Admin)
- **(A)** Sensitivity labels phân loại dữ liệu, không phải endorsement
- **(B)** Promotion chỉ cần Member/Contributor role, chưa qua official review process
- **(D)** Audit logging theo dõi activities, không phải endorsement
- Certified ≠ Promoted: Certified = "governance team đã review", Promoted = "creator cho rằng ready to use"

</details>

---

### Question 8

**You configure OneLake Security with a role that grants access only to the "fact_sales" and "dim_customer" tables. A user with this role queries the SQL analytics endpoint. What happens when they run `SELECT * FROM fact_hr_payroll`?**

A. The query returns all data from fact_hr_payroll  
B. The query returns an empty result set  
C. The query returns an error indicating insufficient permissions  
D. The query returns data with all columns masked  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: C**

**Giải thích:**
- Khi user KHÔNG được grant access tới table `fact_hr_payroll` qua OneLake Security role → query sẽ **fail với permission error**
- **(A)** Sai - OneLake Security sẽ block access
- **(B)** Empty result set xảy ra với RLS (filter rows), không phải folder/table-level restriction
- **(D)** Masking là DDM behavior, không phải OneLake folder security
- Folder/Table-level security = binary access: có quyền hoặc không có quyền, không phải filtered view

</details>

---

### Question 9

**You need to implement column-level security in a Fabric Warehouse using T-SQL. You want to allow the "Analyst" user to see only EmployeeID, Name, and Department columns. Which T-SQL approach is correct?**

A. `DENY SELECT ON dbo.Employee (Salary, SSN) TO [Analyst];`  
B. `GRANT SELECT ON dbo.Employee (EmployeeID, Name, Department) TO [Analyst];`  
C. `ALTER TABLE dbo.Employee ALTER COLUMN Salary ADD MASKED WITH (FUNCTION = 'default()');`  
D. Both A and B are correct approaches  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: D**

**Giải thích:**
- **Cả hai cách đều valid:**
  - **(A)** DENY on specific columns = block those columns, allow rest
  - **(B)** GRANT on specific columns = explicitly allow only those columns
- **(C)** DDM masks data display, user vẫn "thấy" cột (chỉ bị che), khác với CLS (cột hoàn toàn hidden)
- Best practice phụ thuộc context:
  - GRANT approach: Khi muốn explicit "allow list" (whitelist)
  - DENY approach: Khi muốn block chỉ vài columns (blacklist)
- Cả hai đều là valid CLS implementation trong T-SQL

</details>

---

### Question 10

**Your company recently enabled OneLake Security (GA) for their Fabric tenant. The data engineering team uses Spark notebooks to process data in a Lakehouse. The team lead notices that the Spark notebooks can still access all tables regardless of the OneLake Security roles configured. What is the MOST likely reason?**

A. OneLake Security does not support Spark notebooks  
B. The data engineering team members have Contributor or higher workspace roles  
C. Spark notebooks require a separate security configuration  
D. OneLake Security only works with SQL analytics endpoint  

<details>
<summary>✅ Đáp án và giải thích</summary>

**Đáp án: B**

**Giải thích:**
- **Contributor, Member, và Admin** roles **bypass OneLake Security** → data engineering team có Contributor hoặc higher → tất cả security restrictions bị ignored
- **(A)** Sai - OneLake Security hỗ trợ Spark notebooks
- **(C)** Sai - OneLake Security là centralized, không cần config riêng per engine
- **(D)** Sai - OneLake Security enforce xuyên qua tất cả engines bao gồm Spark
- **Giải pháp:** Nếu muốn enforce security cho data engineers: tạo separate "consumption workspace" với Viewer role cho testing, hoặc tách workspace roles

</details>

---

## 📝 TÓM TẮT KIẾN THỨC CẦN NHỚ

| Chủ đề | Điểm then chốt |
|--------|----------------|
| **Workspace Roles** | Admin > Member > Contributor > Viewer; Admin/Member/Contributor bypass OneLake Security |
| **Item-Level** | Share individual items: Read, ReadAll, Write, Build, Reshare |
| **ReadAll** | Cho phép SQL endpoint query; cần cho Lakehouse/Warehouse |
| **Build** | Cho phép tạo reports từ semantic model (self-service BI) |
| **RLS** | Filter rows: DAX (semantic model), T-SQL (SQL), OneLake roles |
| **CLS** | Hide columns: GRANT/DENY SELECT on columns (T-SQL), OneLake roles |
| **OLS** | Hide entire tables/objects: Tabular Editor, T-SQL |
| **DDM** | Mask display: default(), email(), random(), partial(); UNMASK permission |
| **Sensitivity Labels** | Microsoft Purview; downstream inheritance; export controls |
| **Endorsement** | Promoted (Member+) vs Certified (authorized users only) |
| **Audit Logs** | Purview Compliance Portal, PowerShell, REST API |
| **OneLake Security** | centralized; enforced by supported Fabric engines/authorized engines; elevated workspace roles bypass; bypass by elevated roles |

---

> 📖 **Tài liệu tham khảo chính thức:**
> - [Security in Microsoft Fabric](https://learn.microsoft.com/en-us/fabric/security/)
> - [OneLake Security](https://learn.microsoft.com/en-us/fabric/onelake/security/onelake-security-overview)
> - [Dynamic Data Masking](https://learn.microsoft.com/en-us/fabric/data-warehouse/dynamic-data-masking)
> - [Sensitivity Labels in Fabric](https://learn.microsoft.com/en-us/fabric/governance/sensitivity-labels)
> - [Audit Logs](https://learn.microsoft.com/en-us/fabric/admin/track-user-activities)


---

## 11. BỔ SUNG ÔN THI 2026: SECURITY PRECEDENCE

### 11.1 Thứ tự suy luận khi làm câu hỏi

1. User có workspace Admin/Member/Contributor không? Nếu có, họ có elevated data access và OneLake RLS/CLS không hạn chế họ.
2. Nếu là Viewer hoặc chỉ item Read, kiểm tra item permissions và membership trong OneLake security role.
3. Nếu truy cập qua SQL analytics endpoint, phân biệt SQL permissions với OneLake security và khả năng fallback của Direct Lake.
4. Nếu engine bên ngoài truy cập table có RLS/CLS, kiểm tra engine có được authorized không; nếu không, access có thể bị block.

### 11.2 Ví dụ T-SQL DDM

```sql
-- Ví dụ trong Fabric Warehouse/SQL experience hỗ trợ DDM
CREATE TABLE dbo.Customer (
    CustomerId bigint NOT NULL,
    Email varchar(256) MASKED WITH (FUNCTION = 'email()') NULL,
    CreditCard varchar(32) MASKED WITH (FUNCTION = 'partial(0,"XXXX-XXXX-XXXX-",4)') NULL
);
```

### 11.3 Câu hỏi tình huống bổ sung

**Một user thuộc Contributor role và đồng thời là thành viên OneLake role chỉ cho phép đọc bảng `Sales`. User vẫn đọc được bảng `Payroll`. Nguyên nhân hợp lý nhất?**

A. OneLake role bị cache 24 giờ  
B. Contributor là elevated workspace role và bypass OneLake RLS/CLS/OLS  
C. Sensitivity label tự cấp quyền  
D. Endorsement Certified mở khóa dữ liệu

<details><summary>Đáp án</summary>

**B.** Muốn kiểm thử granular security, dùng Viewer hoặc chỉ item-level read permissions.
</details>


---

## NGUỒN CHÍNH THỨC ĐÃ ĐỐI CHIẾU

- [DP-700 study guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)
- [OneLake security roles](https://learn.microsoft.com/en-us/fabric/onelake/security/create-manage-roles)
- [OneLake RLS](https://learn.microsoft.com/en-us/fabric/onelake/security/row-level-security)
- [OneLake SQL endpoint behavior](https://learn.microsoft.com/en-us/fabric/onelake/security/sql-analytics-endpoint-onelake-security)
- [Fabric permission model](https://learn.microsoft.com/en-us/fabric/security/permission-model)

---

## 12. CÂU HỎI SCENARIO — SECURITY & GOVERNANCE

> Các câu hỏi dưới đây là câu tự soạn theo blueprint DP-700, **không phải câu hỏi thật hoặc exam dump**.

### Câu D1-S1 — Least Privilege Cho Item Access

**Người dùng chỉ cần xem một Lakehouse duy nhất, không cần truy cập bất kỳ item nào khác trong workspace. Quyền **tối thiểu** nào phù hợp nhất?**

A. Contributor trên toàn workspace  
B. Admin trên toàn workspace  
C. Share item (Read) trực tiếp trên Lakehouse + gán OneLake role nếu cần giới hạn thêm  
D. Domain Admin  

<details><summary>Đáp án và giải thích</summary>

**C.** [Item sharing](https://learn.microsoft.com/en-us/fabric/get-started/share-items) cho phép cấp quyền **Read trên một item cụ thể** mà không cần cấp quyền workspace. Nếu cần giới hạn thêm đến folder/file cụ thể trong Lakehouse, có thể kết hợp với [OneLake security roles](https://learn.microsoft.com/en-us/fabric/onelake/security/create-manage-roles). Cấp Contributor toàn workspace sẽ vi phạm nguyên tắc **least privilege** và bypass toàn bộ OneLake RLS/CLS.

</details>

---

### Câu D1-S2 — Bypass OneLake RLS

**Một Contributor được thêm vào OneLake role chỉ cho phép đọc data có `Region = 'APAC'`, nhưng vẫn nhìn thấy toàn bộ dữ liệu bao gồm `EMEA` và `Americas`. Nguyên nhân hợp lý nhất là gì?**

A. OneLake RLS có độ trễ 24 giờ  
B. Contributor là elevated workspace role và **bypass OneLake RLS/CLS**  
C. Sensitivity label bị cấu hình sai  
D. KQL không hỗ trợ mệnh đề `where`  

<details><summary>Đáp án và giải thích</summary>

**B.** [Elevated workspace roles](https://learn.microsoft.com/en-us/fabric/onelake/security/onelake-security-overview#workspace-roles-and-onelake-security) (Admin, Member, Contributor) **bypass toàn bộ OneLake granular security** bao gồm RLS, CLS và OLS. Để granular security có hiệu lực, người dùng phải ở Viewer role hoặc chỉ có item-level access (Read), không có workspace role elevated.

</details>

---

### Câu D1-S3 — External Engine Và OneLake Security

**Một external engine (không được authorized bởi Fabric) cố đọc table trong Lakehouse có cấu hình OneLake RLS. Kết quả mong đợi là gì?**

A. Engine luôn đọc được raw Parquet files trực tiếp  
B. Access **có thể bị block** — engine không thể tự thực thi policies vì không được Fabric authorize  
C. Engine tự động downgrade thành Viewer role  
D. Sensitivity label bị xoá khi engine kết nối  

<details><summary>Đáp án và giải thích</summary>

**B.** [OneLake security](https://learn.microsoft.com/en-us/fabric/onelake/security/onelake-security-overview#supported-and-authorized-engines) chỉ được enforce bởi **supported và authorized engines**. External engine hoặc công cụ chưa được authorize không được giả định tự thực thi policies — Fabric có thể block access hoặc không enforce RLS đúng cách. Đây là lý do không thể dựa vào OneLake security cho engines ngoài hệ sinh thái Fabric.

</details>

---

## 13. LAB THỰC HÀNH — SECURITY VALIDATION

> Lab tự tạo để kiểm tra cách OneLake security và workspace role hoạt động thực tế.

### Lab 10 — Kiểm Tra Phân Quyền OneLake

**Mục tiêu:** Hiểu sự khác biệt giữa workspace role và OneLake role khi truy cập dữ liệu Lakehouse.

**Thiết lập:**

```text
Bước 1: Tạo workspace "Security-Lab" với hai Lakehouse:
  - lakehouse_sales   (chứa bảng fact_orders)
  - lakehouse_payroll (chứa bảng payroll_data)

Bước 2: Tạo 2 tài khoản test:
  - User A: gán Contributor trên workspace
  - User B: gán Viewer + OneLake role chỉ phạm vi thư mục "Sales"
```

**Kiểm tra User A (Contributor):**

```python
# User A đăng nhập và chạy notebook
df_orders  = spark.read.table("lakehouse_sales.fact_orders")    # ✅ Có thể đọc
df_payroll = spark.read.table("lakehouse_payroll.payroll_data") # ✅ Có thể đọc — bypass!

# Kết luận: Contributor bypass OneLake security
```

**Kiểm tra User B (Viewer + OneLake role = Sales only):**

```python
# User B đăng nhập và cố đọc Payroll
df_orders  = spark.read.table("lakehouse_sales.fact_orders")    # ✅ Được phép
df_payroll = spark.read.table("lakehouse_payroll.payroll_data") # ❌ Bị block / lỗi permission
```

**Điểm cần ghi chép sau lab:**

| Phương thức truy cập | User A (Contributor) | User B (Viewer + Role) |
|---------------------|---------------------|----------------------|
| Spark notebook | Tất cả data | Chỉ Sales data |
| SQL analytics endpoint | Tất cả data | Chỉ Sales data |
| OneLake shortcut | Tất cả data | Chỉ Sales data |

> [!NOTE]
> Enforcement và limitations có thể khác nhau giữa Spark, SQL endpoint, và shortcut path. Luôn kiểm tra theo từng access path riêng biệt. Tham khảo: [OneLake security per engine](https://learn.microsoft.com/en-us/fabric/onelake/security/onelake-security-overview)

