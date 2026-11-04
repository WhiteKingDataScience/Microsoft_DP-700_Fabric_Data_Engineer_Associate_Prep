# DP-700: Configure Security and Governance
## Implement and manage an analytics solution (30–35%)
### Đối chiếu Study Guide DP-700 (trang cập nhật 30/10/2026)

> [!IMPORTANT]
> **Bản cập nhật ngày 05/11/2026.** Đối chiếu với [Study Guide DP-700](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700) (Microsoft cập nhật trang ngày **30/10/2026**) và tài liệu Microsoft Learn tháng 10–11/2026. Bản trước (09/08/2026) dựa trên blueprint 21/07/2026; những gì đã đổi nằm ở chương **⚡ CẬP NHẬT 05/11/2026** cuối file và ở đầu file [MASTER](DP700_00_MASTER_Trong_Tam_On_Thi.md). Microsoft cho biết phần lớn câu hỏi tập trung vào tính năng GA, nhưng tính năng Preview phổ biến vẫn có thể xuất hiện. Với các bảng “supported items”, runtime, limits và trạng thái Preview/GA, hãy kiểm tra Microsoft Learn lần cuối trước ngày thi.


> [!CAUTION]
> **Không có tài liệu nào có thể bảo đảm “trúng mọi câu hỏi”.** Microsoft nêu rõ danh sách skills là minh họa và đề có thể hỏi các chủ đề liên quan. Mục tiêu của bản này là: phủ đủ blueprint hiện hành, sửa các facts đã thay đổi, và luyện cách chọn đáp án theo **scenario + constraint + least privilege/least operations**.

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
├─────────────────────────────────────────────────────┤
│  Layer 5: INFORMATION PROTECTION                     │
│  • Sensitivity labels (Microsoft Purview)            │
│  • Endorsement (Promoted, Certified, Master data)    │
└─────────────────────────────────────────────────────┘
```

### 1.2 Nguyên tắc cơ bản

- **Principle of Least Privilege:** Cấp quyền tối thiểu cần thiết
- **Defense in Depth:** Nhiều lớp bảo mật chồng lên nhau
- **Centralized Governance:** OneLake Security hướng tới "define once" và enforcement qua Fabric/supported authorized engines; engine/access mode vẫn phải được kiểm tra

---

## 2. IMPLEMENT WORKSPACE-LEVEL ACCESS CONTROLS

### 2.1 Workspace Roles chi tiết

| Quyền | Admin | Member | Contributor | Viewer |
|--------|:-----:|:------:|:-----------:|:------:|
| Xem items | ✅ | ✅ | ✅ | ✅ |
| Đọc Warehouse/Lakehouse qua SQL endpoint (`ReadData`) | ✅ | ✅ | ✅ | ✅ |
| Đọc qua OneLake APIs/Spark/Lakehouse explorer (`ReadAll`) | ✅ | ✅ | ✅ | ❌ |
| Tạo items | ✅ | ✅ | ✅ | ❌ |
| Chỉnh sửa items | ✅ | ✅ | ✅ | ❌ |
| Xóa items | ✅ | ✅ | ✅ | ❌ |
| Share items | ✅ | ✅ | Chỉ khi có **Reshare** riêng | Chỉ khi có **Reshare** riêng |
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

> **Nuance dễ ra bẫy:** Contributor/Viewer không có quyền Reshare chỉ nhờ workspace role, nhưng vẫn có thể share một item nếu được cấp **Reshare** ở item đó. Vì vậy hãy tách hai câu hỏi: “workspace role mặc định cho phép gì?” và “user còn có item permission nào?”. Xem [Roles in workspaces](https://learn.microsoft.com/en-us/fabric/fundamentals/roles-workspaces).

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

### 4.1 Trước tiên: cùng tên nhưng có thể tồn tại ở nhiều security layer

RLS/CLS/OLS có thể xuất hiện trong **OneLake security**, **SQL security** hoặc **semantic model**. Đề thi thường không chỉ hỏi “RLS là gì?” mà hỏi **đặt policy ở đâu để engine X thực thi đúng**.

### 4.2 Row-Level Security (RLS)

**Mục tiêu:** user chỉ thấy một tập **rows**.

- **OneLake RLS (Preview):** tạo rule trong OneLake security role cho table rồi gán users/groups. Fabric/authorized engines lọc rows khi query.
- **SQL RLS:** dùng security predicate/policy trong SQL layer khi scenario dùng SQL security (đặc biệt Delegated identity mode).
- **Semantic-model RLS:** dùng model roles/DAX khi yêu cầu bảo mật ở semantic-model/report layer.

> [!IMPORTANT]
> Không dùng pseudo-syntax kiểu `@user_attribute.region` nếu đề/tài liệu không định nghĩa cơ chế mapping đó. Với DP-700, điều quan trọng là chọn **đúng layer và engine**, không phải ghi nhớ một cú pháp giả định.

**OneLake RLS rule mẫu** nhập trong `Manage OneLake security > Role > Table > Row security`:

```sql
SELECT *
FROM dbo.FactSales
WHERE Region IN ('APAC', 'ANZ')
  AND Amount > 0
```

Rule phải tham chiếu đúng schema/table/column của Delta table và dùng subset syntax được hỗ trợ. OneLake RLS hiện phù hợp với static predicate theo role; không tự suy diễn thuộc tính người dùng nếu chưa có cơ chế mapping được tài liệu hóa.

#### 4.2.1 SQL RLS end-to-end trong Warehouse/SQL analytics endpoint

Ví dụ dưới đây lọc `dbo.FactSales` theo email của nhân viên bán hàng. Người dùng thường chỉ thấy row có `SalesRep = USER_NAME()`; manager được xem tất cả.

```sql
-- Bước 1: schema riêng để chứa security objects
CREATE SCHEMA Security;
GO

-- Bước 2: inline table-valued function trả về 1 khi row được phép hiển thị
CREATE FUNCTION Security.tvf_SalesPredicate
(
    @SalesRep varchar(100)
)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
(
    SELECT 1 AS IsAllowed
    WHERE @SalesRep = USER_NAME()
       OR USER_NAME() = 'manager@contoso.com'
);
GO

-- Bước 3: gắn filter predicate vào cột SalesRep của target table
CREATE SECURITY POLICY Security.SalesFilter
ADD FILTER PREDICATE Security.tvf_SalesPredicate(SalesRep)
ON dbo.FactSales
WITH (STATE = ON);
GO

-- Bước 4: cấp quyền query; GRANT/DENY tự tạo database principal trong Fabric
-- nhưng user vẫn cần Fabric item Read để CONNECT tới Warehouse/SQL endpoint.
GRANT SELECT ON dbo.FactSales TO [sales1@contoso.com];
GRANT SELECT ON dbo.FactSales TO [manager@contoso.com];
GO

-- Bước 5: đăng nhập bằng từng account và kiểm thử identity + kết quả
SELECT USER_NAME() AS CurrentDatabaseUser;
SELECT SaleId, SalesRep, Amount
FROM dbo.FactSales;
GO
```

Tạm tắt, bật lại hoặc tháo policy để bảo trì:

```sql
ALTER SECURITY POLICY Security.SalesFilter WITH (STATE = OFF);
ALTER SECURITY POLICY Security.SalesFilter WITH (STATE = ON);

-- Muốn sửa/drop predicate function phải tháo policy ràng buộc trước.
DROP SECURITY POLICY Security.SalesFilter;
DROP FUNCTION Security.tvf_SalesPredicate;
```

> **Bẫy rất quan trọng:** SQL RLS security policy áp dụng cả với database owner/elevated SQL user nếu predicate không viết exception. Ngược lại, **OneLake RLS** bị bypass bởi workspace Admin/Member/Contributor. Cùng tên “RLS” nhưng engine/layer khác nhau dẫn tới behavior khác nhau. Xem [SQL RLS](https://learn.microsoft.com/en-us/fabric/data-warehouse/row-level-security) và [OneLake RLS](https://learn.microsoft.com/en-us/fabric/onelake/security/row-level-security).

### 4.3 Column-Level Security (CLS)

**Mục tiêu:** user thấy table nhưng không thấy một số **columns** nhạy cảm.

- OneLake CLS định nghĩa allowed columns trong OneLake role.
- SQL CLS dùng column-level permissions, ví dụ chỉ `GRANT SELECT` trên cột cho phép.
- OneLake RLS + CLS trên cùng table phải được thiết kế **trong cùng một role**; combination tách chúng thành nhiều role không được hỗ trợ có thể gây lỗi query.

#### 4.3.1 SQL CLS end-to-end

Thiết kế least privilege dễ kiểm soát nhất là: user **không có workspace role/ReadData rộng**, được share Warehouse bằng **item Read** để có thể connect, rồi chỉ được `GRANT SELECT` trên các cột cần dùng.

```sql
-- User/group được đọc các cột nghiệp vụ, không được đọc CreditCard.
GRANT SELECT ON dbo.Customers
(
    CustomerId,
    FirstName,
    LastName,
    Phone,
    Email
)
TO [SalesReaders@contoso.com];
GO

-- Thành công
SELECT CustomerId, FirstName, LastName, Phone, Email
FROM dbo.Customers;

-- Thất bại vì SELECT * bao gồm CreditCard không được cấp quyền
SELECT *
FROM dbo.Customers;
GO

-- Thu hồi column grant nếu không còn nhu cầu
REVOKE SELECT ON dbo.Customers
(
    CustomerId,
    FirstName,
    LastName,
    Phone,
    Email
)
FROM [SalesReaders@contoso.com];
GO
```

Kiểm tra effective SQL permissions của account đang đăng nhập:

```sql
SELECT *
FROM sys.fn_my_permissions('dbo.Customers', 'Object');
```

> Nếu user đã nhận broad `ReadData`/`SELECT` qua role hoặc item permission khác, một column-level `GRANT` hẹp không tự thu hồi quyền rộng đó. Luôn kiểm tra **mọi đường cấp quyền** và tránh overlapping broad grants. Xem [SQL granular permissions](https://learn.microsoft.com/en-us/fabric/data-warehouse/sql-granular-permissions) và [Column-level security](https://learn.microsoft.com/en-us/fabric/data-warehouse/column-level-security).

### 4.4 Object-Level / Table-Folder Security

**Mục tiêu:** ẩn/chặn cả object thay vì lọc row/column.

- OneLake role có thể grant tới table/folder cụ thể.
- SQL dùng object permissions (`GRANT`/`DENY`) trong SQL-security scenario.
- Semantic model có OLS cho table/column metadata trong model.

### 4.5 Folder/File-Level Security trong OneLake

Dùng khi yêu cầu là “team A chỉ đọc `Files/curated/finance/`, không được đọc `Files/raw/pii/`”. Đừng cấp Contributor toàn workspace chỉ để giải quyết yêu cầu folder-level; granular OneLake role phù hợp hơn nếu access path/item được hỗ trợ.

### 4.6 Most permissive access wins

Nếu user thuộc nhiều OneLake roles, quyền rộng hơn có thể thắng quyền hạn chế. Một role full-access + một role RLS cùng table có thể làm RLS không hạn chế như mong muốn. Tránh overlapping roles không cần thiết.

### 4.7 Decision table

| Requirement | Công cụ/layer nghĩ tới trước |
|---|---|
| Chỉ rows APAC | RLS |
| Ẩn Salary/SSN columns | CLS |
| Không được thấy table Payroll | OLS/object permission |
| Chỉ được đọc một folder OneLake | OneLake table/folder role |
| Mask email nhưng vẫn query được | DDM (SQL layer) |
| Power BI model lọc theo user | Semantic-model RLS |

---

## 5. IMPLEMENT DYNAMIC DATA MASKING

### 5.1 DDM là gì?

**Dynamic Data Masking (DDM)** che giá trị trong **query result** cho người không có `UNMASK`; dữ liệu gốc không bị thay đổi. Trong Fabric Data Warehouse, DDM áp dụng cho **Warehouse và SQL analytics endpoint**.

### 5.2 Bốn masking functions cần biết

| Function | Ý nghĩa |
|---|---|
| `default()` | Mask mặc định theo data type |
| `email()` | Mask dạng email |
| `random(start, end)` | Số ngẫu nhiên trong range |
| `partial(prefix,"padding",suffix)` | Giữ một phần đầu/cuối, mask phần giữa |

```sql
CREATE TABLE dbo.Customer (
    CustomerId bigint NOT NULL,
    Email varchar(256) MASKED WITH (FUNCTION = 'email()') NULL,
    CardNumber varchar(32) MASKED WITH (FUNCTION = 'partial(0,"XXXX-XXXX-XXXX-",4)') NULL
);

GRANT UNMASK ON dbo.Customer TO [Auditor@contoso.com];
REVOKE UNMASK ON dbo.Customer TO [Auditor@contoso.com];
```

### 5.3 DDM không phải là security boundary hoàn chỉnh

DDM chủ yếu giảm **accidental exposure**. Người có query permission vẫn có thể suy luận dữ liệu bằng predicate/aggregate trong một số tình huống. Với dữ liệu nhạy cảm, phải kết hợp **least privilege**, object/column/row controls và permissions phù hợp.

### 5.4 Bẫy với OneLake SQL analytics endpoint access mode

- **User identity mode:** table access/RLS/CLS/OLS đến từ OneLake security; **DDM không được OneLake security hỗ trợ**.
- **Delegated identity mode:** dùng SQL security; DDM, SQL RLS, CLS, `GRANT/REVOKE` hoạt động theo SQL layer.

> 💡 Nếu câu hỏi cần DDM qua SQL analytics endpoint, hãy kiểm tra access mode trước khi chọn đáp án.

## 6. APPLY SENSITIVITY LABELS TO ITEMS

### 6.1 Sensitivity label dùng để làm gì?

Sensitivity labels của **Microsoft Purview Information Protection** giúp phân loại độ nhạy (ví dụ Public / Internal / Confidential) và mang metadata bảo vệ đi cùng item/data. Không nên đồng nhất “gắn label” với “cấp/thu hồi data permission”.

### 6.2 Inheritance — kiến thức mới quan trọng

Fabric có thể reevaluate/inherit label khi:
- tạo item mới từ item đã có label;
- sửa item làm thay đổi data/content;
- thêm data source mới;
- tạo lineage relationship mới.

Nguyên tắc an toàn:
- automatic label **không ghi đè** label được apply thủ công;
- label ít restrictive hơn **không ghi đè** label restrictive hơn.

### 6.3 Protection policies — phân biệt với “protected label”

Đây là phần 2026 dễ nhầm:

- **Protected sensitivity label** truyền thống trong Fabric chủ yếu kiểm soát quyền **thay đổi/gỡ label**; bản thân nó không mặc định là item-access ACL.
- **Microsoft Purview protection policies for Fabric** có thể liên kết với sensitivity label để **chặn truy cập item** cho user/group không nằm trong policy.
- Protection policies hiện hỗ trợ native Fabric item types và semantic models, nhưng **không tích hợp quyền item này vào Git integration/deployment pipelines**; CI/CD vẫn dựa vào workspace permissions.

### 6.4 Domain-level default sensitivity label

Domain Admin có thể đặt default label cho domain nếu tenant setting tương ứng được bật. Feature này hiện có hạn chế: **Git integration và deployment pipelines không được hỗ trợ** cho domain-level default-label behavior.

### 6.5 Exam decision rule

- “Classify/protect information, inherit label” → Sensitivity label.
- “Only certain groups may keep access to items carrying label X” → Protection policy.
- “Limit rows/columns/tables/files” → OneLake/SQL/Semantic-model data security, không phải label đơn thuần.

## 7. ENDORSE ITEMS

### 7.1 Endorsement giúp người dùng tìm nội dung đáng tin cậy

Fabric hiện có **ba** endorsement badges:

| Badge | Ý nghĩa | Ai có thể áp dụng |
|---|---|---|
| **Promoted** | Chủ sở hữu/creator cho rằng item sẵn sàng để chia sẻ/tái sử dụng | User có Write permission |
| **Certified** | Đã được reviewer do tổ chức ủy quyền xác nhận đạt quality standards | Certifier do Fabric admin cho phép |
| **Master data** | Data item là nguồn dữ liệu cốt lõi/authoritative source of truth | Người được Fabric admin cho phép; chỉ áp dụng cho item chứa data |

Certification và Master data phải được Fabric admin enable. Certification settings có thể được delegate theo domain.

### 7.2 Đừng nhầm Endorsement với Security

Endorsement là **trust/discovery/governance signal**, không tự cấp data access. Một Lakehouse được Certified/Master data vẫn cần workspace/item/data permissions đúng.

### 7.3 Scenario phản xạ

- “Team owner tự đánh dấu nội dung đáng dùng” → **Promoted**.
- “Tổ chức cần reviewer chính thức xác nhận” → **Certified**.
- “Danh mục khách hàng chuẩn, authoritative source of truth” → **Master data**.

## 8. IMPLEMENT AND USE MICROSOFT FABRIC AUDIT LOGS

> **Cập nhật 05/11/2026:** audit logs không còn là bullet riêng trong study guide hiện hành — xếp vào **related topic**. Ôn ở mức nhận diện: câu hỏi "ai đã làm gì, khi nào" → audit log (Microsoft Purview).

### 8.1 Audit log trả lời câu hỏi nào?

Audit log dùng cho **who did what, on which item, when** — phục vụ compliance, investigation và records management.

### 8.2 Nơi truy cập đúng

Theo Microsoft hiện hành, Fabric user activities được tìm trong **Microsoft Purview portal**. Người truy cập cần role audit phù hợp (ví dụ Audit Logs role trong Exchange Online role model). Có thể truy vấn bằng Purview UI và PowerShell/Exchange Online tooling.

> [!IMPORTANT]
> **Monitoring Hub không phải Audit Log.** Monitoring Hub theo dõi operational runs/status (pipeline, notebook, Dataflow...), còn Purview audit log theo dõi user/admin activities để audit/compliance.

### 8.3 Đừng học retention bằng một con số cố định

Audit retention phụ thuộc Microsoft Purview Audit licensing và retention policies. Vì các mức retention/licensing có thể thay đổi, đề scenario nên được giải theo yêu cầu **compliance retention + Purview audit policy**, không mặc định “Fabric audit luôn 90 ngày”.

### 8.4 Ví dụ câu hỏi

**Yêu cầu:** Điều tra ai đã thay đổi quyền trên một Fabric item 30 ngày trước.

→ Tìm **Microsoft Purview audit log**, không dùng Monitoring Hub hoặc Capacity Metrics app.

## 9. CONFIGURE AND IMPLEMENT ONELAKE SECURITY

> **Cập nhật 05/11/2026:** OneLake security không còn bullet riêng, nhưng nó là **cơ chế thực hiện** bullet "row-level, column-level, object-level, and **file-level** access controls" trên Lakehouse — **vẫn là trọng tâm**.

### 9.1 Mental model: Control plane trước, data plane sau

Một user muốn đọc data thường cần hai lớp:
1. **Artifact/control-plane access**: workspace role hoặc item `Read`.
2. **Data-plane rules**: OneLake role hoặc SQL permissions tùy engine/access mode.

Nếu thiếu Artifact `Read`, SQL analytics endpoint có thể từ chối kết nối dù SQL/OneLake role nghe có vẻ đúng.

### 9.2 OneLake security roles

OneLake security có thể cấp quyền tới folder/table và thêm:
- **RLS**: lọc rows;
- **CLS**: lọc columns;
- **OLS**: giới hạn object/table/folder;
- **ReadWrite** granular permission cho một số supported items/scenarios.

RLS + CLS có thể kết hợp **trong cùng một OneLake role**. Việc tách RLS và CLS của cùng table vào nhiều role theo combination không hỗ trợ có thể gây query errors.

### 9.3 Multiple roles: most permissive access wins

Nếu user thuộc nhiều OneLake roles, access rộng hơn có thể thắng access hạn chế hơn. Ví dụ một role cho full table và role khác RLS cùng table → full-table grant có thể khiến RLS không hạn chế user như mong muốn. Vì vậy thiết kế roles phải tránh overlap không cần thiết.

### 9.4 Elevated workspace roles — baseline và ngoại lệ

Baseline: Admin/Member/Contributor có broad workspace data privileges và **thường bypass** OneLake granular security trên local tables; để test end-user filtering, dùng Viewer hoặc explicit item Read.

Nhưng không được biến thành quy tắc tuyệt đối:
- shortcut-backed data còn phụ thuộc quyền/policy ở **producer/source**;
- tài liệu troubleshooting hiện ghi nhận một số RLS behavior trong **user identity mode** có thể áp dụng rộng hơn theo scenario.

### 9.5 SQL analytics endpoint: User identity vs Delegated identity

| Security target | User identity mode | Delegated identity mode |
|---|---|---|
| Table access | OneLake security roles | SQL `GRANT/REVOKE` |
| RLS | OneLake RLS | SQL `CREATE SECURITY POLICY` |
| CLS | OneLake CLS | SQL column permissions |
| DDM | **Không hỗ trợ trong OneLake security** | SQL DDM |
| Identity tới OneLake | Signed-in user passthrough | Item/workspace owner delegated |

**User identity mode** phù hợp khi muốn centralized OneLake governance xuyên Spark/SQL/Power BI-supported paths. **Delegated identity mode** phù hợp khi cần full SQL security model.

### 9.6 Authorized-engine model

OneLake policy được định nghĩa tập trung, nhưng enforcement phải do engine được Fabric hỗ trợ/authorize thực hiện. Nếu engine không thể enforce RLS/CLS, OneLake có thể **block access** thay vì cho đọc raw files và bỏ qua policy.

### 9.7 Shortcut security

Đừng giả định role ở consumer workspace nâng quyền trên producer. Quyền ở source/producer vẫn quan trọng. Trong delegated SQL mode, shortcut-backed table có OneLake RLS/CLS/OLS ở source có thể bị block để tránh bypass chính sách.

### 9.8 Exam algorithm

1. User có Artifact `Read`/workspace access chưa?
2. Engine nào đang query: Spark, SQL analytics endpoint, Direct Lake, external engine?
3. Nếu SQL endpoint: **User identity hay Delegated identity?**
4. User có elevated workspace role không?
5. Có nhiều OneLake role overlap không?
6. Data local hay qua shortcut/producer khác?
7. Policy nào thực sự enforce: OneLake hay SQL?

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
- **(A)** Nếu đây là **OneLake RLS**, policy sẽ bị bypass vì user là Contributor. SQL RLS là security layer khác và có behavior khác, nhưng không giải quyết yêu cầu OneLake access-path trong scenario này.
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

| Chủ đề | Phản xạ đúng |
|---|---|
| Workspace / item access | Xác định control-plane permission trước |
| OneLake RLS/CLS/OLS | Fine-grained data access; chú ý overlap/multiple roles |
| SQL access modes | User identity = OneLake policies; Delegated identity = SQL security |
| DDM | 4 mask types; query-result masking; không thay data gốc; không phải security boundary hoàn chỉnh |
| Sensitivity label | Classification/inheritance/information protection metadata |
| Protection policy | Dùng label để giới hạn **item access** cho group/user được phép |
| Endorsement | Promoted, Certified, **Master data**; trust/discovery, không phải ACL |
| Audit | Microsoft Purview audit log; khác Monitoring Hub |
| Elevated roles | Admin/Member/Contributor thường bypass local OneLake granular security; kiểm tra exceptions/shortcuts |
| Authorized engines | Engine không enforce được RLS/CLS có thể bị OneLake block |

---

## 11. BỔ SUNG ÔN THI 2026: SECURITY PRECEDENCE

### 11.1 Thứ tự suy luận

1. **Artifact access** trước: user có Read/workspace access không?
2. Xác định **engine/access mode**.
3. Nếu OneLake mode: resolve role membership + most-permissive effect + RLS/CLS/OLS.
4. Nếu delegated SQL mode: resolve SQL grants/roles/RLS/CLS/DDM.
5. Nếu shortcut: kiểm tra producer/source security, không chỉ consumer workspace.
6. Nếu external engine: kiểm tra authorized-engine support.

### 11.2 OneLake ReadWrite — cập nhật đáng chú ý

OneLake security hiện có granular **ReadWrite** cho supported items/scenarios. Điều này cho phép Viewer hoặc user chỉ có item Read được cấp write tới folder/table cụ thể qua các supported write paths như Spark/OneLake APIs mà không cần nâng họ lên Contributor toàn workspace. Đây là ví dụ điển hình của **least privilege**.

### 11.3 Câu bẫy mẫu

**User cần chỉ sửa dữ liệu trong một folder cụ thể nhưng không được quyền chỉnh sửa mọi item trong workspace. Nên ưu tiên gì?**

→ Nếu item/path hỗ trợ, cân nhắc **OneLake granular ReadWrite** thay vì cấp Contributor toàn workspace.

---

## NGUỒN CHÍNH THỨC ĐÃ ĐỐI CHIẾU

- [DP-700 Study Guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)
- [Fabric permission model](https://learn.microsoft.com/en-us/fabric/security/permission-model)
- [OneLake security overview](https://learn.microsoft.com/en-us/fabric/onelake/security/onelake-security-overview)
- [OneLake SQL analytics endpoint security](https://learn.microsoft.com/en-us/fabric/onelake/security/sql-analytics-endpoint-onelake-security)
- [OneLake RLS](https://learn.microsoft.com/en-us/fabric/onelake/security/row-level-security)
- [OneLake CLS](https://learn.microsoft.com/en-us/fabric/onelake/security/column-level-security)
- [OneLake security best practices](https://learn.microsoft.com/en-us/fabric/onelake/security/best-practices-secure-data-in-onelake)
- [SQL row-level security](https://learn.microsoft.com/en-us/fabric/data-warehouse/row-level-security)
- [SQL column-level security](https://learn.microsoft.com/en-us/fabric/data-warehouse/column-level-security)
- [SQL granular permissions](https://learn.microsoft.com/en-us/fabric/data-warehouse/sql-granular-permissions)
- [Dynamic Data Masking](https://learn.microsoft.com/en-us/fabric/data-warehouse/dynamic-data-masking)
- [Information protection](https://learn.microsoft.com/en-us/fabric/governance/information-protection)
- [Protection policies](https://learn.microsoft.com/en-us/fabric/governance/protection-policies-overview)
- [Endorsement](https://learn.microsoft.com/en-us/fabric/governance/endorsement-overview)
- [Audit logs](https://learn.microsoft.com/en-us/fabric/admin/track-user-activities)

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
  - User B: không có elevated workspace role; cấp item Read trên Lakehouse,
            Read/Execute trên một notebook test riêng,
            và OneLake role chỉ phạm vi dữ liệu "Sales"
```

**Kiểm tra User A (Contributor):**

```python
# User A đăng nhập và chạy notebook
df_orders  = spark.read.table("lakehouse_sales.fact_orders")    # ✅ Có thể đọc
df_payroll = spark.read.table("lakehouse_payroll.payroll_data") # ✅ Có thể đọc — bypass!

# Kết luận: Contributor bypass OneLake security
```

**Kiểm tra User B (item-level access + OneLake role = Sales only):**

```python
# User B đăng nhập và cố đọc Payroll
df_orders  = spark.read.table("lakehouse_sales.fact_orders")    # ✅ Được phép
df_payroll = spark.read.table("lakehouse_payroll.payroll_data") # ❌ Bị block / lỗi permission
```

**Điểm cần ghi chép sau lab:**

| Phương thức truy cập | User A (Contributor) | User B (item access + OneLake role) |
|---------------------|---------------------|----------------------|
| Spark notebook | Tất cả data | Chỉ Sales data |
| SQL analytics endpoint | Tất cả data | Chỉ Sales data |
| OneLake shortcut | Tất cả data | Chỉ Sales data |

> [!NOTE]
> Workspace Viewer role không tự cho phép chạy notebook. Nếu muốn test bằng Spark, phải cấp `Execute` trên notebook test hoặc dùng một supported access path khác. Enforcement và limitations có thể khác nhau giữa Spark, SQL endpoint, và shortcut path; luôn kiểm tra theo từng path. Tham khảo: [Workspace roles](https://learn.microsoft.com/en-us/fabric/fundamentals/roles-workspaces) và [OneLake security per engine](https://learn.microsoft.com/en-us/fabric/onelake/security/onelake-security-overview).

---

# 🎯 CHƯƠNG BỔ SUNG 06/09/2026 — HỌC LẠI TỪ GỐC

> **Vì sao có chương này:** *Configure security and governance* là nhóm kỹ năng có **6 bullet trong study guide hiện hành** (bản 21/07/2026 có 8, gồm cả audit logs và OneLake security). Điều làm thí sinh sai không phải là không biết RLS là gì, mà là không lần được **đường đi của quyền**: cùng một người dùng, đi vào bằng Spark thì thấy hết dữ liệu, đi vào bằng SQL endpoint thì bị chặn. Chương này viết lại theo hướng **giải bài toán bảo mật bằng một khung 5 bước cố định**.
> Xem thêm: [File tổng chỉ huy](DP700_00_MASTER_Trong_Tam_On_Thi.md)

## A. HIỂU BẢN CHẤT TRONG 60 GIÂY

Bảo mật Fabric là **nhiều lớp cổng nối tiếp nhau**. Người dùng phải qua được **tất cả** các cổng trên đường đi của mình:

```text
   NGƯỜI DÙNG
       │
       ▼
 ┌─────────────────┐   Cổng 1: vào được workspace không, với vai trò gì?
 │ WORKSPACE ROLE  │   Admin / Member / Contributor / Viewer
 └────────┬────────┘
          ▼
 ┌─────────────────┐   Cổng 2: có quyền trên item cụ thể không?
 │ ITEM PERMISSION │   Read / ReadData / ReadAll / Write / Reshare / Execute
 └────────┬────────┘
          ▼
 ┌───────────────────────────────────────────────────────┐
 │ CỔNG 3: ĐI VÀO BẰNG ĐƯỜNG NÀO? (access path)          │
 │                                                       │
 │  Spark / OneLake API ──► OneLake security (OLS/RLS/CLS)│
 │  SQL analytics endpoint ─► T-SQL RLS / CLS / DDM       │
 │  Semantic model ─────────► DAX RLS                     │
 └───────────────────────────────────────────────────────┘
```

**Câu quan trọng nhất của cả mục này:**

> **Chính sách bảo mật gắn với ĐƯỜNG ĐI, không gắn với con người.** Cùng một user, đi đường khác nhau thì chính sách áp khác nhau. Đây là nguồn gốc của gần như mọi câu hỏi "vì sao user vẫn thấy dữ liệu?".

## B. KHUNG 5 BƯỚC — DÙNG CHO MỌI CÂU HỎI BẢO MẬT

```text
1. IDENTITY nào?        user cá nhân · security group · service principal · workspace identity
2. WORKSPACE ROLE gì?   Admin / Member / Contributor  →  BYPASS OneLake RLS/CLS
                        Viewer                        →  BỊ RÀNG BUỘC
3. ITEM PERMISSION gì?  Read (kết nối) · ReadData (qua SQL endpoint) · ReadAll (qua OneLake/Spark)
                        Write · Reshare · Execute (chạy notebook)
4. ACCESS PATH nào?     Spark | SQL analytics endpoint | OneLake API/shortcut | Semantic model
5. DATA POLICY nào áp?  OneLake security (OLS/RLS/CLS) | T-SQL RLS/CLS/DDM | DAX RLS
```

**Áp dụng thử — câu hỏi kinh điển:**

> *Bạn tạo RLS trên bảng trong SQL analytics endpoint để user chỉ thấy vùng của mình. Nhưng user vẫn thấy toàn bộ dữ liệu. Vì sao?*

- Bước 2: user đang là **Contributor** → **bypass** chính sách granular. **Đây là nguyên nhân số 1.**
- Bước 4: hoặc user đang đọc bằng **Spark/OneLake**, trong khi RLS bạn tạo chỉ áp cho **SQL endpoint**.
- Cách sửa: hạ về **Viewer** (hoặc cấp quyền item ở mức đọc), và/hoặc áp **OneLake security** để chặn cả đường Spark.

## C. WORKSPACE ROLE — BẢNG PHẢI VIẾT LẠI ĐƯỢC TỪ TRÍ NHỚ

| Hành động | Admin | Member | Contributor | Viewer |
|---|:---:|:---:|:---:|:---:|
| Xoá workspace, đổi workspace settings | ✅ | ❌ | ❌ | ❌ |
| Thêm/xoá người | ✅ mọi role | ⚠️ tới mức Member | ❌ | ❌ |
| Tạo/sửa/xoá item | ✅ | ✅ | ✅ | ❌ |
| Chạy notebook / pipeline | ✅ | ✅ | ✅ | ❌ |
| Reshare item | ✅ | ✅ | ⚠️ chỉ khi được cấp Reshare ở item | ❌ |
| Đọc qua SQL analytics endpoint | ✅ | ✅ | ✅ | ✅ |
| **Bị RLS/CLS/DDM ràng buộc** | ❌ | ❌ | ❌ | ✅ |

> 📌 Ba hệ quả phải thuộc: (1) muốn áp RLS → dùng **Viewer**; (2) **test** RLS phải bằng tài khoản Viewer, test bằng Contributor sẽ luôn thấy hết và bạn tưởng RLS hỏng; (3) Viewer **không chạy được notebook** — cần quyền Execute riêng.

## D. ITEM PERMISSION — PHÂN BIỆT READDATA vs READALL

```text
Read      → kết nối tới item, thấy metadata
ReadData  → ĐỌC BẢNG qua SQL ANALYTICS ENDPOINT
ReadAll   → ĐỌC TRỰC TIẾP file/bảng qua ONELAKE API / SPARK / Lakehouse explorer
Write     → sửa item
Reshare   → chia sẻ lại cho người khác
Execute   → chạy item (ví dụ notebook)
```

> 📌 Cấp **ReadData** không tự động cho phép đọc file bằng Spark. Đề hỏi "user cần đọc bằng notebook Spark" mà đáp án chỉ cấp ReadData → sai.

## E. ONELAKE SECURITY — CÁCH KẾT HỢP QUYỀN

```text
TRONG MỘT ROLE:  quyền = GIAO của ba lớp
                 OLS (bảng/folder nào)  ∩  RLS (dòng nào)  ∩  CLS (cột nào)

NHIỀU ROLE:      kết quả = MOST PERMISSIVE (hợp của các role)
                 → user thuộc 2 role thì thấy phần rộng hơn, không phải phần hẹp hơn
```

Điều này rất hay bị nhớ ngược. Cách nhớ: **trong một role thì thắt chặt (giao); giữa các role thì nới rộng (hợp)**.

**SQL analytics endpoint có hai chế độ bảo mật — phải phân biệt:**

| Chế độ | Chính sách nào áp | Ghi chú |
|---|---|---|
| **User identity mode** | Áp **OneLake security roles** của chính người dùng | Role OneLake được đồng bộ sang endpoint |
| **Delegated / owner identity mode** | Áp quyền **T-SQL**: `GRANT`/`REVOKE`, RLS/CLS/DDM ở tầng SQL | Cách truyền thống |

Chuyển đổi giữa hai chế độ khiến endpoint **tạm thời không khả dụng cho cả workspace** → phải làm trong maintenance window.

## F. RLS / CLS / DDM — MỖI CÁI GIẢI QUYẾT GÌ

| Kỹ thuật | Giới hạn theo | Ví dụ | Ghi chú thi |
|---|---|---|---|
| **RLS** (row-level) | **Dòng** | Sales rep chỉ thấy vùng của mình | Có ở tầng T-SQL (security policy + filter predicate), OneLake security, và semantic model (DAX) |
| **CLS** (column-level) | **Cột** | Ẩn hẳn cột `Salary` | Người không có quyền sẽ **lỗi** khi `SELECT` cột đó |
| **DDM** (dynamic data masking) | **Hiển thị** của cột | `1234-5678-****` | Cột **vẫn truy vấn được** — không phải ranh giới bảo mật đầy đủ |
| **OLS** (object/table-level) | **Bảng/folder** | Không thấy bảng `HR_Payroll` | Trong OneLake security là phạm vi bảng/folder |

> ⚠️ **DDM không phải cơ chế bảo mật hoàn chỉnh.** User có quyền query vẫn có thể suy ra giá trị bằng cách lọc/so sánh (`WHERE Salary > 1000`). Khi đề yêu cầu "người dùng tuyệt đối không được biết giá trị", đáp án phải là **CLS hoặc phân quyền thật**, không phải DDM.

**RLS ở Warehouse vs RLS ở semantic model — chọn cái nào:**

| Tình huống | Chọn |
|---|---|
| Bảo vệ dữ liệu ở tầng kho, mọi công cụ SQL đều bị áp | **RLS trong Warehouse (T-SQL)** |
| Model **Direct Lake** cần giữ chế độ Direct Lake | **RLS trong semantic model (DAX)** — vì RLS ở SQL endpoint gây **fallback DirectQuery** |
| Bảo vệ cả đường Spark/OneLake | **OneLake security** |
| An toàn nhất | Kết hợp nhiều tầng theo đúng access path cần chặn |

## G. NHÃN, ENDORSEMENT, AUDIT — BA THỨ HAY BỊ TRỘN

| | Sensitivity label | Endorsement | Audit log |
|---|---|---|---|
| Mục đích | **Phân loại + bảo vệ** dữ liệu (Purview) | Chỉ dấu **độ tin cậy/chất lượng** | Ghi lại **ai làm gì, khi nào** |
| Giá trị | Public / Internal / Confidential… | **Promoted**, **Certified**, (Master data) | — |
| Có chặn truy cập không? | Có thể áp protection policy | **KHÔNG** — chỉ là nhãn tin cậy | Không, chỉ ghi nhận |
| Đặc điểm cần nhớ | **Kế thừa** xuống downstream item (ví dụ semantic model → report) | Certified do người được uỷ quyền duyệt | Truy cập qua Microsoft Purview / Microsoft 365 audit |

> 📌 Đề hỏi "làm sao để người dùng biết dataset nào đáng tin cậy" → **Endorsement**. Đề hỏi "làm sao để dữ liệu nhạy cảm được phân loại và bảo vệ khi tải xuống" → **Sensitivity label**. Đừng đảo hai đáp án này.

## H. BỘ QUYẾT ĐỊNH — ĐỌC ĐỀ CHỌN ĐƯỢC NGAY

| Đề nói… | Chọn |
|---|---|
| "read-only và chỉ thấy dữ liệu vùng mình" | **Viewer** + RLS (hoặc OneLake security) |
| "user phải đọc dữ liệu bằng notebook Spark" | Item permission **ReadAll** (+ Execute trên notebook nếu phải chạy) |
| "ẩn hoàn toàn cột lương với nhóm A" | **CLS** (không phải DDM) |
| "che số thẻ nhưng vẫn cho query bảng" | **DDM** |
| "chặn cả đường Spark lẫn SQL cho cùng một tập dòng" | **OneLake security** (RLS ở OneLake), không chỉ RLS T-SQL |
| "giữ Direct Lake mà vẫn có bảo mật dòng" | **RLS trong semantic model (DAX)** |
| "đánh dấu dataset chuẩn cho toàn công ty" | **Endorsement: Certified** |
| "dữ liệu nhạy cảm phải được phân loại và kế thừa nhãn" | **Sensitivity label** |
| "ai đã xoá workspace tuần trước?" | **Audit log** |
| "gom workspace theo phòng ban để uỷ quyền quản trị" | **Domain** (không phải cơ chế phân quyền dữ liệu) |

## I. ACTIVE RECALL — 12 CÂU (che đáp án, nói thành tiếng)

<details><summary><b>1.</b> Năm bước giải một câu hỏi bảo mật?</summary>

Identity → workspace role → item permission → access path → data policy.
</details>

<details><summary><b>2.</b> Role nào bypass OneLake RLS/CLS?</summary>

**Admin, Member, Contributor**. Chỉ **Viewer** (và quyền item ở mức đọc) bị ràng buộc.
</details>

<details><summary><b>3.</b> ReadData và ReadAll khác nhau ra sao?</summary>

ReadData = đọc qua **SQL analytics endpoint**. ReadAll = đọc trực tiếp file/bảng qua **OneLake API/Spark**.
</details>

<details><summary><b>4.</b> Trong một OneLake security role, OLS/RLS/CLS kết hợp thế nào? Giữa nhiều role thì sao?</summary>

Trong một role: **giao** (thắt chặt). Giữa nhiều role: **most permissive** — hợp các role (nới rộng).
</details>

<details><summary><b>5.</b> Hai chế độ bảo mật của SQL analytics endpoint?</summary>

**User identity mode** (áp OneLake security roles) và **delegated/owner identity mode** (áp quyền T-SQL: GRANT/REVOKE, RLS/CLS/DDM). Đổi chế độ làm endpoint tạm thời không khả dụng.
</details>

<details><summary><b>6.</b> DDM có phải ranh giới bảo mật đầy đủ không? Vì sao?</summary>

**Không.** Cột vẫn query được nên user có thể suy ra giá trị bằng cách lọc/so sánh. Muốn chặn thật thì dùng **CLS** hoặc phân quyền.
</details>

<details><summary><b>7.</b> Vì sao RLS ở SQL endpoint lại gây vấn đề cho Direct Lake?</summary>

Direct Lake **on SQL endpoint** phát hiện RLS/CLS/DDM ở tầng SQL sẽ **fallback sang DirectQuery** → chậm. Muốn giữ Direct Lake thì đặt RLS trong **semantic model (DAX)**.
</details>

<details><summary><b>8.</b> Endorsement có chặn truy cập không?</summary>

**Không.** Promoted/Certified chỉ là tín hiệu độ tin cậy, hoàn toàn không phải cơ chế bảo mật.
</details>

<details><summary><b>9.</b> Sensitivity label có đặc điểm gì đáng nhớ nhất?</summary>

**Kế thừa** xuống downstream item (ví dụ semantic model → report), và có thể đi kèm protection policy của Purview.
</details>

<details><summary><b>10.</b> Test RLS thế nào cho đúng?</summary>

Dùng tài khoản **Viewer** (hoặc user chỉ có quyền item ở mức đọc). Test bằng Contributor sẽ luôn thấy hết dữ liệu.
</details>

<details><summary><b>11.</b> Domain trong Fabric có phân quyền dữ liệu không?</summary>

**Không.** Domain là cơ chế **tổ chức và uỷ quyền quản trị** (gán workspace, domain admin, default sensitivity label), không phải cơ chế kiểm soát truy cập dữ liệu.
</details>

<details><summary><b>12.</b> User bị chặn ở SQL endpoint nhưng vẫn đọc được file bằng Spark — vì sao và sửa thế nào?</summary>

Vì chính sách T-SQL chỉ áp cho **đường SQL endpoint**. Muốn chặn cả đường Spark/OneLake phải dùng **OneLake security** (và kiểm tra item permission ReadAll).
</details>

## J. MINI-LAB 60 PHÚT

1. Tạo lakehouse có bảng `sales` gồm cột `Region` và `Salary`.
2. Mời một tài khoản test làm **Viewer**. Xác nhận tài khoản đó đọc được bảng qua SQL endpoint nhưng **không chạy được notebook**.
3. Tạo **RLS ở SQL endpoint** lọc theo `Region`. Đăng nhập tài khoản Viewer → xác nhận chỉ thấy vùng của mình.
4. Nâng tài khoản test lên **Contributor** → truy vấn lại → xác nhận **thấy toàn bộ** (chứng minh bypass). Hạ về Viewer.
5. Áp **CLS** để ẩn cột `Salary`; thử `SELECT Salary` bằng tài khoản Viewer → quan sát lỗi. Sau đó thay bằng **DDM** và so sánh hành vi (che hiển thị nhưng vẫn query được).
6. Tạo **OneLake security role** giới hạn theo `Region`; cấp **ReadAll** cho tài khoản test; thử đọc bằng Spark → xác nhận bị giới hạn giống SQL endpoint.
7. Gán **sensitivity label** cho lakehouse; tạo semantic model trên đó và quan sát **nhãn được kế thừa**.
8. **Endorse** một semantic model ở mức Promoted; xác nhận việc này không thay đổi quyền truy cập của ai.

**Ngưỡng đạt:** đọc thuộc khung 5 bước ở mục B, giải được 5 scenario bảo mật bất kỳ mà không cần gợi ý, và trả lời đúng ≥11/12 câu ở mục I.

---

# ⚡ CẬP NHẬT 05/11/2026 — SECURITY AND GOVERNANCE

## U1. Sáu bullet chính thức và cách map vào file này

| Bullet (study guide 30/10/2026) | Mục trong file | Cơ chế cần nắm |
|---|---|---|
| Implement workspace-level access controls | 2 | 4 workspace role; Admin/Member/Contributor đọc-ghi toàn bộ dữ liệu workspace |
| Implement item-level access controls | 3 | Share item: Read, ReadData, ReadAll, Build, Write… |
| Implement **row-level, column-level, object-level, and file-level** access controls | 4, 9 | **SQL**: RLS (security policy + predicate function), CLS (`GRANT SELECT (cols)`), OLS (`GRANT/DENY` trên object). **OneLake security roles**: table/folder (file-level), RLS, CLS trên Lakehouse. **Semantic model**: RLS/OLS |
| Implement dynamic data masking | 5 | `MASKED WITH (FUNCTION = 'default()' / 'email()' / 'partial(...)' / 'random(...)')`, `UNMASK` |
| Apply sensitivity labels to items | 6 | Purview labels, inheritance, default label theo domain |
| Endorse items | 7 | Promoted / Certified / Master data — **không phải** security |
| *(related)* Audit logs | 8 | Purview audit |
| *(related)* OneLake security như một cơ chế | 9 | Data access roles, DefaultReader |

## U2. OneLake security — các điểm đối chiếu lại (Learn, 09/2026)

- **Admin, Member, Contributor không bị OneLake security roles ràng buộc** — họ đã có read/write mọi dữ liệu trong workspace. **Viewer** và người chỉ có **Read item permission** thì **bị** ràng buộc.
- Mỗi Lakehouse có role dựng sẵn **DefaultReader**. Muốn giới hạn một Viewer chỉ thấy một folder/table → tạo role mới với scope hẹp **và loại user khỏi DefaultReader** (nếu không, quyền rộng của DefaultReader thắng vì nhiều role cộng dồn theo kiểu *most permissive*).
- **SQL analytics endpoint** phụ thuộc chế độ truy cập: *user identity* áp OneLake security của người dùng; *delegated identity* dùng quyền SQL (GRANT/REVOKE, RLS/CLS/DDM) và có thể cần ReadData.
- **Enforce OneLake security in authorized third-party engines** là **Preview**: engine bên ngoài được ủy quyền áp table permission, RLS, CLS lúc truy vấn.

## U3. Tính năng governance mới (Preview, 09/2026) — nhận diện

| Tính năng | Một dòng |
|---|---|
| **Item soft-delete and recovery events** | Sự kiện vòng đời item (soft-delete, recover) phát vào Real-Time Hub để tự động hóa và audit |
| **Snowflake security-role mirroring** | Đưa định nghĩa role của Snowflake sang Fabric khi mirror |
| **Workspace outbound access protection** mở rộng cho RTI, Fabric Maps, Operations Agent | Kiểm soát kết nối ra ngoài ở cấp workspace |

## U4. Câu hỏi tình huống cập nhật

<details><summary><b>SG-U1.</b> Analyst được gán workspace role Contributor. Bạn tạo OneLake security role chỉ cho đọc folder `Files/finance`. Analyst vẫn thấy mọi thứ. Vì sao, sửa thế nào?</summary>

Contributor **không bị** OneLake security roles ràng buộc. Hạ analyst xuống **Viewer** (hoặc chỉ share item với Read), gán vào role có scope `Files/finance`, và **loại khỏi DefaultReader**.
</details>

<details><summary><b>SG-U2.</b> Yêu cầu: người dùng truy vấn qua SQL analytics endpoint chỉ thấy dòng thuộc vùng của mình, còn Spark notebook của data engineer (Contributor) đọc đầy đủ. Lớp nào?</summary>

Với endpoint ở **user identity mode**: định nghĩa **RLS trong OneLake security role** — áp dụng nhất quán qua các engine hỗ trợ cho người bị ràng buộc (Viewer/Read). Với **delegated mode**: dùng **SQL RLS** (`CREATE SECURITY POLICY ... ADD FILTER PREDICATE`). Data engineer là Contributor nên không bị giới hạn ở cả hai cách.
</details>

<details><summary><b>SG-U3.</b> Cột `Email` phải hiện dạng `aXX@XXXX.com` cho analyst, nhưng nhóm compliance xem được giá trị thật. T-SQL nào?</summary>

```sql
ALTER TABLE dbo.Customer
ALTER COLUMN Email ADD MASKED WITH (FUNCTION = 'email()');
GRANT UNMASK TO [compliance-group];
```
DDM không phải ranh giới bảo mật tuyệt đối — người có quyền truy vấn tự do vẫn có thể suy luận giá trị; cần kết hợp RLS/CLS khi dữ liệu thật sự nhạy cảm.
</details>

<details><summary><b>SG-U4.</b> Đáp án "Certified endorsement giới hạn ai được đọc semantic model" đúng hay sai?</summary>

**Sai.** Endorsement chỉ là tín hiệu tin cậy để tìm kiếm/khám phá; không thay đổi quyền truy cập.
</details>
