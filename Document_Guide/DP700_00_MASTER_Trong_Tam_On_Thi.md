# DP-700 — FILE TỔNG CHỈ HUY: TRỌNG TÂM ÔN THI & PHẢN XẠ QUYẾT ĐỊNH

> **Bản viết ngày 06/09/2026 — cập nhật 05/11/2026.** Blueprint tham chiếu: [Study Guide DP-700](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700) (trang cập nhật **30/10/2026**) — ba domain đều có trọng số **30–35%**, tổng **49 bullet**. Đọc mục **⚡ CẬP NHẬT 05/11/2026** ngay bên dưới trước tiên.
> Đây là file **đọc mỗi ngày**. Mười file chuyên đề còn lại là nơi tra cứu chi tiết khi file này chỉ bạn tới.

---

## ⚡ CẬP NHẬT 05/11/2026 — ĐỌC PHẦN NÀY TRƯỚC

Mục này gom mọi thay đổi từ sau bản 06/09/2026. Mỗi dòng đã đối chiếu với trang Microsoft Learn ghi ở cột nguồn. Nếu bạn chỉ có 30 phút, hãy đọc **U3** (những điểm làm **đổi đáp án**) và làm **U6** (12 câu drill mới).

### U1. Blueprint chính thức đang hiển thị

Trang [Study Guide DP-700](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700) được Microsoft cập nhật ngày **30/10/2026**. Trang hiện **không còn dòng "Skills measured as of …" và không có bảng change log**, nên không xác định được ngày hiệu lực chính xác từ trang. Thứ cần học là danh sách bullet đang hiển thị:

| Domain (30–35% mỗi domain) | Functional group | Bullet chính thức | File |
|---|---|---|---|
| **D1** Implement and manage | Configure Fabric workspace settings | Spark · domain · OneLake · **data workflow** workspace settings (4) | [01](DP700_Domain1_Implement_Manage_01_Workspace_Settings.md) |
| | Implement lifecycle management | Version control · database projects · deployment pipelines (3) | [02](DP700_Domain1_Implement_Manage_02_Lifecycle_Management.md) |
| | Configure security and governance | Workspace-level · item-level · **row/column/object/file-level** · DDM · sensitivity labels · endorse (6) | [03](DP700_Domain1_Implement_Manage_03_Security_and_Governance.md) |
| | Orchestrate processes | **Choose between a pipeline and a notebook** · schedules & event-based triggers · orchestration patterns + parameters + dynamic expressions (3) | [04](DP700_Domain1_Implement_Manage_04_Orchestrate_Processes.md) |
| **D2** Ingest and transform | Design and implement loading patterns | Full & incremental · dimensional model · streaming loading pattern (3) | [D2-01](DP700_Domain2_Ingest_Transform_01_Design_Loading_Patterns.md) |
| | Ingest and transform batch data | Data store · **dataflows vs notebooks vs T-SQL** · shortcuts · mirroring · pipelines · PySpark/SQL/KQL · denormalize · group/aggregate · duplicate/missing/late-arriving (9) | [D2-02](DP700_Domain2_Ingest_Transform_02_Batch_Data.md) |
| | Ingest and transform streaming data | Streaming engine · eventstreams · Spark structured streaming · KQL · windowing (5) | [D2-03](DP700_Domain2_Ingest_Transform_03_Streaming_Data.md) |
| **D3** Monitor and optimize | Monitor Fabric items | Ingestion · transformation · semantic model refresh · alerts (4) | [D3-01](DP700_Domain3_Monitor_Optimize_01_Monitor_Items_Alerts.md) |
| | Identify and resolve errors | Pipeline · dataflow · notebook · eventhouse · eventstream · T-SQL (6) | [D3-02](DP700_Domain3_Monitor_Optimize_02_Identify_Resolve_Errors.md) |
| | Optimize performance | Lakehouse table · pipeline · warehouse · eventstreams & eventhouses · Spark · query (6) | [D3-03](DP700_Domain3_Monitor_Optimize_03_Optimize_Performance.md) |
| | | **Tổng: 16 + 17 + 16 = 49 bullet** | |

Hai ghi chú chính thức trên trang vẫn giữ nguyên và quyết định cách ôn: (1) các bullet **chỉ minh họa** cách đánh giá, **related topics có thể ra đề**; (2) phần lớn câu hỏi về tính năng **GA**, nhưng tính năng **Preview** phổ biến vẫn có thể xuất hiện.

### U2. So với blueprint 21/07/2026 mà bộ tài liệu dùng trước đây

| Bản 21/07/2026 (tài liệu cũ) | Study guide hiện hành | Cách ôn từ nay |
|---|---|---|
| Configure **Apache Airflow** workspace settings | Configure **data workflow** workspace settings | **Cùng một thứ.** "Data workflow" là tên cũ của **Apache Airflow job**; UI vẫn ghi *Default Data Workflow Setting* trong `Workspace settings > Data Factory > Apache Airflow Runtime Settings`. Học nguyên mục 5 file 01. |
| Security 8 bullet (có **audit logs** và **OneLake security** riêng) | 6 bullet; RLS/CLS/OLS/**file-level** gộp một dòng | **Vẫn học OneLake security** — đó chính là cơ chế thực hiện folder/file-level, RLS, CLS trên Lakehouse. Audit logs hạ xuống related topic: ôn ở mức "câu hỏi nào → audit log ở Purview". |
| Choose between **Dataflow Gen2**, pipeline, notebook | Choose between **a pipeline and a notebook** | Ở D1, câu hỏi xoáy vào pipeline vs notebook (orchestrate hay xử lý code). Dataflow Gen2 vẫn được hỏi ở D2 "dataflows vs notebooks vs T-SQL". |
| Dataflows, notebooks, **KQL**, T-SQL | Dataflows, notebooks, T-SQL | **Không bỏ KQL**: KQL vẫn nằm ở "Transform data by using PySpark, SQL, and KQL", "Process data by using KQL" và windowing. |
| Streaming 7 bullet (có **native tables vs shortcuts**, **query acceleration**) | 5 bullet | Hai chủ đề này thành related topic của "choose streaming engine / process data by using KQL". Ôn ở mức **bảng quyết định một trang**, không cần sâu. |
| Errors 7 bullet (có **OneLake shortcut errors**) | 6 bullet | Shortcut errors thành related topic; vẫn hay xuất hiện lồng trong lỗi pipeline/notebook (401/403, connection, path). |

> **Không xóa nội dung cũ.** Mọi mục trên vẫn giữ trong file chuyên đề vì Microsoft cho phép hỏi related topics. Chỉ **hạ mức ưu tiên** khi thời gian ôn hạn hẹp.

### U3. Thay đổi nền tảng làm ĐỔI ĐÁP ÁN

| # | Chủ đề | Tài liệu cũ nói | Đúng tại 11/2026 | Áp dụng khi làm bài | Nguồn |
|:---:|---|---|---|---|---|
| 1 | **Fabric Runtime 2.0** | Public Preview | **GA và là runtime mặc định** cho workspace và Environment mới; Microsoft khuyến nghị cho production. Spark **4.1**, Delta **4.2**, Python **3.13**, Java **21**, Scala **2.13**. **Runtime 1.3 = EOSA** (đã công bố hết hỗ trợ) | "Runtime nào cho workload mới/production?" → **Runtime 2.0**. Vẫn phải kiểm tra thư viện/JAR trước khi chuyển workload cũ | [Runtimes](https://learn.microsoft.com/en-us/fabric/data-engineering/runtime) · [Runtime 2.0](https://learn.microsoft.com/en-us/fabric/data-engineering/runtime-2-0) |
| 2 | Delta 4.x features trên Runtime 2.0 | Experimental, chỉ Spark | **Vẫn đúng.** Không bật nếu cùng table phải được SQL endpoint/Warehouse/Power BI đọc; nâng protocol **không đảo ngược được** | Table dùng chung nhiều engine → **không** bật feature Delta 4.2 | [Migration guide](https://learn.microsoft.com/en-us/fabric/data-engineering/runtime-2-0-migration-guide) |
| 3 | Spark Autotune | Preview, chỉ Runtime 1.2 | **Vẫn chỉ Runtime 1.2** (đã hết hỗ trợ) — **không bật được trên 1.3 hay 2.0**; query tuning Preview, tắt mặc định | Đáp án "bật Autotune" cho workload hiện đại gần như luôn **sai**; tune thủ công `shuffle.partitions`, broadcast threshold, `maxPartitionBytes` hoặc dựa vào AQE | [Autotune](https://learn.microsoft.com/en-us/fabric/data-engineering/autotune) |
| 4 | Spark đọc Event Hubs | `format("eventhubs")` | **`EventHubConnector` bị gỡ khỏi Runtime 2.0** → dùng **Kafka endpoint** của Event Hubs: `format("kafka")`, `<namespace>.servicebus.windows.net:9093`, `SASL_SSL` + `PLAIN`; payload nằm ở cột **`value`** | Code dùng `eventhubs` chạy lỗi trên 2.0 → sửa sang Kafka; giữ checkpoint hoặc restart có kiểm soát với sink idempotent | [Migration guide](https://learn.microsoft.com/en-us/fabric/data-engineering/runtime-2-0-migration-guide) |
| 5 | Mặc định ghi Delta trên Runtime 2.0 | Theo 1.3 | **Deletion vectors bật mặc định** cho table mới; `spark.sql.files.maxPartitionBytes` = **512 MiB** (1.3: 128 MiB); `optimizeWrite.enabled` **không còn set mặc định** (1.3: true) nhưng **adaptive target file size**, **fast optimize**, **file-level target** bật; `spark.sql.ansi.enabled` Fabric đặt **false** | Đọc kế hoạch/small files trên 2.0 với các default mới; không giả định optimize write đang bật | [Migration guide](https://learn.microsoft.com/en-us/fabric/data-engineering/runtime-2-0-migration-guide) |
| 6 | V-Order | Tắt mặc định workspace mới | **Vẫn đúng**: `spark.sql.parquet.vorder.default=false`; bật bằng table property `delta.parquet.vorder.enabled`, writer option `parquet.vorder.enabled` hoặc resource profile read-heavy | Table read-heavy cho Direct Lake/SQL → cân nhắc V-Order + benchmark | [V-Order](https://learn.microsoft.com/en-us/fabric/data-engineering/delta-optimization-and-v-order) |
| 7 | Git — đổi branch | Hay bị nhớ nhầm là "chỉ Admin" | **Admin luôn được.** Member/Contributor được đổi branch khi admin bật **"Allow users with at least Contributor role to change Git branch"**, workspace đang kết nối Git và user có write trên mọi item. **Connect/Disconnect vẫn chỉ Admin**; Commit/Update cần Contributor | Câu "least privilege để dev tự đổi branch" → Contributor **+ bật setting**, không phải nâng Admin | [Git permissions](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-integration-process) |
| 8 | Surge protection | Chỉ cấp capacity | Capacity-level (background **rejection/recovery threshold**, Admin portal > Capacity settings) **+ Workspace-level (Preview)**: chặn một workspace vượt % CU trong **cửa sổ 24h trượt**; trạng thái **Available / Mission critical / Blocked**; cấu hình ở **OneLake catalog > Govern > Capacities** | "Một workspace dev ngốn hết capacity" → **workspace-level surge protection**; workspace quan trọng → **Mission critical** | [Surge protection](https://learn.microsoft.com/en-us/fabric/enterprise/surge-protection) |
| 9 | Copy job SCD Type 2 | Preview | **Vẫn Preview**; không hỗ trợ khi CDC từ **Oracle**, hoặc khi bạn tự tạo schema ở đích. Custom staging (cho CDC/SCD2), SQL CDC custom capture names và **Copy job workspace monitoring** là **GA 09/2026** | "SCD2 ít công sức nhất" → Copy job (CDC + SCD2) nếu nguồn hỗ trợ; nguồn Oracle → tự viết MERGE/SCD2 | [CDC in Copy job](https://learn.microsoft.com/fabric/data-factory/cdc-copy-job) · [What's new](https://learn.microsoft.com/en-us/fabric/fundamentals/whats-new) |
| 10 | Warehouse data clustering | Preview | **Vẫn Preview**: `WITH (CLUSTER BY (...))` chỉ lúc `CREATE TABLE`/CTAS, **1–4 cột**, không thêm/sửa sau, không dùng cột `IDENTITY`, `SELECT INTO` không hỗ trợ | Đổi cột clustering → **CTAS sang table mới** | [Data clustering](https://learn.microsoft.com/en-us/fabric/data-warehouse/data-clustering) |
| 11 | Materialized Lake View | SQL `CREATE MATERIALIZED LAKE VIEW` | Thêm **PySpark authoring (Preview, chỉ full refresh)**; refresh tối ưu chọn **incremental / full / skip**, incremental cần **Change Data Feed** trên source; constraint `ON MISMATCH DROP` hoặc `FAIL` | "MLV luôn full refresh" → bật **CDF** trên source table | [MLV overview](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/overview-materialized-lake-view) |

### U4. Tính năng mới (Preview) từ 07–09/2026 — biết tên và mục đích là đủ

| Workload | Tính năng (Preview) | Một dòng để nhớ |
|---|---|---|
| CI/CD | **Deployment plans** | Canvas trực quan cho thứ tự deploy item và pre/post actions |
| CI/CD | **File-level Git commit** | Commit từng file, phần chưa xong vẫn ở workspace |
| CI/CD | **Branch workspace admin profiles** | Admin định nghĩa identity/settings chuẩn cho branch workspace của dev |
| CI/CD | **Warehouse CI/CD 2.0** | Định nghĩa item v2.0 dựa trên DacFx, so sánh deploy chính xác hơn |
| Data Factory | **Pipeline-level dependencies** | Pipeline hạ nguồn chỉ chạy khi điều kiện pipeline thượng nguồn thỏa |
| Data Factory | **Fabric Actions activity** · **Refresh Materialized Lake View activity** | Gọi Fabric REST có xác thực / refresh MLV ngay trong pipeline |
| Data Factory | **Copy job ↔ Eventstream** · **Dataflow Gen1 → Gen2 Upgrade Wizard** | Eventstream làm nguồn/đích Copy job / nâng cấp tại chỗ giữ ID, schedule |
| Monitoring | **Dataflow refresh details in Monitoring hub** | Xem chi tiết refresh theo query ngay trong hub |
| RTI | **Eventstream reference data enrichment** | Join event với bảng Delta tham chiếu trong Lakehouse (no-code hoặc SQL) |
| RTI | **Eventstream processing logs → Workspace monitoring** | Cảnh báo/lỗi/phục hồi của xử lý stream vào KQL database monitoring |
| RTI | **Eventhouse workspace-monitoring logs** | Ghi nhận throttling, suboptimal size, scale-out |
| RTI | **Update policies on accelerated shortcuts** | Transform lúc ingest cho OneLake shortcut có query acceleration |
| Warehouse | **T-SQL `QUALIFY`**, mở rộng grouping | Lọc theo kết quả window function không cần subquery |
| Spark | **Custom live pools** (Runtime 2.0), **delegated pool management** | Pool giữ ấm; Member cấu hình qua Environment khi admin cho phép |

> **Chiến thuật:** Preview mới ra hiếm khi thành câu hỏi chính. Dùng bảng này để **loại đáp án**: thấy tên lạ trong đáp án thì biết nó là gì, không bị lừa.

### U5. Practice Assessment — đổi nơi làm

Study guide hiện khuyến nghị **Practice Assessment mới trên AI Skills Navigator** (đăng nhập tại [aka.ms/DP700-PA](http://aka.ms/DP700-PA)) vì sát trải nghiệm thi hơn. Bản cũ trên Microsoft Learn ([aka.ms/DP700-LearnPA](https://aka.ms/DP700-LearnPA)) **không còn được cập nhật**. Từ nay, readiness gate ở mục 9 đo bằng **bản mới**.

### U6. Drill Vòng 7 — 12 câu cập nhật 11/2026

<details><summary><b>U-1.</b> Blueprint hiện ghi "Configure data workflow workspace settings". Đó là cấu hình của item nào, ở đâu?</summary>

**Apache Airflow job** (tên cũ "Data workflows"). `Workspace settings > Data Factory > Apache Airflow Runtime Settings` → *Default Data Workflow Setting*: **Starter pool** (mặc định, tắt sau 20 phút không hoạt động, cho dev) hoặc **Custom pool** (always-on, node size Small/Large, autoscale, extra nodes — cho production).
</details>

<details><summary><b>U-2.</b> Team tạo workspace mới cho dự án Spark năm 2026. Runtime mặc định là gì và có nên giữ cho production?</summary>

**Runtime 2.0** — GA, mặc định cho workspace/Environment mới, Microsoft khuyến nghị cho production. Ngoại lệ: không bật feature Delta 4.2 experimental trên table mà SQL endpoint/Power BI cũng đọc.
</details>

<details><summary><b>U-3.</b> Notebook streaming dùng `spark.readStream.format("eventhubs")` chạy tốt trên 1.3 nhưng lỗi sau khi chuyển 2.0. Vì sao, sửa thế nào?</summary>

`EventHubConnector` **đã bị gỡ** khỏi Runtime 2.0. Chuyển sang **Kafka endpoint** của Event Hubs (`format("kafka")`, port **9093**, `SASL_SSL`/`PLAIN`, username `$ConnectionString`), parse cột **`value`**. Kiểm tra checkpoint cũ có restart được không; nếu không, restart với checkpoint mới và sink idempotent.
</details>

<details><summary><b>U-4.</b> Đáp án gợi ý "bật Spark Autotune trên Runtime 2.0 để tự tối ưu shuffle partitions". Đúng hay sai?</summary>

**Sai.** Autotune chỉ tương thích **Runtime 1.2** (đã hết hỗ trợ), không bật được trên 1.3/2.0. Dùng AQE, chỉnh `spark.sql.shuffle.partitions`, broadcast join, xử lý skew.
</details>

<details><summary><b>U-5.</b> Dev (Contributor) cần tự đổi Git branch của workspace dev mà không được nâng Admin. Cần gì?</summary>

Admin bật **"Allow users with at least Contributor role to change Git branch"**; workspace đang kết nối Git; dev có **write trên mọi item**. Connect/disconnect Git vẫn chỉ Admin.
</details>

<details><summary><b>U-6.</b> Một workspace sandbox liên tục ngốn CU làm report production bị chậm. Admin muốn chặn riêng workspace đó, không đụng các workspace khác. Dùng gì?</summary>

**Workspace-level surge protection (Preview)**: đặt ngưỡng % CU trên cửa sổ 24h trượt (OneLake catalog > Govern > Capacities > Surge protection > Workspace consumption); đánh dấu workspace production là **Mission critical**; workspace vượt ngưỡng sẽ bị **Blocked**.
</details>

<details><summary><b>U-7.</b> Copy job CDC từ Oracle, yêu cầu giữ lịch sử SCD Type 2. Copy job có tự làm được không?</summary>

**Không** — SCD Type 2 trong Copy job (Preview) **chưa hỗ trợ CDC từ Oracle**. Dùng Copy job ở chế độ Merge vào staging rồi tự viết SCD2 bằng Delta `MERGE`/notebook (hoặc T-SQL ở Warehouse).
</details>

<details><summary><b>U-8.</b> Muốn đổi cột clustering của bảng Warehouse đã tạo với `CLUSTER BY (CustomerID)`. Làm sao?</summary>

Không `ALTER` được. **CTAS** sang bảng mới `WITH (CLUSTER BY (...))`, rồi đổi tên. Tối đa 4 cột; không dùng cột `IDENTITY`; `SELECT INTO` không hỗ trợ clustering.
</details>

<details><summary><b>U-9.</b> MLV luôn chạy full refresh dù source chỉ thêm vài dòng. Nguyên nhân thường gặp?</summary>

Source table **chưa bật Change Data Feed** → optimal refresh chỉ còn full/skip. Bật CDF (`delta.enableChangeDataFeed = true`) trên source; kiểm tra truy vấn thuộc pattern hỗ trợ (GROUP BY, left outer/semi join, CTE). MLV tạo bằng PySpark (Preview) luôn full refresh.
</details>

<details><summary><b>U-10.</b> Bảng mới tạo trên Runtime 2.0 có deletion vectors không? Điều đó ảnh hưởng gì?</summary>

**Có** — Runtime 2.0 đặt `spark.databricks.delta.properties.defaults.enableDeletionVectors = true`. DELETE/UPDATE/MERGE ghi nhanh hơn (đánh dấu dòng xóa thay vì viết lại file), nhưng cần **OPTIMIZE/purge** định kỳ; kiểm tra mọi engine đọc table hỗ trợ.
</details>

<details><summary><b>U-11.</b> Blueprint mới không còn bullet "OneLake security". Có nên bỏ ôn?</summary>

**Không.** Bullet "row-level, column-level, object-level, and **file-level** access controls" được thực hiện bằng OneLake security roles (folder/file, RLS, CLS) và SQL (RLS/CLS/DDM). Admin/Member/Contributor vẫn **không bị** OneLake security roles ràng buộc; Viewer và người có quyền Read item thì bị.
</details>

<details><summary><b>U-12.</b> Bạn nên đo readiness bằng Practice Assessment nào?</summary>

**Bản mới trên AI Skills Navigator** (aka.ms/DP700-PA). Bản legacy trên Learn không còn được cập nhật, điểm cao ở đó không còn đại diện cho đề hiện hành.
</details>

---

## 0. CÁCH DÙNG FILE NÀY

Bộ tài liệu này phủ đủ **49/49 bullet** của study guide hiện hành, cộng các related topic của bản 21/07/2026. Nhưng phủ đủ kiến thức và **thi đậu** là hai việc khác nhau. DP-700 là kỳ thi **scenario-based**: đề hiếm khi hỏi "X là gì", mà hỏi "trong ngữ cảnh này, chọn X hay Y, và vì sao".

> Mục tiêu của file này là đưa kiến thức từ mức **"nhìn thấy thì nhận ra"** lên mức **"không nhìn tài liệu vẫn nói lại được và áp dụng được"**.

Hai mức đó khác nhau rất xa. Người ở mức nhận ra sẽ làm tốt khi đề hỏi đúng câu chữ đã đọc, và lúng túng khi đề xoay góc khác. Người ở mức tái tạo trả lời ổn định bất kể đề xoay thế nào.

**Cách học tương ứng với từng mức:**

| Học ở mức nhận ra | Học ở mức tái tạo |
|---|---|
| Đọc lại file chuyên đề từ đầu tới cuối | Đọc mục 2–5 lấy khung, rồi **drill mục 6** khi đã che đáp án |
| Ôn một domain mỗi đợt | **Mỗi ngày chạm cả 3 domain** — kiến thức không ôn sẽ nguội trong 2–3 tuần |
| Học "X là gì" | Học **"khi nào chọn X thay vì Y, và vì sao 3 đáp án kia sai"** |
| Đo bằng cảm giác "đã đọc rồi" | Đo bằng **số câu recall trả lời đúng khi che đáp án** |

**Routine 60 phút/ngày được khuyến nghị:**

```text
20 phút  →  Drill active recall (mục 6) — che đáp án, nói thành tiếng
25 phút  →  Học sâu 1 mục trong file chuyên đề của trọng tâm tuần này
15 phút  →  Gõ lại code/lab từ trí nhớ (không copy/paste)
```

---

## 1. TRỌNG TÂM: NHỮNG VÙNG QUYẾT ĐỊNH KẾT QUẢ

### 1.1 Ba domain, trọng số bằng nhau

| Domain | Trọng số | Nhóm kỹ năng (functional group) | Số objective |
|---|:---:|---|:---:|
| **D1 — Implement and manage an analytics solution** | 30–35% | Workspace settings · Lifecycle management · Security & governance · Orchestrate processes | 4 + 3 + 6 + 3 = 16 |
| **D2 — Ingest and transform data** | 30–35% | Loading patterns · Batch data · Streaming data | 3 + 9 + 5 = 17 |
| **D3 — Monitor and optimize an analytics solution** | 30–35% | Monitor Fabric items · Identify and resolve errors · Optimize performance | 4 + 6 + 6 = 16 |

Điểm quan trọng của study guide hiện hành (giữ nguyên từ bản 21/07/2026): **cả ba domain đều 30–35%**. Không còn domain nào "nhẹ" để bỏ qua — đặc biệt **Monitor and optimize** nay nặng ngang hai domain kia. Nhiều thí sinh vẫn ôn theo trọng số cũ (D3 chỉ 25–30%) và mất điểm ở đúng chỗ này.

### 1.2 Bảy vùng khó nhất của DP-700

Đây là các nhóm kỹ năng mà thí sinh mất điểm nhiều nhất, xếp theo mức độ khó và tần suất xuất hiện dưới dạng câu hỏi scenario:

| Ưu tiên | Vùng | File chuyên đề | Mục tiêu | Vì sao khó |
|:---:|---|---|:---:|---|
| **1** | Optimize performance | [D3-03 Optimize Performance](DP700_Domain3_Monitor_Optimize_03_Optimize_Performance.md) | ≥85% | Đề mô tả **triệu chứng** rồi hỏi nguyên nhân/biện pháp; dễ chọn "biện pháp nghe hợp lý" thay vì biện pháp khớp triệu chứng |
| **2** | Design and implement loading patterns | [D2-01 Loading Patterns](DP700_Domain2_Ingest_Transform_01_Design_Loading_Patterns.md) | ≥85% | Hỏi **chi tiết triển khai** (idempotency, SCD2, late-arriving), không hỏi khái niệm |
| **3** | Workspace settings | [01 Workspace Settings](DP700_Domain1_Implement_Manage_01_Workspace_Settings.md) | ≥85% | Rất dễ nhầm **tenant setting** với **workspace setting**; ma trận role hay bị nhớ sai |
| **4** | Implement lifecycle management | [02 Lifecycle Management](DP700_Domain1_Implement_Manage_02_Lifecycle_Management.md) | ≥85% | Ba công cụ Git / deployment pipeline / variable library có vai trò chồng lấn trong đầu người học |
| **5** | Identify and resolve errors | [D3-02 Resolve Errors](DP700_Domain3_Monitor_Optimize_02_Identify_Resolve_Errors.md) | ≥80% | 6 bullet (+ shortcut errors là related topic); đòi hỏi biết **mở đúng công cụ chẩn đoán** trước khi sửa |
| **6** | Monitor Fabric items | [D3-01 Monitor Items](DP700_Domain3_Monitor_Optimize_01_Monitor_Items_Alerts.md) | ≥80% | Bốn công cụ monitoring rất dễ lẫn vào nhau |
| **7** | Ingest and transform streaming data | [D2-03 Streaming Data](DP700_Domain2_Ingest_Transform_03_Streaming_Data.md) | ≥80% | 5 bullet (+ native tables/shortcuts, query acceleration là related topic); KQL, windowing và checkpoint là phần nhiều người bỏ qua |

**Ba nhóm còn lại — không được để nguội:**

| Vùng | File | Ghi chú |
|---|---|---|
| Security & governance | [03 Security](DP700_Domain1_Implement_Manage_03_Security_and_Governance.md) | **6 bullet** (+ audit logs, OneLake security là related topic nhưng vẫn là cơ chế thực hiện file-level/RLS/CLS). Rủi ro cao nếu chủ quan |
| Ingest and transform batch data | [D2-02 Batch Data](DP700_Domain2_Ingest_Transform_02_Batch_Data.md) | 9 objective — nhóm lớn thứ nhì |
| Orchestrate processes | [04 Orchestrate](DP700_Domain1_Implement_Manage_04_Orchestrate_Processes.md) | 3 objective nhưng xuất hiện xuyên suốt case study |

> **Nguyên tắc chống nguội:** kiến thức không được chạm lại trong 2–3 tuần sẽ tụt về mức nhận ra. Vì vậy lịch ở mục 8 luôn bố trí **mỗi tuần chạm cả ba domain**, chứ không dồn một domain một đợt.

---

## 2. BẢN ĐỒ FABRIC TRONG MỘT TRANG

Nếu chỉ nhớ được một hình trong cả bộ tài liệu, hãy nhớ hình này. Gần như mọi câu hỏi DP-700 đều là câu hỏi **"chọn đúng ô nào trong sơ đồ này"**.

```text
                     ┌──────────────────── CAPACITY (F SKU) ────────────────────┐
                     │  Toàn bộ mọi thứ bên dưới đều tiêu thụ CU từ đây.        │
                     │  Hết CU → smoothing → throttling → job chậm hoặc bị chặn │
                     └──────────────────────────────────────────────────────────┘

  NGUỒN                ĐƯA VÀO                LƯU TRỮ              BIẾN ĐỔI            DÙNG
  ─────                ───────                ────────             ────────            ────
  DB (SQL, Snowflake)  Mirroring ──────┐
  ADLS / S3 / GCS      Shortcut ───────┤      ╔═══════════╗       Notebook/PySpark    Power BI
  File, API, SaaS      Copy Job ───────┼────► ║  OneLake  ║ ────► Dataflow Gen2   ──► Direct Lake
  Event Hub, Kafka     Pipeline Copy ──┤      ║  (1 kho)  ║       T-SQL (WH)          SQL endpoint
  IoT, HTTP            Dataflow Gen2 ──┤      ╚═══════════╝       KQL (EH)            KQL/RTI
                       Eventstream ────┘         │  │  │                              dashboard
                                                 │  │  │
                                    Lakehouse ◄──┘  │  └──► Eventhouse (KQL)
                                    (Delta/Spark)   │        time-series, hot cache
                                                    │
                                              Warehouse (T-SQL)
                                              star schema, ACID multi-table

  ĐIỀU PHỐI:  Data Pipeline (schedule / event trigger / ForEach / retry / alert)
  BẢO MẬT:    Workspace role → Item permission → OneLake security → SQL RLS/CLS/DDM → Semantic model RLS
  VẬN HÀNH:   Monitoring Hub (job) · Capacity Metrics (CU) · Workspace Monitoring (log KQL) · Spark UI · Query Insights
  CI/CD:      Git integration (version) → Deployment pipeline (promote) → Variable library (config theo stage)
```

### 2.1 Ví von cho dễ nhớ

| Thành phần Fabric | Ví von | Câu chốt |
|---|---|---|
| **OneLake** | Kho trung tâm duy nhất của công ty | Một tenant, một OneLake. Mọi item đều ghi vào đây |
| **Shortcut** | Tấm biển chỉ đường tới kệ hàng ở kho khác | Trỏ tới dữ liệu, **không sao chép** |
| **Mirroring** | Xe tải tự động chở hàng từ kho đối tác về, liên tục | Replicate **gần thời gian thực** từ DB nguồn, gần như không cần ETL |
| **Lakehouse** | Nhà kho hàng rời (Delta/Parquet) + xe nâng Spark | Mở, linh hoạt, hợp semi-structured, medallion |
| **Warehouse** | Siêu thị xếp kệ ngay ngắn theo T-SQL | Star schema, transaction đa bảng, người dùng SQL |
| **Eventhouse (KQL)** | Camera an ninh ghi log theo thời gian | Time-series, telemetry, truy vấn siêu nhanh trên hot cache |
| **Capacity (CU)** | Đồng hồ điện của cả nhà máy | Hết công suất → cả nhà máy chậm, không riêng một máy |
| **Deployment pipeline** | Băng chuyền Dev → Test → Prod | Chuyển **định nghĩa item**, không chuyển dữ liệu |
| **Git integration** | Sổ ghi lịch sử sửa bản thiết kế | Chỉ version **metadata/definition**, không version dữ liệu trong bảng |
| **Variable library** | Bảng cấu hình theo môi trường | Mỗi stage chọn một value set riêng |

### 2.2 Luồng suy nghĩ khi gặp bất kỳ câu hỏi nào

```text
1. Đề đang ở ô nào của sơ đồ?  (đưa vào / lưu trữ / biến đổi / điều phối / bảo mật / vận hành)
2. Constraint nào loại đáp án?  (no-code / real-time / zero-copy / least privilege / minimize cost / minimize admin effort)
3. Engine nào thực sự chạy?     (Spark / T-SQL Polaris / Mashup M / Kusto / Copy runtime)
4. Đáp án nào đủ yêu cầu với ít vận hành nhất và quyền tối thiểu?
5. Vì sao 3 đáp án còn lại sai?  ← Không trả lời được câu này = đang đoán
```

---

## 3. TÁM CỤM NHẦM LẪN GIẾT ĐIỂM

Đây là nơi thí sinh mất điểm nhiều nhất. Với mỗi cụm, hãy học **ranh giới quyết định** (một câu hỏi duy nhất phân tách hai lựa chọn), không học định nghĩa.

### Cụm 1 — Shortcut ↔ Mirroring ↔ Copy Job ↔ Pipeline Copy ↔ Dataflow Gen2

| Lựa chọn | Ranh giới quyết định (câu hỏi tự đặt) | Chọn khi |
|---|---|---|
| **Shortcut** | "Có được phép *không* sao chép dữ liệu không?" | Zero-copy; dữ liệu ở ADLS/S3/GCS/lakehouse khác; muốn thấy ngay khi nguồn đổi |
| **Mirroring** | "Nguồn có phải *database* và cần bản sao gần realtime, gần như không cấu hình ETL?" | Azure SQL DB/MI, Snowflake, Cosmos DB, PostgreSQL… → replicate liên tục vào OneLake |
| **Copy Job** | "Chỉ cần *di chuyển* dữ liệu, có sẵn full/incremental/CDC, không cần orchestration?" | Ingest đơn giản; UI chọn Append/Merge/Overwrite/SCD2 |
| **Pipeline (Copy activity)** | "Có cần *nhiều bước*, rẽ nhánh, retry, ForEach, alert không?" | Điều phối end-to-end, metadata-driven |
| **Dataflow Gen2** | "Người làm có phải *citizen dev* dùng Power Query, transform low-code không?" | Transform kéo-thả, chuẩn hoá, nhiều nguồn nhỏ |

> **Bẫy kinh điển:** đề nói "tránh trùng lặp dữ liệu và luôn thấy dữ liệu mới nhất từ S3" → **Shortcut**, không phải Copy Job. Đề nói "giảm thiểu ETL cho một Azure SQL Database đang chạy production" → **Mirroring**, không phải pipeline chạy mỗi 5 phút.

### Cụm 2 — Lakehouse ↔ Warehouse ↔ Eventhouse

| | Lakehouse | Warehouse | Eventhouse (KQL DB) |
|---|---|---|---|
| Engine chính | Spark | T-SQL | Kusto (KQL) |
| Ghi bằng | Spark, pipeline, dataflow | T-SQL DML, `COPY INTO`, CTAS | Eventstream, ingestion API |
| Hợp với | Semi/unstructured, medallion, ML | Star schema, transaction nhiều bảng | Log/telemetry/time-series |
| SQL endpoint | **Chỉ đọc** | Đọc **và ghi** | KQL + một phần T-SQL |
| Câu chốt | "Data engineer làm Spark" | "SQL developer cần T-SQL đầy đủ" | "Sự kiện có timestamp, khối lượng khổng lồ" |

> **Ranh giới quyết định:** hỏi *"có cần UPDATE/DELETE bằng T-SQL và transaction nhiều bảng không?"* → có thì **Warehouse**, không thì Lakehouse. Hỏi *"dữ liệu có phải luồng sự kiện theo thời gian không?"* → có thì **Eventhouse**.

### Cụm 3 — Monitoring Hub ↔ Capacity Metrics ↔ Workspace Monitoring ↔ Spark UI ↔ Query Insights

| Công cụ | Trả lời câu hỏi | KHÔNG trả lời được |
|---|---|---|
| **Monitoring Hub** | "Job nào đang chạy / đã fail / fail ở activity nào?" | Không chứng minh được capacity thiếu CU |
| **Capacity Metrics App** | "Capacity có quá tải, throttling, ai ăn CU nhiều nhất?" | Không cho stack trace lỗi của một notebook |
| **Workspace Monitoring** | "Cho tôi log chi tiết (query text, duration) để truy vấn bằng KQL" | Không thay thế Capacity Metrics (metric ở đây không smoothing/bursting) |
| **Spark UI / history server** | "Stage nào chậm, task nào skew, có spill không?" | Không nói gì về pipeline hay warehouse |
| **Query Insights (Warehouse)** | "Query T-SQL nào chạy lâu, chạy nhiều, lịch sử ra sao?" | Không dùng cho Spark hay KQL |

> **Ranh giới quyết định:** *một* job lỗi → Monitoring Hub. *Nhiều* workload cùng chậm → Capacity Metrics. Cần **truy vấn** log → Workspace Monitoring. Chậm bên trong **một** Spark job → Spark UI. Chậm bên trong **một** query T-SQL → Query Insights.

### Cụm 4 — OPTIMIZE ↔ VACUUM ↔ V-Order ↔ Z-Order ↔ Partitioning ↔ Liquid Clustering

| Kỹ thuật | Giải quyết vấn đề gì | KHÔNG giải quyết |
|---|---|---|
| `OPTIMIZE` | **Nhiều file nhỏ** → gộp thành file lớn | Không xoá file cũ, không giảm dung lượng lưu trữ |
| `VACUUM` | **Rác lưu trữ** — xoá file không còn được tham chiếu (mặc định giữ 7 ngày) | Không làm query nhanh hơn đáng kể; xoá quá sớm phá time travel |
| **V-Order** | Đọc nhanh hơn cho Direct Lake/VertiPaq (tối ưu lúc ghi) | Tốn thêm ~15% thời gian ghi; **tắt mặc định ở workspace mới** |
| **Z-Order** | Data skipping theo vài cột hay lọc | Không dùng chung với liquid clustering trên cùng bảng |
| **Partitioning** | Cắt bớt dữ liệu quét theo cột low-cardinality (ngày, vùng) | Partition theo cột high-cardinality → tự tạo ra small files |
| **Liquid clustering** | Thay thế partition + Z-Order, linh hoạt hơn | Không kết hợp với partitioning/Z-Order cùng bảng |

> **Ranh giới quyết định:** triệu chứng **"query chậm dần, thư mục có hàng nghìn file nhỏ"** → `OPTIMIZE`. Triệu chứng **"dung lượng OneLake phình to dù số dòng không tăng"** → `VACUUM`. Yêu cầu **"tăng tốc report Power BI Direct Lake"** → V-Order (+ `OPTIMIZE`).

### Cụm 5 — Workspace role ↔ Item permission ↔ OneLake security ↔ SQL RLS/CLS/DDM ↔ Semantic model RLS

Thứ tự giải mọi câu hỏi bảo mật — **luôn đi đúng 5 bước này**:

```text
1. Identity nào?          (user, group, service principal, workspace identity)
2. Workspace role gì?     Admin / Member / Contributor  → BYPASS OneLake RLS/CLS
                          Viewer                        → BỊ RÀNG BUỘC bởi RLS/CLS/DDM
3. Item permission gì?    Read / ReadData / ReadAll / Write / Reshare / Execute
4. Đi vào bằng đường nào? Spark  |  SQL analytics endpoint  |  OneLake API/shortcut  |  Semantic model
5. Data policy nào áp?    OneLake security (OLS/RLS/CLS)  |  T-SQL RLS/CLS/DDM  |  DAX RLS
```

> **Bẫy số 1:** muốn cho một người *chỉ đọc và bị RLS ràng buộc* → cấp **Viewer**, không phải Contributor.
> **Bẫy số 2:** chính sách T-SQL (RLS/CLS/DDM) chỉ bảo vệ đường vào **SQL endpoint**; người đọc file Parquet trực tiếp qua Spark/OneLake không bị chặn nếu không có OneLake security tương ứng.
> **Bẫy số 3:** Viewer **không tự động chạy được notebook** — muốn chạy phải có quyền Execute trên notebook.

### Cụm 6 — Git integration ↔ Deployment pipeline ↔ Variable library ↔ Database project

| Công cụ | Nhiệm vụ duy nhất của nó | KHÔNG làm được |
|---|---|---|
| **Git integration** | Version control: commit, branch, history, PR | Không promote Dev→Prod; không đồng bộ **dữ liệu** trong bảng |
| **Deployment pipeline** | Promote **định nghĩa item** giữa các stage (2–10 stage) | Không thay thế version history; không copy dữ liệu thật |
| **Variable library** | Giá trị cấu hình khác nhau theo từng stage (connection, path, tên DB) | Không phải kho secret thay cho Key Vault |
| **Database project (SQL project)** | Định nghĩa schema warehouse/SQL DB dạng code, build ra `.dacpac` | Không quản lý item Fabric ngoài SQL |

> **Ranh giới quyết định:** "giữ lịch sử thay đổi + nhiều người làm trên nhánh" → **Git**. "Đưa bản đã kiểm thử sang Prod" → **Deployment pipeline**. "Cùng một pipeline nhưng Test trỏ DB test, Prod trỏ DB prod" → **Variable library** (ưu tiên hơn deployment rule).

### Cụm 7 — Eventstream ↔ Spark Structured Streaming ↔ KQL update policy

| | Eventstream | Spark Structured Streaming | KQL update policy |
|---|---|---|---|
| Kiểu | No-code kéo thả | Code-first PySpark | Khai báo trong Eventhouse |
| Hợp khi | Ingest + transform nhẹ + route nhiều đích | Logic phức tạp, join, ML, MERGE | Biến đổi **ngay lúc ingest** từ bảng nguồn sang bảng đích |
| Điểm phải nhớ | at-least-once → **có thể trùng**, cần dedup | `checkpointLocation` **duy nhất cho mỗi query** | Không join bảng khác nếu source có streaming ingestion policy |
| Windowing | Tumbling/Hopping/Sliding/Session trong toán tử aggregate | `window()` + `withWatermark()` | `bin()` / `summarize` |

### Cụm 8 — Bốn loại window (ra thi rất thường xuyên)

```text
TUMBLING (5 phút)      |####|####|####|####|     cố định, KHÔNG chồng lấn, mỗi event thuộc đúng 1 window
                        0    5    10   15        → "đếm số đơn hàng mỗi 5 phút"

HOPPING (size 10, hop 5)  |#########|            cố định, CÓ chồng lấn theo bước nhảy
                               |#########|       → "trung bình trượt 10 phút, cập nhật mỗi 5 phút"

SLIDING                 trượt liên tục; window mới sinh ra khi có event vào/ra
                        → "cảnh báo khi có hơn 3 lỗi trong bất kỳ 60 giây nào"

SESSION                 gom event tới khi im lặng quá ngưỡng timeout
                        |##  ##  #|   (gap)   |### #|
                        → "một phiên hoạt động của người dùng"
```

> **Ranh giới quyết định:** đề nói **"mỗi X phút"** → Tumbling. **"cứ mỗi X phút tính cho khoảng Y phút gần nhất" (Y > X)** → Hopping. **"trong bất kỳ khoảng X nào"** → Sliding. **"phiên / kết thúc khi ngừng hoạt động"** → Session.

---

## 4. BẢNG PHẢN XẠ TỪ KHOÁ → ĐÁP ÁN

Học thuộc bảng này. Khi đọc đề, gạch chân các từ khoá bên trái **trước khi** đọc đáp án.

| Từ khoá trong đề | Phản xạ | Kiểm tra thêm trước khi chốt |
|---|---|---|
| "without copying / avoid data duplication" | **Shortcut** | Nguồn có được hỗ trợ? Security đi theo path nào? |
| "near real-time replica of a database", "minimal ETL" | **Mirroring** | Connector có được hỗ trợ, đã GA chưa? |
| "only new or changed rows" | **Incremental**: watermark hoặc CDC | Có delete signal không? Có commit state sau khi thành công không? |
| "must not process the same row twice", "safe to rerun" | **Idempotent MERGE** (dedup + unique key) | Nhiều source row match 1 target row sẽ lỗi |
| "keep history of changes" | **SCD Type 2** | ValidFrom/ValidTo/IsCurrent; 2 bước expire + insert |
| "overwrite with the latest value" | **SCD Type 1** | `MERGE ... WHEN MATCHED THEN UPDATE` |
| "fact arrives before dimension" | **Late-arriving dimension → unknown member (-1)** | Không để pipeline fail |
| "least privilege", "read-only users must not see other regions" | **Viewer + RLS** | Contributor bypass RLS |
| "minimize administrative effort" | Giải pháp **built-in/SaaS**, tránh custom code | Copy Job > pipeline tự viết; Mirroring > pipeline 5 phút |
| "no-code / citizen developer" | **Dataflow Gen2** hoặc **Eventstream** | Có cần logic phức tạp không? |
| "complex transformation / nested JSON / ML" | **Notebook (Spark)** | Runtime, pool, skew |
| "orchestrate, retry, branching, notify on failure" | **Data Pipeline** | Dependency conditions, retry policy |
| "when a file lands in storage" | **Event-based trigger** | Không phải schedule |
| "every N minutes" | **Schedule** | Không phải event trigger |
| "report is slow, cold cache, Direct Lake" | **V-Order + `OPTIMIZE`**, tránh Overwrite | Guardrail của SKU |
| "Direct Lake bị fallback sang DirectQuery" | Do **RLS/CLS/DDM ở SQL endpoint, view, vượt guardrail** | Direct Lake on OneLake = DirectLakeOnly, không fallback |
| "thousands of small files" | `OPTIMIZE` (+ V-Order nếu đọc bằng Direct Lake) | Không phải `VACUUM` |
| "storage cost keeps growing" | `VACUUM` (giữ ≥ retention) | Đừng đặt `RETAIN 0 HOURS` khi còn reader |
| "one task takes 30 min, 199 tasks take 2s" | **Data skew** → salting / AQE skew join | Không phải scale capacity |
| "everything in the workspace got slow at the same time" | **Capacity throttling** → Capacity Metrics | Không phải tuning một query |
| "notebook activity failed but pipeline error is generic" | Mở **run history của notebook** xem stack trace thật | Kiểm tra tab Input xem parameter đã resolve chưa |
| "telemetry, sensors, logs, last 24h" | **Eventhouse + KQL** | Hot cache vs retention |
| "duplicate events in the stream" | **at-least-once** → dedup: KQL `arg_max()` hoặc Delta MERGE | Eventstream không tự khử trùng |
| "exactly-once khi ghi Delta từ stream" | **checkpoint riêng + `foreachBatch` MERGE idempotent** | Mỗi query một checkpoint |
| "promote from Dev to Prod" | **Deployment pipeline** | Dữ liệu không được deploy |
| "different connection string per environment" | **Variable library** (ưu tiên) hoặc deployment rule | Active value set không bị ghi đè khi deploy |
| "track changes, code review, multiple developers" | **Git integration** | Chỉ metadata, không phải dữ liệu |
| "tại sao user vẫn thấy hết dữ liệu dù đã tạo RLS" | Kiểm tra **workspace role** (Admin/Member/Contributor bypass) | Và access path đang dùng |

---

## 5. QUY TRÌNH 5 BƯỚC — HAI VÍ DỤ ÁP DỤNG

### Ví dụ A

> *Một công ty có Azure SQL Database đang chạy production. Đội phân tích cần dữ liệu trong Fabric với độ trễ dưới vài phút, và yêu cầu **giảm thiểu tối đa công sức vận hành ETL**. Nên dùng gì?*
> A. Pipeline Copy activity chạy mỗi 5 phút  B. Dataflow Gen2 refresh mỗi 5 phút  C. Mirroring  D. OneLake shortcut tới Azure SQL

**Bước 1 — Ô nào?** "Đưa vào" (ingest).
**Bước 2 — Constraint?** "gần realtime" + "giảm thiểu công sức vận hành".
**Bước 3 — Engine?** Không cần Spark, không cần Power Query.
**Bước 4 — Đáp án?** **C. Mirroring** — replicate liên tục, cấu hình một lần.
**Bước 5 — Vì sao 3 đáp án kia sai?**
- A sai: chạy mỗi 5 phút là công sức vận hành cao (watermark, retry, lỗi), vẫn có độ trễ và tạo tải lên DB nguồn.
- B sai: Dataflow Gen2 nặng hơn cho ingest thuần và không phải công cụ replicate.
- D sai: **shortcut không trỏ tới database quan hệ** — shortcut trỏ tới storage (ADLS/S3/GCS/OneLake).

### Ví dụ B

> *Báo cáo Power BI dùng semantic model Direct Lake bỗng chậm hẳn. Điều tra thấy model đang chạy ở chế độ DirectQuery. Bảng nguồn nằm trong Lakehouse, và tuần trước đội bảo mật đã thêm row-level security trên SQL analytics endpoint. Cần làm gì để quay lại Direct Lake?*

**Bước 1 — Ô nào?** "Dùng" + "vận hành/tối ưu".
**Bước 2 — Constraint?** Phải giữ được bảo mật dòng **và** giữ Direct Lake.
**Bước 3 — Engine?** VertiPaq (Direct Lake) so với SQL endpoint.
**Bước 4 — Đáp án?** Bỏ RLS ở tầng SQL endpoint và **triển khai RLS trong chính semantic model bằng DAX** (hoặc chuyển sang mô hình Direct Lake on OneLake + OneLake security). Direct Lake on SQL endpoint **fallback sang DirectQuery khi phát hiện RLS/CLS/DDM ở tầng SQL**.
**Bước 5 — Vì sao đáp án khác sai?** Scale capacity không sửa nguyên nhân; `OPTIMIZE`/V-Order giúp tốc độ nhưng không gỡ fallback; chuyển sang Import làm mất tính cập nhật gần realtime.

> **Bài tập bắt buộc mỗi ngày:** lấy 3 câu bất kỳ trong các file chuyên đề, che đáp án, **nói thành tiếng đủ 5 bước**. Chỉ khi trả lời được bước 5 mới tính là làm đúng.

---

## 6. BỘ DRILL ACTIVE RECALL — 72 CÂU

Cách dùng: mỗi ngày làm **một vòng 12 câu**, **nói đáp án thành tiếng trước khi mở**. Ghi lại câu sai; câu nào sai 2 lần liên tiếp thì quay lại đúng mục trong file chuyên đề.

### Vòng 1 — Nền tảng

<details><summary><b>1.</b> Bốn workspace role và role nào bị OneLake RLS/CLS ràng buộc?</summary>

Admin, Member, Contributor, Viewer. **Chỉ Viewer** (và người được cấp quyền item ở mức đọc) bị RLS/CLS/DDM ràng buộc. Admin/Member/Contributor **bypass**.
</details>

<details><summary><b>2.</b> Starter pool khác Custom pool ở điểm nào quan trọng nhất khi ra đề?</summary>

Starter pool là pool **pre-warmed** → khởi động gần như tức thì, cấu hình cố định (Medium). Custom pool cấu hình được node size/autoscale nhưng **mất vài phút khởi động**. Đề hỏi "dev/test, khởi động nhanh" → Starter; "production cần kiểm soát tài nguyên" → Custom.
</details>

<details><summary><b>3.</b> High concurrency mode chia sẻ session giữa hai user khác nhau được không?</summary>

**Không.** Session sharing nằm trong ranh giới **cùng một user** và các notebook phải tương thích (cùng workspace, cùng cấu hình Spark, cùng default lakehouse, cùng libraries).
</details>

<details><summary><b>4.</b> Git integration đồng bộ cái gì và KHÔNG đồng bộ cái gì?</summary>

Đồng bộ **metadata/definition** của item (notebook, pipeline, lakehouse metadata, warehouse schema…). **Không** đồng bộ **dữ liệu** trong bảng/file. Git và deployment không bao giờ ghi đè dữ liệu.
</details>

<details><summary><b>5.</b> Full load và incremental load khác nhau ở đâu, và incremental cần thêm cái gì để an toàn?</summary>

Full load nạp lại toàn bộ; incremental chỉ nạp dữ liệu mới/thay đổi dựa trên **watermark** hoặc **CDC**. Incremental cần: state (watermark) được **commit sau khi chạy thành công**, dedup nguồn, và MERGE idempotent để retry không sinh trùng.
</details>

<details><summary><b>6.</b> Vì sao `MERGE` không tự động idempotent?</summary>

Vì nếu source có nhiều dòng cùng match một dòng target thì `MERGE` **lỗi**, và nếu điều kiện match không xác định (không unique key) thì chạy lại có thể sinh kết quả khác. Idempotent = dedup source theo business key + match key duy nhất + điều kiện xác định + commit state sau success.
</details>

<details><summary><b>7.</b> SCD Type 1 và Type 2 khác nhau thế nào về mặt bảng đích?</summary>

Type 1: **ghi đè** giá trị cũ, mỗi business key một dòng. Type 2: **giữ lịch sử**, thêm `ValidFrom`/`ValidTo`/`IsCurrent`, mỗi business key có nhiều version — làm 2 bước: expire dòng cũ rồi insert dòng mới.
</details>

<details><summary><b>8.</b> Late-arriving dimension xử lý ra sao?</summary>

Fact tới trước dimension → gán surrogate key **unknown member** (thường `-1`) thay vì để pipeline fail; sau đó khi dimension xuất hiện thì một job cập nhật lại (hoặc dùng inferred member rồi hoàn thiện).
</details>

<details><summary><b>9.</b> Monitoring Hub trả lời câu hỏi gì, Capacity Metrics trả lời câu hỏi gì?</summary>

Monitoring Hub = **operational status**: job nào chạy/fail/fail ở đâu. Capacity Metrics = **resource health**: CU consumption, throttling, timepoint, ai ăn tài nguyên.
</details>

<details><summary><b>10.</b> `OPTIMIZE` và `VACUUM` — cái nào giải quyết small files, cái nào giảm dung lượng?</summary>

`OPTIMIZE` gộp small files (không giảm dung lượng vì file cũ vẫn còn). `VACUUM` xoá file không còn được tham chiếu → giảm dung lượng, mặc định giữ 7 ngày.
</details>

<details><summary><b>11.</b> Ba giai đoạn throttling của Fabric capacity theo thứ tự?</summary>

(1) **Interactive delay** — thêm độ trễ cho interactive; (2) **Interactive rejection** — chặn request interactive mới; (3) **Background rejection** — chặn cả background. Trước đó còn có overage protection cho phép "vay" một khoảng capacity tương lai.
</details>

<details><summary><b>12.</b> Interactive và background operation khác nhau ở smoothing window thế nào?</summary>

**Interactive** (query Power BI, truy vấn ad-hoc) smoothing theo phút (thường tối thiểu ~5 phút). **Background** (pipeline theo lịch, Spark job, scheduled refresh) smoothing trải trên **24 giờ**.
</details>

### Vòng 2 — Quyết định kiến trúc

<details><summary><b>13.</b> Khi nào chọn Lakehouse, khi nào chọn Warehouse?</summary>

Lakehouse: Spark, Delta, semi-structured, medallion, data engineer viết code. Warehouse: cần **T-SQL đầy đủ có ghi** (INSERT/UPDATE/DELETE, transaction đa bảng), star schema, người dùng SQL. SQL endpoint của Lakehouse **chỉ đọc**.
</details>

<details><summary><b>14.</b> Shortcut có hỗ trợ trỏ tới Azure SQL Database không?</summary>

**Không.** Shortcut trỏ tới **storage** (OneLake khác, ADLS Gen2, Amazon S3, Google Cloud Storage, S3-compatible, Dataverse…). Với database quan hệ, dùng **Mirroring** hoặc Copy.
</details>

<details><summary><b>15.</b> Copy Job khác Copy activity trong pipeline ở chỗ nào?</summary>

Copy Job là **item độc lập** cho việc di chuyển dữ liệu, có sẵn full/incremental, CDC hoặc watermark, và các chế độ ghi Append/Merge/Overwrite/SCD2 ngay trong UI. Copy activity là **một bước** trong pipeline, cần bạn tự dựng logic incremental.
</details>

<details><summary><b>16.</b> Dataflow Gen2 hợp và không hợp với việc gì?</summary>

Hợp: transform low-code Power Query, nhiều nguồn nhỏ, người dùng nghiệp vụ. Không hợp: ingest khối lượng rất lớn thuần tuý (dùng Copy), logic phức tạp/ML (dùng Notebook), orchestration nhiều bước (dùng Pipeline).
</details>

<details><summary><b>17.</b> Medallion — mỗi lớp làm gì?</summary>

**Bronze**: dữ liệu thô, giữ nguyên, append. **Silver**: làm sạch, chuẩn hoá, khử trùng, conform. **Gold**: mô hình nghiệp vụ, aggregate, star schema phục vụ báo cáo.
</details>

<details><summary><b>18.</b> Ba cách chạy một pipeline?</summary>

Chạy thủ công (on-demand), theo **schedule**, và theo **event trigger** (ví dụ file đến trong storage/OneLake, hoặc job event qua Real-Time hub/Activator).
</details>

<details><summary><b>19.</b> Bốn dependency condition giữa các activity trong pipeline?</summary>

**Success**, **Failure**, **Completion**, **Skipped**. Muốn "chạy dù bước trước thành công hay thất bại" → Completion.
</details>

<details><summary><b>20.</b> Biểu thức pipeline nào lấy giá trị từ output của activity Lookup?</summary>

`@activity('LookupWatermark').output.firstRow.WatermarkValue`. Các hàm hay dùng khác: `@pipeline().RunId`, `@item()`, `@concat()`, `@coalesce()`, `@if()`, `@utcnow()`.
</details>

<details><summary><b>21.</b> Notebook trả giá trị về pipeline bằng cách nào?</summary>

Bằng **exit value** (`notebookutils.notebook.exit(value)`), pipeline đọc qua output của activity. Lưu ý `getActivityOutput` chỉ lấy được output của activity **thành công**.
</details>

<details><summary><b>22.</b> Deployment pipeline có tối đa bao nhiêu stage và deploy được cái gì?</summary>

Từ **2 đến 10 stage**. Deploy **định nghĩa item**, không deploy dữ liệu thật, không deploy workspace role/connection; một số item (ví dụ mirrored database) có giới hạn — luôn kiểm tra danh sách hiện hành.
</details>

<details><summary><b>23.</b> Variable library dùng để làm gì và vì sao ưu tiên hơn deployment rule?</summary>

Giữ **giá trị cấu hình theo môi trường** (connection, tên DB, đường dẫn) trong một item có version, tích hợp Git và deployment pipeline; mỗi stage chọn một **active value set** riêng, và active value set **không bị ghi đè khi deploy**. Deployment rule chỉ hỗ trợ một số item/rule nhất định.
</details>

<details><summary><b>24.</b> Auto-binding trong deployment pipeline là gì?</summary>

Khi deploy sang stage kế tiếp, item tham chiếu tới item khác **trong cùng deployment pipeline** sẽ tự động trỏ lại vào bản sao ở stage đích (ví dụ notebook trỏ vào lakehouse của stage đó) thay vì vẫn trỏ về Dev.
</details>

### Vòng 3 — Streaming & Real-Time

<details><summary><b>25.</b> Eventstream đảm bảo delivery kiểu gì, hệ quả là gì?</summary>

**At-least-once** → có thể **trùng** event. Phải tự khử trùng: trong Eventhouse dùng KQL `arg_max()` theo event id (hoặc `ingest_if_not_exists`), trong Lakehouse dùng Delta `MERGE` theo event id.
</details>

<details><summary><b>26.</b> Kể 5 nguồn và 4 đích của Eventstream.</summary>

Nguồn: Azure Event Hubs, IoT Hub, Kafka, Google Pub/Sub, Amazon Kinesis, Azure Service Bus, HTTP/custom endpoint, CDC từ Azure SQL/PostgreSQL. Đích: Eventhouse (KQL), Lakehouse (Delta), Warehouse, Activator, custom endpoint, hoặc eventstream khác.
</details>

<details><summary><b>27.</b> Derived stream là gì?</summary>

Một luồng đã được transform trong Eventstream và được **publish lại** vào Real-Time hub để nhiều consumer khác dùng lại, thay vì mỗi consumer tự transform.
</details>

<details><summary><b>28.</b> `checkpointLocation` trong Structured Streaming — quy tắc vàng?</summary>

**Mỗi streaming query một checkpoint riêng biệt.** Dùng chung checkpoint giữa hai query gây hỏng state/kết quả sai. Checkpoint là điều kiện để recovery và đảm bảo không xử lý lại dữ liệu đã commit.
</details>

<details><summary><b>29.</b> `trigger(availableNow=True)` dùng khi nào?</summary>

Khi muốn xử lý **hết dữ liệu hiện có rồi dừng** — chạy stream như một batch. Tiết kiệm CU cho workload không cần chạy 24/7 nhưng vẫn muốn cơ chế checkpoint/incremental của streaming.
</details>

<details><summary><b>30.</b> `withWatermark("ts", "10 minutes")` có tác dụng gì?</summary>

Cho phép engine **giới hạn state** bằng cách bỏ qua event tới muộn hơn 10 phút so với watermark hiện tại; cần thiết cho aggregation theo window và join có state.
</details>

<details><summary><b>31.</b> Bốn loại window và ví dụ tương ứng?</summary>

Tumbling (đếm mỗi 5 phút), Hopping (trung bình 10 phút cập nhật mỗi 5 phút), Sliding (bất kỳ khoảng 60 giây nào), Session (phiên người dùng, kết thúc khi im lặng quá timeout).
</details>

<details><summary><b>32.</b> KQL update policy dùng làm gì và có giới hạn nào?</summary>

Tự động biến đổi dữ liệu từ bảng nguồn sang bảng đích **ngay khi ingest**. Giới hạn quan trọng: nếu bảng nguồn có **streaming ingestion policy**, update policy **không được join** sang bảng khác — phải dùng bảng trung gian hoặc scheduled query.
</details>

<details><summary><b>33.</b> Native table trong Eventhouse khác OneLake shortcut ở điểm nào?</summary>

Native table: dữ liệu nằm trong Eventhouse, được index + hot cache → query nhanh nhất. Shortcut: dữ liệu vẫn nằm ở OneLake/Delta, không copy, nhưng query chậm hơn — trừ khi bật **query acceleration**.
</details>

<details><summary><b>34.</b> Query acceleration cho shortcut giải quyết gì?</summary>

Tạo lớp cache/index cho dữ liệu shortcut để query KQL nhanh gần bằng native table, đổi lấy chi phí tài nguyên. Chọn khi cần zero-copy **nhưng** vẫn cần tốc độ truy vấn cao.
</details>

<details><summary><b>35.</b> Caching policy và retention policy trong KQL khác nhau ra sao?</summary>

**Caching (hot cache)**: bao nhiêu dữ liệu gần đây được giữ trên SSD để query nhanh. **Retention**: dữ liệu được giữ tổng cộng bao lâu trước khi bị xoá. Hot cache ⊆ retention. Query chậm do dữ liệu ngoài hot cache → tăng caching policy, không phải tăng retention.
</details>

<details><summary><b>36.</b> Khi nào chọn Eventstream, khi nào chọn Spark Structured Streaming?</summary>

Eventstream khi cần no-code, route nhiều đích, transform nhẹ. Structured Streaming khi cần logic phức tạp, join với bảng dimension, MERGE idempotent, hoặc tích hợp ML.
</details>

### Vòng 4 — Tối ưu & xử lý lỗi

<details><summary><b>37.</b> Triệu chứng "một task chạy 30 phút, 199 task chạy 2 giây" là gì và xử lý sao?</summary>

**Data skew**. Xử lý: salting key, bật AQE skew join (`spark.sql.adaptive.skewJoin.enabled`), repartition, hoặc broadcast bảng nhỏ. Không phải vấn đề capacity.
</details>

<details><summary><b>38.</b> Spill trong Spark là gì, nhìn ở đâu?</summary>

Khi shuffle/aggregation không đủ RAM, dữ liệu bị đẩy xuống disk (memory spill/disk spill) → chậm. Nhìn ở **Spark UI → Stages** (cột spill) hoặc Executors. Xử lý: giảm dữ liệu shuffle, tăng partition hợp lý, tránh cột thừa, tăng node size khi cần.
</details>

<details><summary><b>39.</b> V-Order là gì, đánh đổi ra sao, và mặc định trong workspace mới?</summary>

Tối ưu **lúc ghi** (sorting/encoding/nén thân thiện VertiPaq) → đọc nhanh hơn, đặc biệt cho Direct Lake. Đánh đổi: ghi chậm hơn khoảng 15%. Trong **workspace mới, V-Order tắt mặc định** — bật bằng `spark.sql.parquet.vorder.default=true` hoặc `OPTIMIZE ... VORDER`.
</details>

<details><summary><b>40.</b> Direct Lake: transcoding và framing là gì?</summary>

**Transcoding**: nạp dữ liệu theo cột từ OneLake vào cache VertiPaq **khi query chạm cột đó lần đầu**. **Framing**: thao tác lúc refresh, đọc metadata Delta để chốt tập file Parquet mà model nhìn thấy — query chỉ thấy dữ liệu tại thời điểm framing gần nhất.
</details>

<details><summary><b>41.</b> Ba nguyên nhân phổ biến khiến Direct Lake fallback sang DirectQuery?</summary>

(1) Có **RLS/CLS/DDM ở tầng SQL analytics endpoint**; (2) model trỏ vào **view** thay vì bảng Delta; (3) bảng **vượt guardrail** của SKU (số file parquet, row group, số dòng). Direct Lake **on OneLake** chạy DirectLakeOnly nên không fallback — nó báo lỗi.
</details>

<details><summary><b>42.</b> Vì sao nên tránh `Overwrite` toàn bảng với model Direct Lake?</summary>

Overwrite tạo lại Delta log/file → toàn bộ cột đang resident bị **evict**, model quay về trạng thái cold, query đầu tiên chậm. Ưu tiên append/incremental/MERGE.
</details>

<details><summary><b>43.</b> Warehouse: khi nào cần can thiệp statistics?</summary>

Warehouse tự tạo/cập nhật statistics, nhưng sau **bulk load rất lớn** hoặc dữ liệu đổi đột ngột, plan có thể lệch → chạy `UPDATE STATISTICS`/`CREATE STATISTICS` thủ công cho cột hay filter/join.
</details>

<details><summary><b>44.</b> Kể tên các view Query Insights của Warehouse.</summary>

`queryinsights.exec_requests_history`, `queryinsights.frequently_run_queries`, `queryinsights.long_running_queries`. Ngoài ra có DMV `sys.dm_exec_requests`, `sys.dm_exec_sessions`.
</details>

<details><summary><b>45.</b> Vì sao `VARCHAR(MAX)` bừa bãi lại hại trong Warehouse?</summary>

Engine cấp phát bộ nhớ theo **kiểu khai báo**, không theo dữ liệu thực → lãng phí RAM, dễ spill xuống disk, query chậm. Khai báo độ dài sát thực tế.
</details>

<details><summary><b>46.</b> Pipeline lỗi nhưng thông báo rất chung chung — quy trình debug đúng?</summary>

(1) Mở run detail, xem **activity nào** fail; (2) xem tab **Input** để kiểm tra parameter đã resolve đúng chưa; (3) với Notebook/Dataflow, mở **run history của chính item đó** để lấy stack trace thật; (4) nếu nhiều job cùng lỗi/chậm, kiểm tra **Capacity Metrics** (throttling); (5) rerun từ activity lỗi thay vì chạy lại cả pipeline.
</details>

<details><summary><b>47.</b> Khi nào nên retry và khi nào retry là vô ích?</summary>

Retry hợp lý với lỗi **transient** (timeout mạng, throttling tạm thời). Vô ích với lỗi **cấu hình/credential/schema** — retry chỉ tốn CU và làm chậm phát hiện lỗi.
</details>

<details><summary><b>48.</b> Shortcut báo lỗi truy cập — kiểm tra theo thứ tự nào?</summary>

(1) Credential/identity dùng để tạo shortcut còn hợp lệ không; (2) quyền trên **nguồn** (S3/ADLS) đã cấp chưa; (3) đường dẫn/bucket có đúng không; (4) quyền của user **trong Fabric** trên item chứa shortcut; (5) OneLake security/cache có ảnh hưởng không.
</details>

### Vòng 5 — Bảo mật, quản trị, vận hành

<details><summary><b>49.</b> ReadData và ReadAll khác nhau ra sao?</summary>

**ReadData**: đọc bảng qua **SQL analytics endpoint**. **ReadAll**: đọc trực tiếp file/bảng qua **OneLake API/Spark/Lakehouse explorer**. Cấp ReadData không tự động cho phép đọc file bằng Spark.
</details>

<details><summary><b>50.</b> OneLake security roles kết hợp OLS/RLS/CLS theo nguyên tắc nào?</summary>

Trong **một role**, quyền là **giao** của phạm vi bảng/folder (OLS) ∩ điều kiện dòng (RLS) ∩ cột được phép (CLS). Khi user thuộc **nhiều role**, kết quả là **most permissive** (hợp các role).
</details>

<details><summary><b>51.</b> SQL analytics endpoint có hai chế độ bảo mật nào?</summary>

**User identity mode** (áp dụng OneLake security roles của người dùng) và **delegated/owner identity mode** (áp dụng quyền T-SQL: GRANT/REVOKE, RLS/CLS/DDM). Chuyển mode làm endpoint tạm thời không khả dụng → làm trong maintenance window.
</details>

<details><summary><b>52.</b> Dynamic Data Masking có phải ranh giới bảo mật đầy đủ không?</summary>

**Không.** DDM chỉ che hiển thị ở tầng SQL; user có quyền query vẫn có thể suy ra dữ liệu bằng cách lọc/so sánh. Phải kết hợp với phân quyền thật (CLS/RLS/OneLake security).
</details>

<details><summary><b>53.</b> Sensitivity label và endorsement khác nhau thế nào?</summary>

**Sensitivity label** (Purview) = phân loại + bảo vệ, có thể kế thừa xuống downstream item. **Endorsement** (Promoted/Certified) = tín hiệu **chất lượng/độ tin cậy** để người dùng tìm nội dung đúng — **không phải cơ chế bảo mật**.
</details>

<details><summary><b>54.</b> Domain trong Fabric để làm gì?</summary>

Nhóm các workspace theo lĩnh vực nghiệp vụ (Sales, Finance…) để quản trị phân tán: gán domain admin/contributor, đặt default sensitivity label, uỷ quyền một số tenant setting xuống domain.
</details>

<details><summary><b>55.</b> Workspace Monitoring cho gì mà Capacity Metrics không cho?</summary>

Log chi tiết cấp workspace (query text, duration, lỗi) nằm trong một **Eventhouse/KQL database**, truy vấn được bằng KQL/SQL. Đổi lại, metric ở đây **không** được smoothing/bursting như Capacity Metrics, và không bật cùng lúc với Log Analytics.
</details>

<details><summary><b>56.</b> Alert khi pipeline fail — có những cách nào?</summary>

(1) **Failure notification** built-in của pipeline (email/Teams); (2) activity Office 365 Outlook/Teams trong nhánh Failure; (3) **Activator** trên job events; (4) KQL trên Workspace Monitoring để cảnh báo toàn workspace.
</details>

<details><summary><b>57.</b> Semantic model Import refresh và Direct Lake "refresh" khác nhau thế nào?</summary>

Import: nạp toàn bộ/incremental dữ liệu vào model, xem ở **refresh history**. Direct Lake: "refresh" thực chất là **framing** — cập nhật metadata trỏ tới file Delta mới nhất, rất nhẹ, không copy dữ liệu.
</details>

<details><summary><b>58.</b> Bursting và smoothing khác nhau ra sao?</summary>

**Bursting**: cho phép một job dùng nhiều CU hơn baseline trong thời gian ngắn để chạy nhanh. **Smoothing**: trải phần CU đã tiêu thụ ra theo thời gian (interactive vài phút, background 24 giờ) để tránh throttle đột ngột.
</details>

<details><summary><b>59.</b> Khi capacity đạt 100% thì làm gì?</summary>

Xem Capacity Metrics để tìm operation ngốn CU; tạm dừng job không thiết yếu; **scale-up SKU** (hiệu lực gần như tức thì); chờ overage cháy hết; bật surge protection để chặn background chiếm hết capacity.
</details>

<details><summary><b>60.</b> Vì sao không nên `VACUUM ... RETAIN 0 HOURS` trên bảng đang có reader?</summary>

Xoá ngay file cũ có thể khiến query đang chạy (hoặc time travel) bị **file not found**. Giữ retention mặc định 7 ngày (168 giờ) trừ khi có lý do rõ ràng.
</details>

### Vòng 6 — Câu hỏi "vì sao đáp án kia sai"

<details><summary><b>61.</b> Đề yêu cầu zero-copy từ S3 — vì sao Copy Job sai?</summary>

Copy Job **sao chép** dữ liệu vào OneLake → vi phạm ràng buộc zero-copy và tạo bản trùng phải đồng bộ. Shortcut mới đúng.
</details>

<details><summary><b>62.</b> Đề yêu cầu "user chỉ được xem dữ liệu vùng của mình" — vì sao cấp Contributor + RLS là sai?</summary>

Contributor **bypass** RLS/CLS → user vẫn thấy hết. Đúng: cấp **Viewer** (hoặc item read) rồi áp RLS/OneLake security.
</details>

<details><summary><b>63.</b> Report Direct Lake chậm — vì sao "scale lên SKU lớn hơn" thường không phải đáp án?</summary>

Vì nguyên nhân thường là **fallback DirectQuery**, thiếu V-Order, small files hoặc Overwrite làm cold cache. Scale chỉ che triệu chứng và tốn tiền; đề DP-700 luôn ưu tiên đáp án **khớp nguyên nhân**.
</details>

<details><summary><b>64.</b> Pipeline chậm — vì sao tăng `parallelCopies` lên tối đa không phải lúc nào cũng đúng?</summary>

Throughput ≈ min(source, network, destination, copy compute). Nếu **source hoặc sink đã bão hoà/throttle**, tăng song song chỉ gây lỗi và tốn CU.
</details>

<details><summary><b>65.</b> Cần cấu hình khác nhau cho Dev/Test/Prod — vì sao "sửa tay sau mỗi lần deploy" sai?</summary>

Không lặp lại được, dễ sai, không audit được. Đúng: **Variable library** (hoặc deployment rule ở nơi được hỗ trợ) để giá trị tự áp theo stage.
</details>

<details><summary><b>66.</b> Stream có event trùng — vì sao "bật exactly-once trên Eventstream" là đáp án bẫy?</summary>

Eventstream đảm bảo **at-least-once**; không có công tắc exactly-once. Phải khử trùng ở đích: `arg_max()`/`ingest_if_not_exists` trong KQL, hoặc Delta `MERGE` theo event id.
</details>

<details><summary><b>67.</b> Notebook fail trong pipeline — vì sao "đọc error message của pipeline" là chưa đủ?</summary>

Pipeline chỉ báo lỗi chung. Stack trace thật nằm trong **run history của notebook**; phải mở đúng đó mới biết nguyên nhân (thư viện thiếu, OOM, path sai…).
</details>

<details><summary><b>68.</b> Bảng Delta phình dung lượng — vì sao `OPTIMIZE` không giải quyết?</summary>

`OPTIMIZE` **tạo thêm** file mới đã gộp, file cũ vẫn được giữ cho time travel → dung lượng còn tăng. Muốn thu hồi phải `VACUUM`.
</details>

<details><summary><b>69.</b> Muốn version control cho warehouse schema — vì sao "dùng deployment pipeline" chưa đủ?</summary>

Deployment pipeline promote item, không cho **lịch sử thay đổi và code review**. Cần **Git integration** và/hoặc **database project (SQL project)** để quản lý schema dạng code.
</details>

<details><summary><b>70.</b> Query KQL chậm với dữ liệu 90 ngày — vì sao tăng retention là sai?</summary>

Retention chỉ quyết định **giữ bao lâu**, không quyết định tốc độ. Tốc độ phụ thuộc **caching policy (hot cache)** — muốn nhanh cho 90 ngày thì tăng hot cache (đổi lại tốn tài nguyên).
</details>

<details><summary><b>71.</b> Muốn chạy notebook nhưng user là Viewer — cách đúng?</summary>

Viewer không chạy được notebook. Hoặc nâng role phù hợp, hoặc cấp quyền **Execute** trên notebook đó, hoặc đổi access path (ví dụ đọc qua SQL endpoint).
</details>

<details><summary><b>72.</b> Nhiều workload cùng chậm sau 9h sáng mỗi ngày — chẩn đoán đầu tiên là gì?</summary>

**Capacity throttling / smoothing** do trùng giờ cao điểm → mở Capacity Metrics, xem timepoint và background vs interactive, dời lịch job nặng hoặc scale/surge protection. Không đi tuning từng query trước.
</details>

---

## 7. NHỮNG NHẦM LẪN PHỔ BIẾN CẦN ĐÍNH CHÍNH

Các khẳng định dưới đây xuất hiện rất nhiều trong tài liệu ôn thi và video trên mạng, nhưng **không đúng với Fabric hiện hành**. Học nhầm những điểm này sẽ dẫn tới chọn sai đáp án:

| Khẳng định thường gặp (SAI hoặc thiếu chính xác) | Điều chỉnh đúng |
|---|---|
| "V-Order mặc định luôn bật trong Fabric" | Với **workspace mới**, V-Order **tắt mặc định** cho Spark. Bật bằng `spark.sql.parquet.vorder.default=true`, `option("parquet.vorder.enabled","true")`, hoặc `OPTIMIZE ... VORDER`. |
| "Direct Lake không hỗ trợ View" | Chính xác hơn: model Direct Lake **on SQL endpoint** khi gặp view (hoặc RLS/CLS/DDM ở tầng SQL, hoặc vượt guardrail) sẽ **fallback sang DirectQuery**. Direct Lake **on OneLake** chạy DirectLakeOnly nên **báo lỗi thay vì fallback**. |
| "Trọng số ba domain là 35–40 / 30–35 / 25–30%" | Đó là blueprint **cũ**. Theo bản **21/07/2026** và study guide hiện hành (30/10/2026), cả ba domain đều **30–35%**. Monitor & Optimize nay nặng ngang hai domain kia. |
| "Fabric Warehouse dùng `CREATE MATERIALIZED VIEW`" | Warehouse **không** hỗ trợ materialized view. Materialized **Lake** View là tính năng của **Lakehouse/Spark**. |
| "Spark Autotune nên bật cho mọi workload" | Autotune (query tuning) là **Preview, tắt mặc định**, và chỉ tương thích **Runtime 1.2** (đã hết hỗ trợ) — **không bật được trên Runtime 1.3/2.0**; không chạy khi bật high concurrency. |
| "Runtime 2.0 còn Preview, production nên ở 1.3" | Từ 10/2026, **Runtime 2.0 là GA và mặc định**, Microsoft khuyến nghị cho production; **Runtime 1.3 là EOSA**. Chỉ tránh bật feature Delta 4.2 experimental trên table dùng chung nhiều engine. |
| "Chỉ Admin được đổi Git branch" | Admin luôn được; **Contributor/Member** được khi admin bật *Allow users with at least Contributor role to change Git branch* và user có write trên mọi item. |
| "Spark đọc Event Hubs bằng `format('eventhubs')`" | Trên **Runtime 2.0** connector này đã bị gỡ → dùng **Kafka endpoint** (`format('kafka')`, port 9093). |
| "Interactive smoothing là đúng 5 phút" / "đúng 10 phút" | Đừng học một con số cứng. Học **cơ chế**: interactive smoothing theo phút, background trải **24 giờ**, throttling tiến triển delay → interactive rejection → background rejection. |
| "OneLake shortcut trỏ được tới Azure SQL Database" | Shortcut trỏ tới **storage** (OneLake, ADLS Gen2, S3, GCS, S3-compatible, Dataverse). Với database quan hệ dùng **Mirroring**. |
| "`MERGE` luôn idempotent" | Không. Cần dedup source theo business key, match key duy nhất, điều kiện xác định và commit state sau khi thành công. |
| "Cấp Contributor rồi bật RLS là đủ để giới hạn dữ liệu" | Admin/Member/Contributor **bypass** OneLake RLS/CLS. Phải dùng **Viewer** hoặc item-level read. |
| "Eventstream có chế độ exactly-once" | Eventstream là **at-least-once**. Khử trùng ở đích bằng `arg_max()` (KQL) hoặc Delta `MERGE`. |

---

## 8. LỊCH 6 TUẦN — MỖI TUẦN CHẠM CẢ BA DOMAIN

Nguyên tắc số một: **không ôn một domain đơn lẻ trong suốt một tuần.** Kiến thức không được chạm lại sẽ nguội, và bài thi đòi hỏi cả ba domain cùng vững tại một thời điểm.

| Tuần | Trọng tâm mới (60%) | Ôn quay vòng chống nguội (25%) | Drill (15%) |
|:---:|---|---|---|
| **1** | Workspace Settings + Lifecycle Management (D1) | Loading patterns (D2), Monitoring Hub vs Capacity Metrics (D3) | Vòng 1–2 |
| **2** | Loading Patterns + Batch Data (D2) | Git/deployment pipeline (D1), `OPTIMIZE`/`VACUUM` (D3) | Vòng 2–3 |
| **3** | Optimize Performance + Monitor Items (D3) | Workspace roles (D1), MERGE/SCD2 (D2) | Vòng 4 |
| **4** | Streaming Data (D2) + Identify & Resolve Errors (D3) | Security & governance (D1) | Vòng 3–5 |
| **5** | Security & Orchestrate (D1) + Practice Assessment lượt 1 | Bịt lỗ hổng theo kết quả practice | Vòng 5–6 |
| **6** | Chỉ sửa lỗ hổng + Practice Assessment lượt 2, 3 | Cả 3 domain mỗi ngày 1 giờ | Vòng 1–6 xoay vòng |

**Quy tắc mỗi ngày:** chạm cả ba domain. Ví dụ một ngày tuần 2:

```text
Sáng   25 phút: đọc + gõ lại code incremental watermark (D2, trọng tâm)
Trưa   20 phút: drill 12 câu recall (hỗn hợp cả 3 domain)
Tối    15 phút: mở Capacity Metrics/Monitoring Hub trong tenant, tự hỏi 3 câu (D3)
       + 10 phút: nhắc lại ma trận workspace role (D1)
```

---

## 9. READINESS GATE & CHIẾN THUẬT PHÒNG THI

### 9.1 Tám điều kiện sẵn sàng

- [ ] Practice Assessment **bản mới trên AI Skills Navigator** ([aka.ms/DP700-PA](http://aka.ms/DP700-PA)) đạt **≥85% ở CẢ BA domain**, **hai lượt liên tiếp** (không phải 85% tổng).
- [ ] Trả lời đúng **≥65/72** câu drill ở mục 6 **và ≥11/12** câu Vòng 7 ở mục U6 khi che đáp án, ở hai ngày khác nhau.
- [ ] Giải thích được 49 bullet của study guide (và các related topic ở U2) bằng lời của mình, không đọc nguyên văn tài liệu.
- [ ] Viết từ trí nhớ (không copy): một ví dụ end-to-end **T-SQL**, một **PySpark**, một **KQL**.
- [ ] Tự làm xong lab: watermark incremental, MERGE idempotent, SCD2, Git + deployment pipeline, Eventstream → Eventhouse/Lakehouse, RLS test bằng Viewer, `OPTIMIZE`/`VACUUM`, đọc Capacity Metrics.
- [ ] Đọc vanh vách 8 cụm nhầm lẫn ở mục 3 và nêu được **điều kiện loại trừ** của từng cặp.
- [ ] Với mỗi triệu chứng performance, chỉ ra **công cụ chẩn đoán trước** rồi mới đề xuất biện pháp.
- [ ] Đã đọc lại live Study Guide + What's new in Fabric trong 7 ngày trước ngày thi.

> **Thông tin chính sách cần biết:** không đạt lần đầu phải chờ **24 giờ**; từ lần thứ hai trở đi phải chờ **14 ngày** giữa các lần; tối đa **5 lần trong 12 tháng** tính từ lần thi đầu tiên. Mỗi lần đăng ký lại đều mất phí, trừ khi có voucher.

### 9.2 Trong phòng thi

1. **Đọc constraint trước, requirement sau.** Constraint ("không copy dữ liệu", "least privilege", "minimize administrative effort", "near real-time") loại 2/4 đáp án nhanh nhất.
2. **Case study: đọc câu hỏi trước, rồi mới nhảy vào tab Requirements** để tìm đúng đoạn liên quan. Giới hạn khoảng 15 phút mỗi case.
3. **Không quá 2 phút cho một câu.** Chọn đáp án khả dĩ nhất, Mark for Review, đi tiếp.
4. **Câu Yes/No dạng chuỗi không quay lại được** — cân nhắc kỹ trước khi bấm Next.
5. **Microsoft Learn split-screen chỉ dùng cho 5–7 câu.** Tra bằng từ khoá kỹ thuật (`fabric git integration supported items`, `fabric capacity smoothing`, `delta change data feed`), không tra nguyên văn câu hỏi.
6. **Khi phân vân giữa hai đáp án đều chạy được:** chọn cái ít vận hành hơn, ít quyền hơn, dùng tính năng built-in của Fabric.
7. **Không bỏ trống câu nào** — trả lời sai không bị trừ điểm.

---

## 10. ĐƯỜNG DẪN NHANH TỚI MƯỜI FILE CHUYÊN ĐỀ

| # | File | Nội dung | Mức khó |
|:---:|---|---|:---:|
| 1 | [Workspace Settings](DP700_Domain1_Implement_Manage_01_Workspace_Settings.md) | Spark/domain/OneLake/Airflow settings, workspace roles | ★★★ |
| 2 | [Lifecycle Management](DP700_Domain1_Implement_Manage_02_Lifecycle_Management.md) | Git, database project, deployment pipeline, variable library | ★★★ |
| 3 | [Security and Governance](DP700_Domain1_Implement_Manage_03_Security_and_Governance.md) | Role, item permission, OneLake security, RLS/CLS/DDM, label | ★★ |
| 4 | [Orchestrate Processes](DP700_Domain1_Implement_Manage_04_Orchestrate_Processes.md) | Chọn công cụ, schedule/event trigger, pipeline pattern | ★ |
| 5 | [Loading Patterns](DP700_Domain2_Ingest_Transform_01_Design_Loading_Patterns.md) | Full/incremental, watermark, CDC, SCD, dimensional model | ★★★ |
| 6 | [Batch Data](DP700_Domain2_Ingest_Transform_02_Batch_Data.md) | Data store, shortcut, mirroring, pipeline, PySpark/SQL/KQL | ★ |
| 7 | [Streaming Data](DP700_Domain2_Ingest_Transform_03_Streaming_Data.md) | Eventstream, Structured Streaming, KQL, windowing | ★★ |
| 8 | [Monitor Items and Alerts](DP700_Domain3_Monitor_Optimize_01_Monitor_Items_Alerts.md) | Monitoring Hub, Capacity Metrics, Workspace Monitoring, alert | ★★ |
| 9 | [Identify and Resolve Errors](DP700_Domain3_Monitor_Optimize_02_Identify_Resolve_Errors.md) | Debug pipeline/dataflow/notebook/eventhouse/T-SQL/shortcut | ★★ |
| 10 | [Optimize Performance](DP700_Domain3_Monitor_Optimize_03_Optimize_Performance.md) | Delta, pipeline, warehouse, eventhouse, Spark, query | ★★★ |

> Mỗi file trọng tâm đều được bổ sung một chương cuối tên **"HỌC LẠI TỪ GỐC"** — đó là nơi bắt đầu nếu chương chính quá dày.
