# DP-700 — Bắt đầu từ đây

> **Bản cập nhật ngày 05/11/2026** (bản trước: 09/08/2026). Blueprint dùng trong bộ tài liệu là [DP-700 Study Guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700) — trang được Microsoft cập nhật **30/10/2026**, hiện **49 bullet**, ba domain đều **30–35%**. Trang không còn ghi "Skills measured as of" và không có change log; các thay đổi so với bản 21/07/2026 được tóm tắt ở đầu file [MASTER](Document_Guide/DP700_00_MASTER_Trong_Tam_On_Thi.md) (mục ⚡ CẬP NHẬT 05/11/2026). Trước ngày thi, luôn kiểm tra lại Study Guide vì Microsoft Fabric thay đổi nhanh.

## Kết luận đánh giá

**Bộ tài liệu hiện phủ đủ 49/49 bullet của study guide hiện hành, cộng các related topic của bản 21/07/2026 (audit logs, OneLake security, native tables vs shortcuts, query acceleration, shortcut errors)**, có ví dụ SQL, PySpark, KQL, pipeline expressions, câu hỏi scenario, lab và hyperlink tới tài liệu chính thức.

Tuy nhiên, câu trả lời trung thực cho câu hỏi “đã đủ để trả lời mọi câu hỏi chưa?” là **chưa thể bảo đảm chỉ bằng việc đọc tài liệu**. Microsoft ghi rõ các bullet trong Study Guide chỉ minh họa cách đánh giá; đề có thể hỏi các chủ đề liên quan. Phần lớn câu hỏi dùng tính năng GA, nhưng Preview feature phổ biến vẫn có thể xuất hiện. Vì vậy readiness phải gồm cả:

1. Hiểu và giải thích được khái niệm bằng lời của mình.
2. Chọn đúng Fabric item/engine theo scenario và constraint.
3. Tự viết/chạy được code cốt lõi bằng SQL, PySpark và KQL.
4. Tự làm lab trong tenant Fabric, không chỉ đọc đáp án.
5. Làm [Microsoft Practice Assessment](https://learn.microsoft.com/en-us/credentials/certifications/fabric-data-engineer-associate/) và xem [Exam Sandbox](https://aka.ms/examdemo).

## Ma trận bao phủ study guide 49/49 (cập nhật 05/11/2026)

| Domain | Functional group | Số objective | Tài liệu chính | Coverage |
|---|---|---:|---|:---:|
| Implement and manage | Configure workspace settings (Spark, domain, OneLake, **data workflow**) | 4 | [Workspace Settings](Document_Guide/DP700_Domain1_Implement_Manage_01_Workspace_Settings.md) | 4/4 |
| Implement and manage | Implement lifecycle management | 3 | [Lifecycle Management](Document_Guide/DP700_Domain1_Implement_Manage_02_Lifecycle_Management.md) | 3/3 |
| Implement and manage | Configure security and governance | 6 (+2 related) | [Security and Governance](Document_Guide/DP700_Domain1_Implement_Manage_03_Security_and_Governance.md) | 6/6 |
| Implement and manage | Orchestrate processes | 3 | [Orchestrate Processes](Document_Guide/DP700_Domain1_Implement_Manage_04_Orchestrate_Processes.md) | 3/3 |
| Ingest and transform | Design and implement loading patterns | 3 | [Loading Patterns](Document_Guide/DP700_Domain2_Ingest_Transform_01_Design_Loading_Patterns.md) | 3/3 |
| Ingest and transform | Ingest and transform batch data | 9 | [Batch Data](Document_Guide/DP700_Domain2_Ingest_Transform_02_Batch_Data.md) | 9/9 |
| Ingest and transform | Ingest and transform streaming data | 5 (+2 related) | [Streaming Data](Document_Guide/DP700_Domain2_Ingest_Transform_03_Streaming_Data.md) | 5/5 |
| Monitor and optimize | Monitor Fabric items | 4 | [Monitor Items and Alerts](Document_Guide/DP700_Domain3_Monitor_Optimize_01_Monitor_Items_Alerts.md) | 4/4 |
| Monitor and optimize | Identify and resolve errors | 6 (+1 related) | [Identify and Resolve Errors](Document_Guide/DP700_Domain3_Monitor_Optimize_02_Identify_Resolve_Errors.md) | 6/6 |
| Monitor and optimize | Optimize performance | 6 | [Optimize Performance](Document_Guide/DP700_Domain3_Monitor_Optimize_03_Optimize_Performance.md) | 6/6 |
| **Tổng** | **10 functional groups** | **49** (+5 related) | **10 file chuyên đề** | **49/49** |

> **Coverage 49/49 không có nghĩa xác suất đậu 100%.** Coverage đo sự hiện diện của kiến thức; readiness đo khả năng áp dụng dưới dạng case study, multiple choice, build list, drag-and-drop và câu hỏi có nhiều đáp án gần đúng.

## Thông tin kỳ thi cần biết

- Ba domain đều có trọng số **30–35%**; không có domain nào nên bỏ.
- Điểm đậu theo Microsoft là **700/1000**; đây là scaled score, không nên quy đổi máy móc thành “70% câu đúng”.
- Trang chứng chỉ hiện ghi thời gian assessment là **100 phút**.
- Tính đến ngày đối chiếu, kỳ thi không liệt kê tiếng Việt. Microsoft nêu rằng nếu kỳ thi không có ngôn ngữ ưu tiên, thí sinh có thể yêu cầu thêm 30 phút; hãy xác minh lúc đăng ký.
- Nguồn chính: [Certification page](https://learn.microsoft.com/en-us/credentials/certifications/fabric-data-engineer-associate/), [Study Guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700), [Course DP-700T00-A](https://learn.microsoft.com/en-us/training/courses/dp-700t00).

## Bắt đầu ở đâu: file tổng chỉ huy

> **Cập nhật 06/09/2026.** Bộ tài liệu có thêm một file đứng đầu: **[Trọng tâm ôn thi & phản xạ quyết định](Document_Guide/DP700_00_MASTER_Trong_Tam_On_Thi.md)**.

File đó là nơi đọc mỗi ngày, gồm: bản đồ Fabric trong một trang, **8 cụm nhầm lẫn giết điểm**, bảng phản xạ từ khoá → đáp án, quy trình 5 bước giải câu hỏi scenario, **72 câu active recall** có che đáp án, bảng đính chính các nhầm lẫn phổ biến, và lịch 6 tuần chạm cả ba domain mỗi tuần.

Ngoài ra, **cả 10 file chuyên đề đều được bổ sung một chương cuối tên “HỌC LẠI TỪ GỐC”** gồm: bản chất trong 60 giây (kèm sơ đồ/ví von), cây quyết định, bảng phản xạ, bộ câu hỏi active recall, và mini-lab. Nếu chương chính thấy quá dày, hãy bắt đầu từ chương đó rồi mới đọc ngược lên.

## Cách học cho người mới bắt đầu

### Vòng 1 — Xây mental model

Đọc theo thứ tự sau và chưa cần học thuộc cú pháp:

0. [**Trọng tâm ôn thi & phản xạ quyết định**](Document_Guide/DP700_00_MASTER_Trong_Tam_On_Thi.md) — đọc mục 2 (bản đồ Fabric) và mục 3 (8 cụm nhầm lẫn) trước tiên
1. [Workspace Settings](Document_Guide/DP700_Domain1_Implement_Manage_01_Workspace_Settings.md)
2. [Loading Patterns](Document_Guide/DP700_Domain2_Ingest_Transform_01_Design_Loading_Patterns.md)
3. [Batch Data](Document_Guide/DP700_Domain2_Ingest_Transform_02_Batch_Data.md)
4. [Streaming Data](Document_Guide/DP700_Domain2_Ingest_Transform_03_Streaming_Data.md)
5. [Security and Governance](Document_Guide/DP700_Domain1_Implement_Manage_03_Security_and_Governance.md)
6. [Lifecycle Management](Document_Guide/DP700_Domain1_Implement_Manage_02_Lifecycle_Management.md)
7. [Orchestrate Processes](Document_Guide/DP700_Domain1_Implement_Manage_04_Orchestrate_Processes.md)
8. Ba file Monitor → Troubleshoot → Optimize.

Sau vòng này, bạn phải giải thích được luồng tổng quát:

```text
Source
  → chọn copy / shortcut / mirroring / event ingestion
  → chọn Lakehouse / Warehouse / Eventhouse
  → transform bằng Dataflow Gen2 / Notebook / T-SQL / KQL
  → orchestrate bằng Pipeline / schedule / event trigger
  → bảo mật ở workspace / item / OneLake / SQL / semantic model
  → monitor execution + capacity
  → đo bottleneck rồi mới optimize
```

### Vòng 2 — Viết code và làm lab

Không copy/paste ở lượt đầu. Tự gõ lại, chạy, tạo lỗi có chủ đích rồi sửa:

- Full load, incremental watermark, dedup và retry-safe Delta `MERGE`.
- SCD Type 1 và Type 2; late-arriving dimension với unknown member.
- PySpark `Window`, `groupBy`, `join`, missing values, write Delta.
- Spark Structured Streaming: `readStream`, watermark, window, unique checkpoint, `foreachBatch`.
- T-SQL: DDL/DML, CTAS, `COPY INTO`, `MERGE`, stored procedure, RLS, CLS, DDM, `GRANT`/`DENY`/`REVOKE`.
- KQL: `where`, `project`, `extend`, `summarize`, `bin`, `join`, `make-series`, ingestion/caching/retention policies.
- Pipeline parameters, `@pipeline()`, `@item()`, `@activity()`, `concat`, `if`, `coalesce`, `ForEach`, dependency conditions.
- Monitoring Hub, Capacity Metrics, Workspace Monitoring, Query Insights và alert.

### Vòng 3 — Luyện scenario

Che phần đáp án trong mỗi file. Với mỗi câu, nói thành tiếng theo mẫu:

```text
Requirement chính là gì?
Constraint nào loại đáp án?
Engine/access path nào thực thi?
Đáp án nào đáp ứng đủ với ít vận hành và least privilege nhất?
Vì sao ba đáp án còn lại sai?
```

## 15 phản xạ chọn công cụ phải thuộc

| Từ khóa trong đề | Phản xạ đầu tiên | Kiểm tra thêm trước khi chốt |
|---|---|---|
| Không copy dữ liệu, multi-cloud | OneLake shortcut | Source type, credential, cache, security path |
| Replicate database liên tục với ít ETL | Mirroring | Connector support và trạng thái GA/Preview |
| Copy đơn giản hoặc CDC no-code | Copy Job | Source/destination support, update method |
| Low-code Power Query | Dataflow Gen2 | Query folding, destination, refresh behavior |
| Điều phối nhiều bước, branch/retry/alert | Data Pipeline | Parameters, dependencies, schedule/event trigger |
| Transform phân tán, code phức tạp | Notebook/PySpark | Runtime, Environment, pool, partition/skew |
| Quan hệ, star schema, T-SQL analytics | Warehouse | SQL surface area và load pattern |
| Delta/open format, Spark + SQL | Lakehouse | File size, table maintenance, interoperability |
| Telemetry/log time-series, KQL | Eventhouse | Hot cache, retention, ingestion latency |
| Streaming low-code | Eventstream | Source, processor, destination, event-time semantics |
| Full snapshot thay toàn bộ | Overwrite/CORT/INSERT OVERWRITE | Giữ table identity và schema có đổi không |
| Chỉ lấy row mới/đổi | Watermark hoặc CDC | Delete signal, retry, state commit |
| Xem job đang chạy/lỗi | Monitoring Hub | Nếu là resource pressure thì xem Capacity Metrics |
| Capacity overload/throttling | Capacity Metrics app | Smoothing, background vs interactive, timepoint |
| Giới hạn người dùng | Least privilege | Workspace → item → data/engine security, access path |

## Những bẫy đã hiệu chỉnh trong bộ tài liệu

- Contributor/Viewer không được Reshare chỉ nhờ workspace role, nhưng có thể share item nếu được cấp **Reshare** riêng.
- Custom Airflow pool là **always-on** cho đến khi pause; không nên diễn giải thành SLA high availability cho toàn dịch vụ.
- `MERGE` không tự thân luôn idempotent. Phải có source dedup, unique match key, deterministic conditions và commit state/watermark sau success.
- Nhiều source row cùng match một target row có thể làm `MERGE` lỗi; `foreachBatch` phải xử lý vấn đề này.
- Checkpoint là điều kiện để recovery/fault tolerance bền vững trong Structured Streaming production, nhưng không phải mọi console/memory test đều bắt buộc khai báo checkpoint.
- `OPTIMIZE` giải quyết small-file/layout problem; `VACUUM` xóa file không còn được tham chiếu. Không dùng thay thế cho nhau.
- Không bật Spark Autotune với Runtime 2.0: tài liệu hiện hành vẫn giới hạn Autotune ở Runtime 1.2.
- Admin/Member/Contributor bypass OneLake RLS/CLS; test granular security bằng Viewer hoặc item-level user phù hợp.

## Snapshot tính năng dễ đổi — cập nhật 05/11/2026

| Tính năng | Trạng thái/giới hạn cần nhớ tại thời điểm đối chiếu | Link kiểm tra live |
|---|---|---|
| DP-700 study guide | Trang cập nhật 30/10/2026; 49 bullet (bản trước 21/07/2026) | [Study Guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700) |
| Fabric Runtime 2.0 | **GA, mặc định** cho workspace/Environment mới (10/2026); Spark 4.1, Delta 4.2, Python 3.13, Java 21, Scala 2.13. Runtime 1.3 = EOSA. `EventHubConnector` bị gỡ → Kafka endpoint | [Runtime 2.0](https://learn.microsoft.com/en-us/fabric/data-engineering/runtime-2-0) |
| Spark Autotune | Preview, off by default, chỉ tương thích Runtime 1.2 → không bật được trên 1.3/2.0 | [Autotune](https://learn.microsoft.com/en-us/fabric/data-engineering/autotune) |
| OneLake security | Trang Learn (09/2026) không ghi nhãn GA/Preview tổng thể; **enforcement trong authorized third-party engines: Preview**. Admin/Member/Contributor không bị roles ràng buộc | [Get started](https://learn.microsoft.com/en-us/fabric/onelake/security/get-started-onelake-security) |
| Warehouse `IDENTITY` | Preview; chỉ `BIGINT`, không seed/increment custom | [IDENTITY](https://learn.microsoft.com/en-us/fabric/data-warehouse/identity) |
| Warehouse data clustering | Preview; `CLUSTER BY`, 1–4 columns | [Data clustering](https://learn.microsoft.com/en-us/fabric/data-warehouse/data-clustering) |
| Materialized Lake Views | Lakehouse (không phải Warehouse MV); SQL authoring; **PySpark authoring Preview** (chỉ full refresh); incremental refresh cần CDF | [Materialized Lake Views](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/overview-materialized-lake-view) |
| Custom live pools | Preview trong Runtime 2.0 | [Runtime 2.0](https://learn.microsoft.com/en-us/fabric/data-engineering/runtime-2-0#compute-management-in-runtime-20) |
| Fabric Apache Airflow job ("data workflow") | Airflow 2.10.5, Python 3.12; Private/VNet chưa được hỗ trợ; không đổi version Airflow của job đã tạo | [Airflow workspace settings](https://learn.microsoft.com/en-us/fabric/data-factory/apache-airflow-jobs-workspace-settings) |
| Copy job SCD Type 2 | Preview; cột `Valid_From`, `Valid_To`, `Is_Current`; chưa hỗ trợ CDC từ Oracle | [CDC in Copy job](https://learn.microsoft.com/fabric/data-factory/cdc-copy-job) |
| Workspace-level surge protection | Preview; ngưỡng % CU/24h, Available/Mission critical/Blocked | [Surge protection](https://learn.microsoft.com/en-us/fabric/enterprise/surge-protection) |
| Git — đổi branch | Admin; hoặc Contributor/Member khi bật *Allow users with at least Contributor role to change Git branch* | [Git permissions](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-integration-process) |
| OneLake lifecycle management | 1 policy/workspace, tối đa 10 rules, tier Hot/Cool/Cold | [Lifecycle management](https://learn.microsoft.com/en-us/fabric/onelake/onelake-lifecycle-management) |

> Các trạng thái trên là ảnh chụp kiến thức để ôn thi, không phải cam kết roadmap. Kiểm tra lại trong tuần thi tại [What’s new in Fabric](https://learn.microsoft.com/en-us/fabric/fundamentals/whats-new) và [Fabric known issues](https://support.fabric.microsoft.com/known-issues/).

## Readiness gate trước khi đăng ký thi

Các ngưỡng dưới đây là tiêu chuẩn tự học khuyến nghị, **không phải quy định chính thức của Microsoft**:

- [ ] Tự giải thích 49 bullet (và 5 related topic) mà không đọc nguyên văn tài liệu.
- [ ] Đạt ít nhất **85%** câu scenario trong từng domain, không chỉ 85% tổng.
- [ ] Làm Microsoft Practice Assessment **bản mới trên AI Skills Navigator** ([aka.ms/DP700-PA](http://aka.ms/DP700-PA); bản legacy trên Learn không còn được cập nhật) ít nhất hai lượt; lượt sau đạt ổn định và giải thích được đáp án sai.
- [ ] Tự hoàn thành các lab full/incremental load, SCD2, pipeline, streaming, KQL, T-SQL security, monitoring và optimization.
- [ ] Viết được từ trí nhớ một ví dụ SQL, PySpark và KQL end-to-end.
- [ ] Phân biệt được Monitoring Hub với Capacity Metrics; `OPTIMIZE` với `VACUUM`; shortcut với mirroring; Pipeline với Dataflow Gen2 với Notebook.
- [ ] Giải được security scenario bằng cách xác định **identity + workspace/item permission + engine/access path + data policy**.
- [ ] Đọc lại live Study Guide và snapshot feature 3–7 ngày trước kỳ thi.

Nếu chưa đạt một ô, quay lại đúng file chuyên đề thay vì đọc lại cả bộ từ đầu.

## Nguồn tham khảo ưu tiên

### Microsoft — nguồn chính cho phạm vi thi và Fabric behavior

- [DP-700 Study Guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700)
- [Microsoft Certified: Fabric Data Engineer Associate](https://learn.microsoft.com/en-us/credentials/certifications/fabric-data-engineer-associate/)
- [Course DP-700T00-A](https://learn.microsoft.com/en-us/training/courses/dp-700t00)
- [Microsoft Fabric documentation](https://learn.microsoft.com/en-us/fabric/)
- [Microsoft Fabric training](https://learn.microsoft.com/en-us/training/fabric/)
- [What’s new in Microsoft Fabric](https://learn.microsoft.com/en-us/fabric/fundamentals/whats-new)
- [Fabric REST APIs](https://learn.microsoft.com/en-us/rest/api/fabric/)
- [T-SQL surface area in Fabric Warehouse](https://learn.microsoft.com/en-us/fabric/data-warehouse/tsql-surface-area)
- [Kusto Query Language reference](https://learn.microsoft.com/en-us/kusto/query/?view=microsoft-fabric)
- [Power Query M reference](https://learn.microsoft.com/en-us/powerquery-m/)

### Engine documentation — nguồn kiểm tra semantics của code

- [Apache Spark Structured Streaming guide](https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html)
- [Delta Lake documentation](https://docs.delta.io/)
- [Delta MERGE semantics](https://learn.microsoft.com/en-us/azure/databricks/delta/merge#merge-operation-semantics)

Khi nguồn cộng đồng mâu thuẫn với Microsoft Learn về Fabric feature status, support matrix hoặc UI behavior, ưu tiên Microsoft Learn hiện hành; khi cần hiểu semantics nền tảng của Spark/Delta, đối chiếu thêm tài liệu engine chính thức.
