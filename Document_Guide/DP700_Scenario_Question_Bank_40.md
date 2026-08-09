# DP-700 SCENARIO QUESTION BANK — 40 CÂU TỰ SOẠN
> Đây là câu hỏi luyện tập tự soạn theo blueprint, **không phải câu hỏi thật hoặc exam dump**.

## Câu 1 — D1 Workspace

**Một workload scheduled cần Spark session khởi động ổn định vào khung 02:00–04:00 và Environment có nhiều wheel libraries. Chọn compute nào?**
A. Starter pool  
B. Custom live pool với active window  
C. High concurrency giữa nhiều users  
D. Airflow starter pool  

<details><summary>Đáp án và giải thích</summary>

**B.** Custom live pool pre-warm compute và libraries trong active window; starter pool là best-effort.

</details>

## Câu 2 — D1 Workspace

**Hai notebook cùng workspace nhưng do hai user khác nhau chạy. High concurrency có chia sẻ cùng Spark session không?**
A. Có, vì cùng workspace  
B. Có, nếu cùng default lakehouse  
C. Không, session sharing theo cùng user  
D. Chỉ khi cả hai là Admin  

<details><summary>Đáp án và giải thích</summary>

**C.** Notebook high concurrency bị giới hạn theo user boundary.

</details>

## Câu 3 — D1 Workspace

**Solution dùng Runtime 2.0 Delta 4.2 feature experimental nhưng Warehouse/Direct Lake cũng phải đọc table. Hành động tốt nhất?**
A. Bật ngay vì runtime mới nhất  
B. Kiểm tra interoperability và thử trên bản sao  
C. VACUUM 0 giờ  
D. Chuyển table sang CSV  

<details><summary>Đáp án và giải thích</summary>

**B.** Delta 4.x-specific features có thể chỉ hoạt động ở Spark experiences.

</details>

## Câu 4 — D1 Lifecycle

**Ai có quyền tối thiểu để connect workspace với Git repo?**
A. Viewer  
B. Contributor  
C. Member  
D. Admin  

<details><summary>Đáp án và giải thích</summary>

**D.** Initial connect/sync/disconnect yêu cầu workspace Admin.

</details>

## Câu 5 — D1 Lifecycle

**Contributor có Write trên mọi item và Git repo, workspace đã connect. Họ cần làm gì để update workspace từ Git?**
A. Nâng lên Admin bắt buộc  
B. Có thể update nếu đáp ứng owner/build/tenant conditions  
C. Tạo deployment pipeline trước  
D. Export data files vào repo  

<details><summary>Đáp án và giải thích</summary>

**B.** Sau connection, Contributor có đủ permissions có thể commit/update.

</details>

## Câu 6 — D1 Lifecycle

**Mục tiêu là promote Dev→Test→Prod và đổi connection theo môi trường. Chọn tổ hợp?**
A. Git chỉ  
B. Deployment pipeline + variable library/deployment rules  
C. OneLake shortcut  
D. Monitoring Hub  

<details><summary>Đáp án và giải thích</summary>

**B.** Deployment pipeline xử lý promotion; variable library/rules xử lý environment values.

</details>

## Câu 7 — D1 Security

**User cần xem một lakehouse duy nhất, không phải toàn workspace. Least privilege?**
A. Contributor workspace  
B. Admin workspace  
C. Share item với Read và OneLake role nếu cần  
D. Domain Admin  

<details><summary>Đáp án và giải thích</summary>

**C.** Item sharing tránh cấp quyền toàn workspace.

</details>

## Câu 8 — D1 Security

**Contributor được thêm vào OneLake RLS role giới hạn Region=APAC nhưng vẫn thấy toàn bộ region. Vì sao?**
A. RLS delay 24h  
B. Contributor bypass OneLake RLS/CLS  
C. Sensitivity label lỗi  
D. KQL không hỗ trợ where  

<details><summary>Đáp án và giải thích</summary>

**B.** Elevated workspace roles bypass granular OneLake security.

</details>

## Câu 9 — D1 Security

**External engine đọc table có OneLake RLS nhưng không được authorized. Kết quả mong đợi?**
A. Luôn đọc raw files  
B. Có thể bị block thay vì tự enforce RLS  
C. Tự chuyển thành Viewer  
D. Sensitivity label bị xóa  

<details><summary>Đáp án và giải thích</summary>

**B.** Non-authorized engines không được giả định tự thực thi policies.

</details>

## Câu 10 — D1 Orchestration

**Cần low-code rename/filter/merge với connector và query folding. Chọn?**
A. Dataflow Gen2  
B. Notebook  
C. Deployment pipeline  
D. Activator  

<details><summary>Đáp án và giải thích</summary>

**A.** Power Query low-code và folding là tín hiệu Dataflow Gen2.

</details>

## Câu 11 — D1 Orchestration

**Cần orchestrate Copy→Notebook→Validation→Teams on failure. Chọn?**
A. Pipeline  
B. Dataflow Gen2  
C. KQL database  
D. Sensitivity label  

<details><summary>Đáp án và giải thích</summary>

**A.** Pipeline cung cấp dependencies, activities, conditions và alert actions.

</details>

## Câu 12 — D1 Orchestration

**40 bảng theo metadata, tối đa 5 song song. Pattern?**
A. 40 pipelines  
B. Lookup + ForEach batch count 5 + parameters  
C. One notebook hardcode  
D. OneLake role  

<details><summary>Đáp án và giải thích</summary>

**B.** Metadata-driven ForEach là pattern chuẩn.

</details>

## Câu 13 — D2 Loading

**Incremental pipeline lỗi sau MERGE nhưng trước update watermark. Thiết kế idempotent?**
A. Append lại  
B. Staging + dedup/MERGE + update watermark sau success  
C. Overwrite Delta log  
D. VACUUM trước rerun  

<details><summary>Đáp án và giải thích</summary>

**B.** Rerun không duplicate và watermark chỉ commit khi target thành công.

</details>

## Câu 14 — D2 Loading

**Full snapshot mới phải thay dữ liệu nhưng giữ table identity/history. Chọn?**
A. DROP + CREATE  
B. CREATE OR REPLACE hoặc INSERT OVERWRITE  
C. VACUUM 0 HOURS  
D. DELETE _delta_log  

<details><summary>Đáp án và giải thích</summary>

**B.** Replace/overwrite tạo Delta version mới và giữ identity/history tới retention.

</details>

## Câu 15 — D2 Loading

**Late-arriving fact chưa có dimension key. Pattern?**
A. Drop fact  
B. Unknown member + later key correction  
C. Grant Admin  
D. Disable relationship  

<details><summary>Đáp án và giải thích</summary>

**B.** Đây là pattern dimensional loading thông dụng.

</details>

## Câu 16 — D2 Batch

**Structured/semi-structured, PySpark, ML và medallion. Store?**
A. Lakehouse  
B. Warehouse only  
C. Eventhouse only  
D. SQL Database only  

<details><summary>Đáp án và giải thích</summary>

**A.** Lakehouse phù hợp Spark, multi-format và medallion.

</details>

## Câu 17 — D2 Batch

**Relational star schema, T-SQL, high concurrency BI. Store?**
A. Warehouse  
B. Eventhouse  
C. Notebook  
D. Airflow pool  

<details><summary>Đáp án và giải thích</summary>

**A.** Warehouse tối ưu relational/T-SQL analytics.

</details>

## Câu 18 — D2 Batch

**Data ở ADLS/S3 cần truy cập không copy. Chọn?**
A. OneLake shortcut  
B. Copy full mỗi giờ  
C. Import model  
D. Deployment pipeline  

<details><summary>Đáp án và giải thích</summary>

**A.** Shortcut tham chiếu source mà không di chuyển dữ liệu.

</details>

## Câu 19 — D2 Batch

**Data JSON lớn cần UDF/dedup/complex schema evolution. Transform?**
A. Notebook PySpark  
B. Dataflow chỉ vì low-code  
C. Deployment pipeline  
D. Sensitivity label  

<details><summary>Đáp án và giải thích</summary>

**A.** Spark phù hợp distributed custom transformation.

</details>

## Câu 20 — D2 Batch

**Time-series telemetry đã ở Eventhouse, cần aggregate 5 phút. Language?**
A. KQL summarize + bin  
B. T-SQL MERGE vào Git  
C. DAX only  
D. M language in deployment pipeline  

<details><summary>Đáp án và giải thích</summary>

**A.** KQL là ngôn ngữ chính của Eventhouse.

</details>

## Câu 21 — D2 Streaming

**Yêu cầu low-latency dashboard/alert, event source và KQL. Chọn?**
A. Eventstream→Eventhouse  
B. Daily pipeline  
C. Import semantic model  
D. Full load notebook  

<details><summary>Đáp án và giải thích</summary>

**A.** RTI phù hợp operational real-time analytics.

</details>

## Câu 22 — D2 Streaming

**Cần stateful stream-stream join và Delta sink với checkpoint. Chọn?**
A. Spark Structured Streaming  
B. Deployment pipeline  
C. Sensitivity label  
D. Git integration  

<details><summary>Đáp án và giải thích</summary>

**A.** Spark state store/checkpoint phù hợp stateful ETL.

</details>

## Câu 23 — D2 Streaming

**Delta table trong OneLake, không muốn ingest copy nhưng KQL 7 ngày gần nhất cần nhanh. Chọn?**
A. Standard shortcut  
B. Shortcut + query acceleration  
C. Native table bắt buộc  
D. CSV export  

<details><summary>Đáp án và giải thích</summary>

**B.** Acceleration cache khoảng dữ liệu để tăng query performance.

</details>

## Câu 24 — D2 Streaming

**Mỗi event thuộc đúng một cửa sổ 5 phút. Window?**
A. Tumbling  
B. Sliding  
C. Session  
D. Snapshot  

<details><summary>Đáp án và giải thích</summary>

**A.** Tumbling fixed-size, nonoverlapping.

</details>

## Câu 25 — D2 Streaming

**Session kết thúc sau 15 phút inactivity. Window?**
A. Tumbling  
B. Sliding  
C. Session  
D. Hopping 1 ngày  

<details><summary>Đáp án và giải thích</summary>

**C.** Session window dùng inactivity gap.

</details>

## Câu 26 — D2 Streaming

**Structured Streaming state tăng không giới hạn. Hành động?**
A. Bỏ checkpoint  
B. Thêm watermark/TTL và key/window hợp lý  
C. Tăng Viewer role  
D. Git commit  

<details><summary>Đáp án và giải thích</summary>

**B.** Bounded state cần watermark/TTL và thiết kế key.

</details>

## Câu 27 — D3 Monitoring

**Cần xem mọi pipeline/notebook run failed trong workspace. Tool?**
A. Monitoring Hub  
B. Capacity Metrics app  
C. Git repo  
D. Domain  

<details><summary>Đáp án và giải thích</summary>

**A.** Monitoring Hub là operational run monitoring.

</details>

## Câu 28 — D3 Monitoring

**Cần tìm item tiêu nhiều CU và throttling. Tool?**
A. Capacity Metrics app  
B. Monitoring Hub only  
C. KQL database schema  
D. Deployment pipeline  

<details><summary>Đáp án và giải thích</summary>

**A.** Metrics app phân tích capacity resource usage.

</details>

## Câu 29 — D3 Monitoring

**Email cho scheduled pipeline failure đơn giản. Chọn?**
A. Built-in failure notification  
B. Custom Spark pool  
C. V-Order  
D. RLS  

<details><summary>Đáp án và giải thích</summary>

**A.** Built-in notification là cách ít vận hành nhất.

</details>

## Câu 30 — D3 Monitoring

**Alert Teams cho manual và scheduled failures, condition phức tạp. Chọn?**
A. Activator trên Job events/KQL  
B. Only schedule email  
C. VACUUM  
D. GitHub issue  

<details><summary>Đáp án và giải thích</summary>

**A.** Activator hỗ trợ actions và conditions rộng hơn.

</details>

## Câu 31 — D3 Monitoring

**CU peak 120% nhưng chưa reject request. Diễn giải?**
A. Không thể xảy ra  
B. Overload; smoothing/overage protection/autoscale quyết định throttling stage  
C. Mọi job succeeded  
D. KQL syntax error  

<details><summary>Đáp án và giải thích</summary>

**B.** >100% không đồng nghĩa immediate rejection.

</details>

## Câu 32 — D3 Errors

**AuthenticationError lặp lại mọi run. FIRST?**
A. Tăng retry  
B. Kiểm tra credential/identity/permission  
C. Tăng partitions  
D. OPTIMIZE  

<details><summary>Đáp án và giải thích</summary>

**B.** Persistent auth error không phải transient.

</details>

## Câu 33 — D3 Errors

**Một vài Spark tasks rất chậm trong khi đa số nhanh. MOST likely?**
A. Data skew  
B. Git conflict  
C. Sensitivity label  
D. Airflow pool paused  

<details><summary>Đáp án và giải thích</summary>

**A.** Straggler tasks thường do skew/uneven partitions.

</details>

## Câu 34 — D3 Errors

**Dataflow owner rời tổ chức, staging permission error. Fix?**
A. Take ownership và refresh credentials/connections  
B. Scale capacity only  
C. VACUUM  
D. Change KQL hot cache  

<details><summary>Đáp án và giải thích</summary>

**A.** Ownership/credentials là root cause phổ biến.

</details>

## Câu 35 — D3 Errors

**GatewayUnreachable xảy ra định kỳ. FIRST evidence?**
A. Đoán auto-update  
B. Đối chiếu gateway/service/network logs theo timestamp  
C. Drop table  
D. Disable security  

<details><summary>Đáp án và giải thích</summary>

**B.** Không kết luận từ chu kỳ; cần log correlation.

</details>

## Câu 36 — D3 Optimize

**Delta table có hàng nghìn small files. FIRST?**
A. OPTIMIZE/compaction  
B. VACUUM only  
C. Grant Admin  
D. Git sync  

<details><summary>Đáp án và giải thích</summary>

**A.** OPTIMIZE giảm file count; VACUUM dọn obsolete files.

</details>

## Câu 37 — D3 Optimize

**Cần xóa obsolete files nhưng giữ time travel 7 ngày. Chọn?**
A. VACUUM với retention ≥168h theo policy  
B. DROP table  
C. Delete _delta_log  
D. Overwrite CSV  

<details><summary>Đáp án và giải thích</summary>

**A.** VACUUM tuân retention; không xóa log trực tiếp.

</details>

## Câu 38 — D3 Optimize

**Read-heavy cross-engine trên Parquet/Delta. Cân nhắc?**
A. V-Order  
B. RetryCount  
C. Sensitivity label  
D. Airflow DAG  

<details><summary>Đáp án và giải thích</summary>

**A.** V-Order tối ưu layout file cho đọc.

</details>

## Câu 39 — D3 Optimize

**Runtime 2.0 table filter theo high-cardinality columns và cần đổi keys. Chọn?**
A. Liquid clustering  
B. Static partition by CustomerId  
C. VACUUM only  
D. Dataflow Gen2  

<details><summary>Đáp án và giải thích</summary>

**A.** Liquid clustering linh hoạt cho high-cardinality/file skipping.

</details>

## Câu 40 — D3 Optimize

**Spark join fact lớn với dimension nhỏ. FIRST optimization sau khi xác nhận size?**
A. Broadcast dimension và xem physical plan  
B. Repartition thành 1  
C. Collect fact về driver  
D. VACUUM warehouse  

<details><summary>Đáp án và giải thích</summary>

**A.** Broadcast có thể tránh shuffle; phải xác nhận size và explain plan.

</details>
