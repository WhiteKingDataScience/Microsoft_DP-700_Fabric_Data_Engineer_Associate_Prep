# ĐÁNH GIÁ MỨC ĐỘ SẴN SÀNG DP-700 VÀ KẾ HOẠCH VƯỢT QUA KỲ THI

**Ngày đánh giá:** 04/08/2026  
**Blueprint:** Skills measured as of 21/07/2026

## 1. Kết luận

Bộ 10 file ban đầu **bao phủ gần như đầy đủ các bullet trong blueprint DP-700**, được chia đúng ba domain 30–35%. Tuy nhiên, **không nên dùng bản ban đầu như nguồn duy nhất để đi thi** vì có một số vấn đề:

- Khẳng định cứng về limits, latency, kích thước dữ liệu và connector count trong khi Microsoft thay đổi thường xuyên.
- Một số mô tả kỹ thuật sai bản chất, nổi bật là cho rằng `overwrite` xóa Delta transaction log.
- Runtime 2.0 đã thay đổi từ Spark 4.0/Delta 4.0 thành Spark 4.1/Delta 4.2 Preview.
- High concurrency bị mô tả như chia sẻ giữa nhiều users, trong khi notebook session sharing theo cùng user và điều kiện tương thích.
- Một số UI/AI feature như “Fix with Copilot”, `/fix`, “Error Insights Copilot” và chu kỳ gateway 7 ngày không có đủ căn cứ để học như facts của kỳ thi.
- Một số câu hỏi dựa trên con số tuyệt đối như >10 GB, sub-second, 1M events/s; đề Microsoft thường yêu cầu chọn kiến trúc theo requirements và trade-offs hơn là ngưỡng tự đặt.

**Đánh giá bản gốc nếu chỉ đọc lý thuyết:** khoảng **72/100 mức sẵn sàng**.  
**Bản đã hiệu chỉnh + làm lab + đạt ≥80% ba lần liên tiếp ở practice/mocked scenarios:** khoảng **88–92/100 mức sẵn sàng**. Đây không phải xác suất đậu và không có bảo đảm; điểm thi là scaled score, cần **700 trở lên**.

## 2. Chấm theo tiêu chí

| Tiêu chí | Bản gốc | Sau cập nhật | Nhận xét |
|---|---:|---:|---|
| Khớp blueprint | 94 | 98 | Đủ toàn bộ nhóm kỹ năng chính |
| Tính chính xác/currentness | 74 | 92 | Đã sửa facts thay đổi nhanh và overclaim |
| SQL/PySpark/KQL | 72 | 90 | Thêm code hợp lệ và decision context |
| Hands-on readiness | 63 | 86 | Cần tự chạy lab trong tenant Fabric |
| Câu hỏi scenario | 76 | 90 | Tăng câu hỏi trade-off, least privilege, troubleshooting |
| Exam strategy | 58 | 91 | Có checklist và kế hoạch 14 ngày |

## 3. Các lỗ hổng ưu tiên đã sửa

1. **Workspace/Spark:** Runtime 2.0, starter pool best-effort, high concurrency cùng user, session timeout.
2. **Lifecycle:** Admin initial Git connection; supported providers/items thay đổi; variable libraries và deployment pipeline trade-offs.
3. **Security:** OneLake enforcement theo supported/authorized engines; elevated workspace roles bypass; permission precedence.
4. **Orchestration:** bỏ ngưỡng GB/connector count tự đặt; thêm dynamic expressions và metadata-driven pipeline.
5. **Loading:** sửa `overwrite`; thêm idempotent MERGE, watermark commit và checkpoint.
6. **Batch/Streaming:** code PySpark/KQL/T-SQL; query acceleration là cache policy chứ không phải “không có local storage”; latency không phải SLA tuyệt đối.
7. **Monitoring:** overload >100% không đồng nghĩa immediate rejection; phân biệt Monitoring Hub, Capacity Metrics, built-in notification và Activator.
8. **Troubleshooting:** bỏ các facts Copilot/gateway không được chứng minh; thêm systematic playbook.
9. **Optimization:** liquid clustering ưu tiên có điều kiện, compatibility; sửa writer API và overwrite/history.

## 4. Kế hoạch 14 ngày

### Ngày 1–2: Domain 1 — Workspace, CI/CD, Security

- Tạo workspace Dev/Test, xem Spark settings, Environment, starter/custom pool.
- Kết nối một workspace test với Git và thực hiện commit/update/conflict.
- Tạo deployment pipeline; phân biệt paired/unpaired items.
- Tạo Viewer/item access và OneLake role; kiểm tra khác biệt với Contributor.

### Ngày 3–5: Domain 2 — Batch và loading

- Full load bằng `CREATE OR REPLACE` hoặc overwrite.
- Incremental load với control watermark và `MERGE`.
- SCD Type 1/2, dedup, missing/late-arriving dimension.
- So sánh Lakehouse, Warehouse, Eventhouse, SQL Database bằng 10 scenarios.

### Ngày 6–7: Domain 2 — Streaming

- Eventstream → Eventhouse/Lakehouse.
- KQL `where`, `project`, `summarize`, `bin`, `join`, materialized view/update policy concepts.
- Spark Structured Streaming với checkpoint, watermark, window, output mode.
- Standard shortcut vs query acceleration vs native table.

### Ngày 8–9: Domain 3 — Monitor và troubleshoot

- Monitoring Hub: pipeline, notebook, Dataflow run.
- Capacity Metrics: identify overload, top operations, smoothing/throttling.
- Activator alert trên Job events hoặc KQL query.
- Thực hành 5 lỗi: credential, schema drift, gateway, OOM/skew, shortcut permission.

### Ngày 10–11: Optimization

- Tạo small files, chạy OPTIMIZE/V-Order/VACUUM trên dữ liệu lab.
- So sánh partitioning, Z-Order và liquid clustering bằng physical plan/file count.
- Tune Copy activity bằng partitioning/parallelism/throughput option.
- KQL filter early, hot cache; Spark broadcast/AQE/skew.

### Ngày 12–13: Mock exam

- Làm 2 lượt 50 câu scenario, giới hạn thời gian 90–100 phút.
- Ghi error log theo blueprint bullet, không chỉ theo tên file.
- Mọi câu sai phải viết lại: requirement → rejected options → correct trade-off.

### Ngày 14: Final review

- Đọc lại bảng “bẫy thi” và các decision matrix.
- Làm Microsoft Practice Assessment và Exam Sandbox.
- Chỉ đặt lịch khi đạt ≥80% ổn định và giải thích được vì sao ba đáp án còn lại sai.

## 5. Kinh nghiệm làm bài

- Đọc câu cuối trước để biết họ hỏi **BEST**, **MINIMUM**, **FIRST** hay **MOST likely**.
- Gạch yêu cầu: latency, no-copy, least privilege, low-code/pro-code, environment promotion, retry/transient, cost, compatibility.
- Không chọn công cụ vì “mới nhất”; chọn vì nó đáp ứng ràng buộc với ít vận hành/rủi ro nhất.
- Với security, xác định workspace role trước rồi mới suy luận RLS/CLS/item permissions.
- Với data movement, phân biệt **copy**, **transform**, **orchestrate**.
- Với monitoring, phân biệt **operation status** và **capacity resource health**.
- Với errors, retry chỉ cho transient; credential/schema/permission phải sửa nguyên nhân.
- Đánh dấu câu dài để quay lại, không tiêu quá 2–3 phút ở lượt đầu.

## 6. Ngưỡng “đủ để thi”

Bạn nên hoãn thi nếu vẫn không thể làm độc lập các việc sau:

- Viết PySpark `MERGE`, dedup và Structured Streaming checkpoint/watermark.
- Viết KQL filter/project/summarize/bin/join và giải thích native vs shortcut vs acceleration.
- Chọn Dataflow Gen2/Pipeline/Notebook theo scenario.
- Giải thích Git vs deployment pipeline và permissions.
- Giải thích OneLake security precedence và bypass.
- Chẩn đoán pipeline/notebook/capacity từ đúng monitoring surface.
- Chọn OPTIMIZE/VACUUM/V-Order/Z-Order/liquid clustering đúng mục đích.

## 7. Nguồn chính thức

- https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700
- https://learn.microsoft.com/en-us/credentials/certifications/fabric-data-engineer-associate/
- https://learn.microsoft.com/en-us/training/courses/dp-700t00
