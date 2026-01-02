# Real-time Job Market Analytics with Big Data Streaming (Apache Kafka)

Dự án xây dựng pipeline phân tích thị trường việc làm theo **dòng dữ liệu thời gian thực (streaming)** trên nền tảng **Big Data**.  
Dữ liệu tin tuyển dụng được phát lên **Apache Kafka** dưới dạng JSON; consumer đọc theo batch để tổng hợp thống kê và xuất kết quả ra **CSV/JSON/PNG** phục vụ báo cáo.

---

## 1) Mục tiêu
- Xây dựng pipeline **Producer → Kafka → Consumer** cho dữ liệu job postings.
- Tạo kết quả phân tích mô tả:
  - Phân bố số lượng tin theo **quốc gia**
  - Phân bố **job type** (Onsite/Hybrid/Remote)
  - **Top skills** (nhóm kỹ năng xuất hiện nhiều)
- Xuất kết quả có thể kiểm chứng và tái lập: CSV/JSON/PNG + báo cáo LaTeX.

---

## 2) Kiến trúc tổng quan
Luồng dữ liệu:
`CSV Dataset → Producer → Kafka Topic (jobs_raw) → Consumer Group → Outputs (CSV/JSON/PNG)`

Vai trò Kafka/Big Data:
- Kafka là tầng ingest/streaming (commit-log), tách ingest và analytics, hỗ trợ replay dữ liệu.
- Consumer xử lý theo batch và commit offset theo chu kỳ để kiểm soát tiến độ xử lý.

---

## 3) Các file/folder và nội dung
- **kafka/**: cấu hình Docker Compose để khởi tạo Kafka/Zookeeper.
- **app/producer.py**: đọc CSV theo từng phần (chunk), chuẩn hoá record, serialize JSON và gửi message lên topic `jobs_raw`.
- **app/consumer.py**: subscribe topic `jobs_raw`, poll theo batch, tổng hợp thống kê (country/job_type/skills), export CSV/JSON và (tuỳ chọn) vẽ biểu đồ PNG.
- **data_sample/jobs_clean_sample.csv**: sample dataset để chạy demo và tái lập pipeline.
- **data/**: dataset full và/hoặc output khi chạy local (không push lên GitHub do giới hạn dung lượng).
- **run_metrics.json**: metrics của lần chạy consumer (processed/runtime/throughput, phân bố country/job type, thông số liên quan).
- **top_countries.png**: biểu đồ phân bố số lượng tin theo quốc gia.
- **top_job_types.png**: biểu đồ phân bố job type.
- **top_skills_overall.png**: biểu đồ top skills tổng hợp.
- **main.tex**: báo cáo LaTeX (Overleaf), mô tả hệ thống + kết quả + phân tích dữ liệu.
- **Picture1.png**: logo dùng cho trang bìa báo cáo.
- **requirements.txt**: danh sách thư viện Python cần để chạy producer/consumer.
- **.gitignore**: loại bỏ dataset full/artefacts lớn khỏi repo.

---

## 4) Yêu cầu môi trường
- Docker Desktop + Docker Compose
- Python 3.10+ (khuyến nghị 3.11)

Cài thư viện:
```bash
pip install -r requirements.txt

## 5) Cách chạy nhanh (Quickstart)

### 5.1 Khởi động Kafka
- Di chuyển vào thư mục cấu hình Kafka: `cd kafka`
- Khởi động Kafka/Zookeeper bằng Docker Compose: `docker compose up -d`
- Kiểm tra container đang chạy: `docker ps`

### 5.2 Chạy Producer (CSV → Kafka)
- Chạy producer với sample dataset (khuyến nghị để tái lập):  
  `python app/producer.py --bootstrap localhost:29092 --topic jobs_raw --csv data_sample/jobs_clean_sample.csv`
- Nếu chạy dataset đầy đủ (chỉ có trên máy local):  
  `python app/producer.py --bootstrap localhost:29092 --topic jobs_raw --csv data/jobs_clean.csv`

### 5.3 Chạy Consumer (Kafka → Outputs)
- Chạy consumer để đọc topic và tạo thống kê (tự dừng khi idle):  
  `python app/consumer.py --bootstrap localhost:29092 --topic jobs_raw --idle-seconds 25 --top-n 30`
- Nếu cần xuất biểu đồ PNG (phục vụ báo cáo):  
  `python app/consumer.py --bootstrap localhost:29092 --topic jobs_raw --idle-seconds 25 --top-n 30 --plot`

---

## 6) Output
Sau khi consumer hoàn tất, hệ thống tạo các đầu ra phục vụ phân tích và báo cáo:
- `run_metrics.json`: tổng quan lần chạy (processed/runtime/throughput) và thống kê phân bố (country/job type, v.v.).
- `top_countries.png`: biểu đồ phân bố số lượng tin theo quốc gia.
- `top_job_types.png`: biểu đồ phân bố job type.
- `top_skills_overall.png`: biểu đồ top skills tổng hợp.
- Các bảng tổng hợp dạng CSV (tuỳ cấu hình consumer) để phân tích sâu hơn.

---

## 7) Ghi chú diễn giải dữ liệu
- Kết quả “overall” phụ thuộc vào phân bố mẫu: nếu dữ liệu tập trung ở một quốc gia, các insight tổng hợp sẽ bị chi phối bởi nhóm đó.
- Biến kỹ năng có thể bị tách do khác cách viết (ví dụ `Customer service` và `Customer Service`), vì vậy cần chuẩn hoá văn bản để xếp hạng top skills ổn định và phản ánh đúng nhu cầu năng lực.
- Job type lệch mạnh có thể xuất phát từ cách gắn nhãn hoặc mặc định khi thiếu dữ liệu; nên kiểm tra tỷ lệ thiếu và quy tắc chuẩn hoá trước khi kết luận.

---

## 8) Authors
- Đặng Đức Duy — 23020347
- Trịnh Hoàng Đức — 23020359

Giảng viên hướng dẫn:
- TS. Trần Hồng Việt
- ThS. Ngô Minh Hương

