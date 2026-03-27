# Agoda Hotel Scraper

Web App dùng Streamlit để cào dữ liệu khách sạn từ Agoda phục vụ nghiên cứu thị trường.

## Cài đặt

### Bước 1: Tạo môi trường ảo (khuyến nghị)
```bash
python -m venv venv
# Windows
venv\Scripts\activate
# macOS/Linux
source venv/bin/activate
```

### Bước 2: Cài đặt thư viện
```bash
pip install -r requirements.txt
```

### Bước 3: Cài Playwright browsers
```bash
playwright install chromium
playwright install-deps chromium
```

## Chạy ứng dụng

```bash
streamlit run app.py
```

Mở trình duyệt tại: `http://localhost:8501`

## Cấu trúc file

```
agoda-scraper/
├── app.py          # Giao diện Streamlit
├── scraper.py      # Logic cào dữ liệu (Playwright)
├── requirements.txt
└── README.md
```

## Dữ liệu thu thập

| Trường | Mô tả |
|--------|-------|
| Tỉnh thành / Điểm đến | Địa điểm tìm kiếm |
| Tên khách sạn | Tên đầy đủ |
| Địa chỉ | Địa chỉ cụ thể |
| Hạng sao | 1-5 sao |
| Giá thấp nhất (đã gồm thuế & phí) | Giá đã bao gồm thuế |
| Meal Plan | Có/không bao gồm bữa sáng... |
| Chính sách hoàn hủy | Hủy miễn phí / Không hoàn tiền... |

## Lưu ý

- App sử dụng Playwright headless Chromium, **không cần cài Chrome riêng**.
- Random delay và random User-Agent được bật mặc định.
- Agoda có thể thay đổi cấu trúc HTML — nếu không lấy được dữ liệu, kiểm tra log để debug.
- Chỉ dùng cho mục đích nghiên cứu, tuân thủ ToS của Agoda.
