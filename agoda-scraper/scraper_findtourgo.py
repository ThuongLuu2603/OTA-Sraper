import math
import re
from datetime import datetime
from urllib.parse import parse_qs, urlencode, urlparse
import concurrent.futures
import requests

BASE_SITE = "https://findtourgo.com"
SEARCH_API = "https://api-v2.findtourgo.com/v1/search/tours"
DETAIL_API = "https://api-v2.findtourgo.com/v1/public/tours/{tour_code}"

def _safe_text(v, default=""):
    if v is None:
        return default
    return str(v).strip()

def _to_int(v, default=0):
    try:
        return int(v)
    except Exception:
        return default

def _fmt_money(v):
    try:
        n = float(v or 0)
        return f"{n:,.0f}"
    except Exception:
        return ""

def _pick_display_price(item: dict, want_currency: str) -> tuple[float | None, str]:
    """
    Giá ở cấp item thường theo item['currency'] (USD hoặc VND tùy nhà điều hành).
    Để khớp filter currency= trên URL, lấy đúng phần tử trong item['prices'].
    """
    want = _safe_text(want_currency).upper() or "USD"

    for p in item.get("prices") or []:
        if not isinstance(p, dict):
            continue
        if _safe_text(p.get("currency")).upper() != want:
            continue
        sale = p.get("salePrice")
        reg = p.get("regularPrice")
        try:
            if sale is not None and float(sale) > 0:
                return float(sale), want
            if reg is not None and float(reg) > 0:
                return float(reg), want
        except (TypeError, ValueError):
            pass

    native = _safe_text(item.get("currency")).upper() or want
    sale_price = item.get("salePrice")
    regular_price = item.get("regularPrice")
    try:
        if sale_price is not None and float(sale_price) > 0:
            return float(sale_price), native
        if regular_price is not None and float(regular_price) > 0:
            return float(regular_price), native
    except (TypeError, ValueError):
        pass
    return None, want

def _build_public_tour_url(locale: str, tour_code: str, slug: str) -> str:
    locale = _safe_text(locale, "vi") or "vi"
    tour_code = _safe_text(tour_code)
    slug = _safe_text(slug)
    if not tour_code:
        return ""
    if slug:
        return f"{BASE_SITE}/{locale}/tours/{tour_code}/{slug}"
    return f"{BASE_SITE}/{locale}/tours/{tour_code}"

_WEEKDAY_VI = {
    "monday": "Thứ 2",
    "tuesday": "Thứ 3",
    "wednesday": "Thứ 4",
    "thursday": "Thứ 5",
    "friday": "Thứ 6",
    "saturday": "Thứ 7",
    "sunday": "CN",
}

def _iso_to_ddmmyyyy(iso_s: str) -> str:
    """Chuẩn hóa ISO API (UTC) sang dd/mm/yyyy để không hiển thị raw timestamp."""
    s = _safe_text(iso_s)
    if not s:
        return ""
    try:
        if "T" in s:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt.strftime("%d/%m/%Y")
    except Exception:
        pass
    return s

def _weekdays_vi(weekdays: list) -> str:
    parts = []
    for x in weekdays or []:
        k = _safe_text(x).lower()
        if not k:
            continue
        parts.append(_WEEKDAY_VI.get(k, k))
    return ", ".join(parts)

def _departure_range_label(start_iso: str, end_iso: str) -> str:
    a = _iso_to_ddmmyyyy(start_iso)
    b = _iso_to_ddmmyyyy(end_iso)
    if a and b:
        return f"{a} – {b}"
    return " – ".join([x for x in [a, b] if x])

def _extract_one_schedule_departure(sch: dict) -> str:
    """Một dòng mô tả lịch khởi hành cho một tourSchedules entry."""
    if not isinstance(sch, dict):
        return ""
    spec = [_safe_text(x) for x in (sch.get("departureSpecifiedDates") or []) if _safe_text(x)]
    if spec:
        return ", ".join(spec)

    dtype = _safe_text(sch.get("departureType")).upper()
    start_date = sch.get("startDate")
    end_date = sch.get("endDate")
    range_txt = _departure_range_label(start_date or "", end_date or "")
    wd_vi = _weekdays_vi(sch.get("departureWeekdays") or [])

    # DAILY: khoảng ngày là “có tour mỗi ngày”, không phải 2 ngày khởi hành cố định.
    if dtype == "DAILY":
        if range_txt:
            return f"Hằng ngày ({range_txt})"
        return "Hằng ngày"

    if dtype == "RECURRING_WEEKDAYS" and wd_vi:
        if range_txt:
            return f"Theo thứ: {wd_vi} ({range_txt})"
        return f"Theo thứ: {wd_vi}"

    if wd_vi:
        if range_txt:
            return f"Theo thứ: {wd_vi} ({range_txt})"
        return f"Theo thứ: {wd_vi}"

    if range_txt:
        return range_txt

    return ""

def _extract_departure_dates(detail_json: dict) -> str:
    schedules = detail_json.get("tourSchedules") or []
    pieces: list[str] = []
    for sch in schedules:
        seg = _extract_one_schedule_departure(sch)
        if seg:
            pieces.append(seg)
    seen: set[str] = set()
    ordered: list[str] = []
    for p in pieces:
        if p not in seen:
            seen.add(p)
            ordered.append(p)
    return " | ".join(ordered)

_ISO_TIMESTAMP_RE = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+\-]\d{2}:\d{2})"
)

def normalize_findtourgo_departure_display(text: str) -> str:
    """
    Đưa cột Ngày khởi hành về dạng đọc được: thay mọi timestamp ISO trong chuỗi
    (kể cả dữ liệu scrape cũ / nạp từ DB) sang dd/mm/yyyy; dịch thứ tiếng Anh còn sót.
    """
    t = _safe_text(text)
    if not t or t.lower() == "nan":
        return ""

    def iso_sub(m: re.Match) -> str:
        return _iso_to_ddmmyyyy(m.group(0))

    out = _ISO_TIMESTAMP_RE.sub(iso_sub, t)
    out = re.sub(r"(\d{2}/\d{2}/\d{4})\s+-\s+(\d{2}/\d{2}/\d{4})", r"\1 – \2", out)

    def wd_sub(m: re.Match) -> str:
        k = m.group(1).lower()
        return _WEEKDAY_VI.get(k, m.group(0))

    out = re.sub(
        r"\b(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b",
        wd_sub,
        out,
        flags=re.I,
    )
    return out

def _parse_country_and_dates(url: str):
    parsed = urlparse(_safe_text(url))
    q = parse_qs(parsed.query)
    country_code = (q.get("where") or q.get("countryCode") or [""])[0].upper()
    period_start = (q.get("tourPeriodStart") or [""])[0]
    period_end = (q.get("tourPeriodEnd") or [""])[0]
    currency = (q.get("currency") or ["USD"])[0].upper()
    locale = "vi"
    parts = [p for p in parsed.path.split("/") if p]
    if parts and len(parts[0]) == 2:
        locale = parts[0].lower()
    return country_code, period_start, period_end, currency, locale

def _normalize_country_code(raw: str) -> str:
    txt = _safe_text(raw).upper()
    if not txt:
        return ""
    # Keep only letters for robust matching.
    letters = re.sub(r"[^A-Z]", "", txt)
    if len(letters) == 2:
        return letters
    aliases = {
        "TRUNGQUOC": "CN",
        "TRUUNGQUOC": "CN",
        "CHINA": "CN",
        "VIETNAM": "VN",
        "VIETNAMM": "VN",
        "NHATBAN": "JP",
        "JAPAN": "JP",
        "THAILAN": "TH",
        "THAILAND": "TH",
        "HANQUOC": "KR",
        "KOREA": "KR",
        "SINGAPORE": "SG",
        "MALAYSIA": "MY",
        "INDONESIA": "ID",
    }
    return aliases.get(letters, "")

def build_findtourgo_url(
    country_code: str,
    tour_period_start: str,
    tour_period_end: str,
    currency: str = "USD",
    locale: str = "vi",
) -> str:
    query = urlencode(
        {
            "tourPeriodEnd": _safe_text(tour_period_end),
            "tourPeriodStart": _safe_text(tour_period_start),
            "where": _safe_text(country_code).upper(),
            "currency": _safe_text(currency).upper() or "USD",
        }
    )
    locale = _safe_text(locale, "vi") or "vi"
    country_slug = _safe_text(country_code).lower() or "country"
    return f"{BASE_SITE}/{locale}/country/{country_slug}?{query}"


# ==========================================
# CÁC HÀM XỬ LÝ ĐA QUỐC GIA MỚI THAY THẾ
# ==========================================

def _scrape_single_country(
    country_code: str,
    period_start: str,
    period_end: str,
    currency: str,
    locale: str,
    page_size: int,
    max_pages: int,
    status_callback
) -> list:
    """Hàm worker xử lý việc scrape cho MỘT quốc gia duy nhất."""
    
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
    })

    rows = []
    seen_codes = set()
    page = 0
    total_pages = None
    page_size = max(10, min(_to_int(page_size, 50), 100))
    max_pages = max(1, _to_int(max_pages, 20))

    while page < max_pages:
        params = {
            "countryCode": country_code,
            "page": page,
            "pageSize": page_size,
            "tourPeriodStart": period_start,
            "tourPeriodEnd": period_end,
            "locale": locale,
        }
        
        try:
            r = sess.get(SEARCH_API, params=params, timeout=30)
            r.raise_for_status()
            payload = r.json() if r.text else {}
        except Exception as e:
            status_callback(f"❌ Lỗi API khi tìm {country_code} trang {page}: {str(e)}")
            break

        items = payload.get("items") or []
        
        if total_pages is None:
            total_items = _to_int(payload.get("totalItems"), 0)
            total_pages = _to_int(payload.get("totalPage"), 0)
            if total_pages <= 0 and total_items > 0:
                total_pages = int(math.ceil(total_items / float(page_size)))
            status_callback(f"📊 [{country_code}] Tìm thấy ~{total_items} tour, {max(total_pages, 1)} trang")

        if not items:
            break

        for item in items:
            if not isinstance(item, dict):
                continue
            tour_code = _safe_text(item.get("tourCode"))
            if not tour_code or tour_code in seen_codes:
                continue
            seen_codes.add(tour_code)

            detail_url = DETAIL_API.format(tour_code=tour_code)
            departure_text = ""
            try:
                dr = sess.get(detail_url, timeout=30)
                if dr.status_code == 200 and dr.text:
                    departure_text = _extract_departure_dates(dr.json())
            except Exception:
                departure_text = ""

            agency = item.get("travelAgency") or {}
            price_val, price_ccy = _pick_display_price(item, currency)
            stars = item.get("score")
            score = item.get("ratingScore")

            rows.append(
                {
                    "Phân khúc": "Tour",
                    "Nguồn": "FindTourGo",
                    "Quốc gia": country_code,
                    "Tên tour": _safe_text(item.get("name")),
                    "Mã tour": tour_code,
                    "Công ty lữ hành": _safe_text(agency.get("name")),
                    "Thời lượng (ngày)": _to_int(item.get("duration"), 0),
                    "Giá từ": _fmt_money(price_val),
                    "Tiền tệ": price_ccy,
                    "Điểm đánh giá": _safe_text(score),
                    "Xếp hạng nội bộ": _safe_text(stars),
                    "Ngày khởi hành": departure_text,
                    "Link tour": _build_public_tour_url(locale=locale, tour_code=tour_code, slug=_safe_text(item.get("slug"))),
                }
            )

        status_callback(f"📄 [{country_code}] Xong trang {page + 1}/{total_pages} (+{len(items)} items)")
        page += 1
        if total_pages and page >= total_pages:
            break

    return rows

def run_scrape_multi_findtourgo(
    country_codes_input: str,  # Nhập chuỗi: "VN, TH, JP, SG"
    period_start: str = "",
    period_end: str = "",
    currency: str = "USD",
    locale: str = "vi",
    page_size: int = 50,
    max_pages: int = 20,
    max_workers: int = 4,      # Số luồng chạy song song (tùy chỉnh)
    status_callback=None,
) -> list:
    """Hàm chính điều phối việc chạy đa luồng cho nhiều quốc gia."""
    
    if status_callback is None:
        status_callback = lambda _msg: None

    # 1. Tiền xử lý đầu vào
    period_start = _safe_text(period_start)
    period_end = _safe_text(period_end)
    currency = _safe_text(currency).upper() or "USD"
    locale = _safe_text(locale).lower() or "vi"

    if not period_start or not period_end:
        raise ValueError("Thiếu tourPeriodStart hoặc tourPeriodEnd.")

    # Tách chuỗi nhập vào thành danh sách các quốc gia hợp lệ
    raw_codes = [c.strip() for c in _safe_text(country_codes_input).replace(";", ",").split(",")]
    valid_country_codes = set()
    for code in raw_codes:
        norm = _normalize_country_code(code)
        if norm:
            valid_country_codes.add(norm)
            
    valid_country_codes = list(valid_country_codes)
    
    if not valid_country_codes:
        raise ValueError("Thiếu hoặc sai country code. Vui lòng nhập mã như VN, TH, JP, CN...")

    status_callback(f"🌐 Bắt đầu Scrape đa quốc gia: {valid_country_codes} | {period_start} -> {period_end} | Max Workers: {max_workers}")

    all_scraped_rows = []

    # 2. Chạy đa luồng bằng ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Khởi tạo các tasks
        future_to_code = {
            executor.submit(
                _scrape_single_country, 
                code, period_start, period_end, currency, locale, page_size, max_pages, status_callback
            ): code for code in valid_country_codes
        }

        # Thu thập kết quả khi các task hoàn thành
        for future in concurrent.futures.as_completed(future_to_code):
            code = future_to_code[future]
            try:
                rows = future.result()
                all_scraped_rows.extend(rows)
                status_callback(f"✅ Hoàn thành [{code}]: Thu thập được {len(rows)} tours.")
            except Exception as exc:
                status_callback(f"❌ Lỗi nghiêm trọng ở luồng quốc gia [{code}]: {exc}")

    status_callback(f"🎉 Hoàn tất quá trình! Tổng thu thập: {len(all_scraped_rows)} tours duy nhất.")
    return all_scraped_rows