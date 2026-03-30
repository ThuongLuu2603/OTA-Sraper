"""
travel.com.vn hotel scraper.

Strategy:
1. Open the search page once with Playwright to capture the per-session JWT.
2. Use requests to POST to api2.travel.com.vn/core/Hotel/search-hotel
   for each page (pageIndex = 1, 2, …).
3. Parse response.resultHotels from the JSON payload.

URL format: https://travel.com.vn/hotels/khach-san-tai-{slug}.aspx
            ?room=1&in=DD-MM-YYYY&out=DD-MM-YYYY&adults=2&children=0
            &hid={city_id}&cid={city_id}&htitle={city_name}
"""

import asyncio
import json
import re
import shutil
import time
import sys
from urllib.parse import urlparse, parse_qs, quote

import requests as _requests
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from playwright_bootstrap import ensure_playwright_chromium

# ---------------------------------------------------------------------------
# Known cities: (slug, city_id, display_name)
# ---------------------------------------------------------------------------
CITY_MAP: dict[str, tuple[str, int, str]] = {
    # Hà Nội
    "hà nội": ("ha-noi", 1, "Hà Nội"),
    "ha noi": ("ha-noi", 1, "Hà Nội"),
    "hanoi": ("ha-noi", 1, "Hà Nội"),
    # TP. HCM
    "hồ chí minh": ("ho-chi-minh", 2, "Hồ Chí Minh"),
    "ho chi minh": ("ho-chi-minh", 2, "Hồ Chí Minh"),
    "tp. hồ chí minh": ("ho-chi-minh", 2, "Hồ Chí Minh"),
    "hcm": ("ho-chi-minh", 2, "Hồ Chí Minh"),
    "sài gòn": ("ho-chi-minh", 2, "Hồ Chí Minh"),
    # Đà Nẵng
    "đà nẵng": ("da-nang", 3, "Đà Nẵng"),
    "da nang": ("da-nang", 3, "Đà Nẵng"),
    # Nha Trang
    "nha trang": ("nha-trang-khanh-hoa", 4, "Nha Trang"),
    # Phú Quốc
    "phú quốc": ("phu-quoc-kien-giang", 5, "Phú Quốc"),
    "phu quoc": ("phu-quoc-kien-giang", 5, "Phú Quốc"),
    # Đà Lạt
    "đà lạt": ("da-lat-lam-dong", 6, "Đà Lạt"),
    "da lat": ("da-lat-lam-dong", 6, "Đà Lạt"),
    # Hội An
    "hội an": ("hoi-an-quang-nam", 7, "Hội An"),
    "hoi an": ("hoi-an-quang-nam", 7, "Hội An"),
    # Vũng Tàu
    "vũng tàu": ("vung-tau-ba-ria-vung-tau", 8, "Vũng Tàu"),
    "vung tau": ("vung-tau-ba-ria-vung-tau", 8, "Vũng Tàu"),
    # Hạ Long
    "hạ long": ("ha-long-quang-ninh", 9, "Hạ Long"),
    "ha long": ("ha-long-quang-ninh", 9, "Hạ Long"),
    # Huế
    "huế": ("hue-thua-thien-hue", 10, "Huế"),
    "hue": ("hue-thua-thien-hue", 10, "Huế"),
    # Sa Pa
    "sa pa": ("sa-pa-lao-cai", 11, "Sa Pa"),
    "sapa": ("sa-pa-lao-cai", 11, "Sa Pa"),
    # Cần Thơ
    "cần thơ": ("can-tho", 12, "Cần Thơ"),
    "can tho": ("can-tho", 12, "Cần Thơ"),
    # Phan Thiết / Mũi Né
    "phan thiết": ("phan-thiet-binh-thuan", 13, "Phan Thiết"),
    "mũi né": ("mui-ne-binh-thuan", 14, "Mũi Né"),
    # Quy Nhơn
    "quy nhơn": ("quy-nhon-binh-dinh", 15, "Quy Nhơn"),
    # Châu Đốc
    "châu đốc": ("chau-doc-an-giang", 17162, "Châu Đốc"),
    "chau doc": ("chau-doc-an-giang", 17162, "Châu Đốc"),
    # Ninh Bình
    "ninh bình": ("ninh-binh", 16, "Ninh Bình"),
    "ninh binh": ("ninh-binh", 16, "Ninh Bình"),
    # Hải Phòng
    "hải phòng": ("hai-phong", 17, "Hải Phòng"),
    "hai phong": ("hai-phong", 17, "Hải Phòng"),
}

API_URL = "https://api2.travel.com.vn/core/Hotel/search-hotel"
PAGE_SIZE = 20
# Khi API không trả gross / thuế tách → hệ số dự phòng cho cột "đã gồm thuế phí".
TRAVEL_TAX_FEE_RATE = 0.15


def _ensure_windows_proactor_policy() -> None:
    """
    Playwright requires subprocess support; force Proactor loop policy on Windows.
    Some hosts set Selector policy, which raises NotImplementedError for subprocess.
    """
    if sys.platform.startswith("win") and hasattr(asyncio, "WindowsProactorEventLoopPolicy"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())


def resolve_travel_city(destination: str) -> tuple[str, int, str] | None:
    """Return (slug, city_id, display_name) or None."""
    key = destination.strip().lower()
    if key in CITY_MAP:
        return CITY_MAP[key]
    for k, v in CITY_MAP.items():
        if key in k or k in key:
            return v
    return None


def build_travel_url(city_slug: str, city_id: int, city_name: str,
                     check_in: str, check_out: str,
                     rooms: int = 1, adults: int = 2, children: int = 0) -> str:
    """Build a travel.com.vn hotel listing URL. check_in / check_out: DD-MM-YYYY"""
    encoded_name = quote(city_name)
    return (
        f"https://travel.com.vn/hotels/khach-san-tai-{city_slug}.aspx"
        f"?room={rooms}&in={check_in}&out={check_out}"
        f"&adults={adults}&children={children}"
        f"&hid={city_id}&cid={city_id}&htitle={encoded_name}"
    )


def get_chromium() -> str | None:
    for name in ["chromium-browser", "chromium", "google-chrome"]:
        p = shutil.which(name)
        if p:
            return p
    return None


# ---------------------------------------------------------------------------
# Parse URL params to build the POST payload
# ---------------------------------------------------------------------------

def _url_to_payload(url: str, city_name_no_sign: str = "", city_title: str = "",
                    page_index: int = 1) -> dict:
    """Convert a travel.com.vn search URL into the API POST payload."""
    parsed = urlparse(url)
    qs = parse_qs(parsed.query, keep_blank_values=True)

    def q(key, default=""):
        v = qs.get(key)
        return v[0] if v else default

    city_id = q("hid") or q("cid") or ""
    check_in = q("in", "")
    check_out = q("out", "")
    rooms = int(q("room", "1") or 1)
    adults = int(q("adults", "2") or 2)
    children = int(q("children", "0") or 0)
    htitle = q("htitle", city_title)

    # Derive cityNameNoSign from URL path: /hotels/khach-san-tai-{slug}.aspx
    slug = ""
    path_parts = parsed.path.rstrip("/").split("/")
    for part in reversed(path_parts):
        if part.startswith("khach-san-tai-"):
            slug = part.replace("khach-san-tai-", "").replace(".aspx", "")
            break

    if not city_name_no_sign:
        city_name_no_sign = slug or "unknown"

    return {
        "cityId": str(city_id),
        "cityNameNoSign": city_name_no_sign,
        "hotelId": str(city_id),
        "hotelTitle": htitle,
        "propertyIds": [str(city_id), str(city_id)],
        "checkIn": check_in,
        "checkOut": check_out,
        "rooms": rooms,
        "adults": adults,
        "children": children,
        "childrenAges": [],
        "pageIndex": page_index,
        "pageSize": PAGE_SIZE,
        "accommodationTypes": [],
        "facilities": [],
    }


# ---------------------------------------------------------------------------
# Format hotel record
# ---------------------------------------------------------------------------

def _fmt_vnd(price) -> str:
    if price is None:
        return ""
    try:
        v = float(price)
        if v <= 0:
            return ""
        return f"{v:,.0f} VND".replace(",", ".")
    except Exception:
        return str(price)


def _price_with_tax_fee(price):
    try:
        v = float(price)
        if v <= 0:
            return None
        return round(v * (1 + TRAVEL_TAX_FEE_RATE))
    except Exception:
        return None


def _travel_positive_float(val) -> float | None:
    try:
        x = float(val)
        return x if x > 0 else None
    except (TypeError, ValueError):
        return None


def _travel_ci_map(d: dict) -> dict[str, object]:
    return {str(k).lower().replace("_", ""): v for k, v in d.items()}


def _travel_price_chunks(hotel: dict) -> list[dict]:
    """Các dict con có thể chứa gross / thuế (tuỳ payload search-hotel)."""
    out: list[dict] = [hotel]
    for nk in (
        "pricing",
        "lowestRoom",
        "lowestRoomInfo",
        "room",
        "roomInfo",
        "rate",
        "rates",
        "hotelPrice",
    ):
        sub = hotel.get(nk)
        if isinstance(sub, dict):
            out.append(sub)
        elif isinstance(sub, list):
            for it in sub[:5]:
                if isinstance(it, dict):
                    out.append(it)
    return out


def _travel_resolve_gross_price(hotel: dict) -> tuple[float | None, float | None, str]:
    """
    Trả về (giá đêm hiển thị đã gồm thuế/phí, giá gốc price, ghi chú nguồn).

    Thứ tự: (1) field tổng/gross rõ ràng từ API; (2) price + các khoản tax/fee tách;
    (3) price × (1 + TRAVEL_TAX_FEE_RATE).

    Payload thực tế chỉ có thể xác nhận khi bắt JSON (JWT). Các tên khóa dưới đây
    là dự đoán phổ biến — bổ sung khi dump được mẫu từ api2.travel.com.vn.
    """
    base = _travel_positive_float(hotel.get("price"))
    chunks = _travel_price_chunks(hotel)

    gross_keys = (
        "grossprice",
        "totalprice",
        "finalprice",
        "totalamount",
        "grandtotal",
        "priceincludedvat",
        "priceincludetax",
        "pricewithtax",
        "amountaftertax",
        "sellingprice",
        "displaytotal",
    )
    for ch in chunks:
        cmap = _travel_ci_map(ch)
        for gk in gross_keys:
            gv = _travel_positive_float(cmap.get(gk))
            if gv is None:
                continue
            if base is None:
                return round(gv), base, f"từ API ({gk})"
            if gv > base * 1.005:
                return round(gv), base, f"từ API ({gk})"

    tax_keys = (
        "tax",
        "taxamount",
        "taxvalue",
        "vat",
        "vatamount",
        "valueaddedtax",
        "servicefee",
        "servicecharge",
        "processingfee",
        "totalfee",
        "fees",
        "surcharge",
    )
    for ch in chunks:
        cmap = _travel_ci_map(ch)
        extra = 0.0
        for tk in tax_keys:
            tv = _travel_positive_float(cmap.get(tk))
            if tv is not None:
                extra += tv
        if base is not None and extra > 0:
            return round(base + extra), base, "từ API (price + thuế/phí)"

    if base is not None:
        est = _price_with_tax_fee(base)
        return est, base, f"ước tính {int(TRAVEL_TAX_FEE_RATE * 100)}%"

    return None, None, ""


def _fmt_stars(rating) -> str:
    if rating is None:
        return ""
    try:
        v = float(rating)
        if v <= 0:
            return ""
        return f"{v:g} sao"
    except Exception:
        return str(rating)


def _fmt_score(score) -> str:
    if score is None:
        return ""
    try:
        v = float(score)
        if v <= 0:
            return ""
        return f"{v}/10"
    except Exception:
        return str(score)


def _fmt_coord_travel(raw) -> str:
    if raw is None:
        return ""
    try:
        x = float(raw)
        s = f"{x:.8f}".rstrip("0").rstrip(".")
        return s
    except (TypeError, ValueError):
        return str(raw).strip()


def _lat_lng_from_block(block: dict) -> tuple[str, str]:
    if not isinstance(block, dict):
        return "", ""
    pairs = (
        ("latitude", "longitude"),
        ("lat", "lng"),
        ("geoLatitude", "geoLongitude"),
        ("Latitude", "Longitude"),
    )
    for a, b in pairs:
        if block.get(a) is not None and block.get(b) is not None:
            la, lo = _fmt_coord_travel(block.get(a)), _fmt_coord_travel(block.get(b))
            if la and lo:
                return la, lo
    return "", ""


def _travel_lat_lng(hotel: dict) -> tuple[str, str]:
    la, lo = _lat_lng_from_block(hotel)
    if la and lo:
        return la, lo
    for nest_key in ("location", "geoLocation", "coordinates", "mapLocation", "geo", "position"):
        block = hotel.get(nest_key)
        la, lo = _lat_lng_from_block(block)
        if la and lo:
            return la, lo
    return "", ""


def _travel_agoda_property_id(hotel: dict) -> str:
    for k in (
        "agodaPropertyId",
        "agodaHotelId",
        "agodaId",
        "sourceAgodaId",
        "agodaPropertyID",
        "AgodaPropertyId",
    ):
        v = hotel.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        if s.isdigit():
            return s
        m = re.search(r"(\d{4,})", s)
        if m:
            return m.group(1)
    hid = str(hotel.get("hotelId") or "")
    m = re.match(r"(?i)^AGODA[_\-]?(\d+)$", hid)
    if m:
        return m.group(1)
    return ""


def _travel_hotel_id_raw(hotel: dict) -> str:
    return str(hotel.get("hotelId") or "").strip()


def _hotel_url(hotel: dict, city_slug: str = "") -> str:
    """Build a best-guess hotel detail URL."""
    hid = hotel.get("hotelId", "")
    name = hotel.get("hotelName", "")
    # Strip prefix like AGODA_, BOOKING_
    raw_id = re.sub(r'^[A-Z_]+_', '', str(hid))
    # Slugify name
    slug = re.sub(r'[^a-z0-9]+', '-', name.lower()).strip('-')
    if raw_id:
        return f"https://travel.com.vn/hotels/chi-tiet-{slug}-{raw_id}.aspx"
    return f"https://travel.com.vn/hotels/"


def _parse_hotel(hotel: dict, destination: str, city_slug: str) -> dict:
    base_price = hotel.get("price")
    gross_price, _, price_source_note = _travel_resolve_gross_price(hotel)
    agoda_pid = _travel_agoda_property_id(hotel)
    lat_s, lng_s = _travel_lat_lng(hotel)
    travel_hid = _travel_hotel_id_raw(hotel)
    return {
        "Tên khách sạn": hotel.get("hotelName", ""),
        "Link khách sạn": _hotel_url(hotel, city_slug),
        "Địa chỉ": (hotel.get("address") or "").strip(),
        "Mã Property Agoda": agoda_pid,
        "ID khách sạn Travel": travel_hid,
        "Mã property (OTA)": "",
        "Vĩ độ": lat_s,
        "Kinh độ": lng_s,
        "Hạng sao": _fmt_stars(hotel.get("starRating")),
        "Loại": hotel.get("typeTitle", ""),
        "Điểm đánh giá": _fmt_score(hotel.get("reviewScore")),
        "Số đánh giá": str(int(hotel.get("reviewCount") or 0)) if hotel.get("reviewCount") else "",
        "Giá/đêm (chưa gồm thuế phí)": _fmt_vnd(base_price),
        "Giá/đêm (VND)": _fmt_vnd(gross_price if gross_price is not None else base_price),
        "Thuế phí ước tính": price_source_note or f"{int(TRAVEL_TAX_FEE_RATE * 100)}%",
        "Chính sách hoàn hủy": "",
        "Nguồn": "travel.com.vn",
        "Điểm đến": destination,
    }


# ---------------------------------------------------------------------------
# JWT capture via Playwright (one-time page load)
# ---------------------------------------------------------------------------

async def _capture_jwt(url: str, chromium: str | None, status_callback) -> tuple[str, str, dict] | None:
    """
    Open the search URL once, capture:
      - JWT token from Authorization header
      - ClientId header
      - First API response (to save a round-trip)
    Returns (jwt, client_id, first_response_json) or None on failure.
    """
    # Some launch args can crash Chromium on Windows. Try stable profiles in order.
    launch_profiles = [
        ["--disable-blink-features=AutomationControlled"],
        ["--disable-gpu", "--disable-blink-features=AutomationControlled"],
    ]
    if not sys.platform.startswith("win"):
        launch_profiles.append([
            "--no-sandbox", "--disable-setuid-sandbox",
            "--disable-dev-shm-usage", "--disable-gpu",
            "--disable-blink-features=AutomationControlled",
        ])

    for attempt, launch_args in enumerate(launch_profiles, start=1):
        captured: dict = {}
        async with async_playwright() as pw:
            launch_kwargs = {"headless": True, "args": launch_args}
            if chromium:
                launch_kwargs["executable_path"] = chromium

            browser = None
            try:
                browser = await pw.chromium.launch(**launch_kwargs)
                ctx = await browser.new_context(
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/122.0.0.0 Safari/537.36"
                    ),
                    viewport={"width": 1366, "height": 768},
                    locale="vi-VN",
                    extra_http_headers={"Accept-Language": "vi-VN,vi;q=0.9"},
                )
                page = await ctx.new_page()
                await page.add_init_script(
                    "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
                )

                async def on_response(resp):
                    if "search-hotel" in resp.url and "jwt" not in captured:
                        h = dict(resp.request.headers)
                        captured["jwt"] = h.get("authorization", "")
                        captured["client_id"] = h.get("clientid", "")
                        captured["req_body"] = resp.request.post_data or ""
                        try:
                            captured["resp_json"] = await resp.json()
                        except Exception:
                            pass

                page.on("response", on_response)

                status_callback("🌐 Đang mở travel.com.vn để lấy token xác thực...")
                try:
                    await page.goto(url, wait_until="networkidle", timeout=40000)
                except PlaywrightTimeoutError:
                    await page.goto(url, wait_until="domcontentloaded", timeout=25000)

                # Wait for API response (up to 10s)
                for _ in range(20):
                    if "jwt" in captured:
                        break
                    await asyncio.sleep(0.5)
            except Exception as e:
                status_callback(f"⚠️ Chromium thử lần {attempt} lỗi: {type(e).__name__}")
            finally:
                if browser:
                    await browser.close()

        if "jwt" in captured and captured["jwt"]:
            return captured["jwt"], captured["client_id"], captured.get("resp_json", {})

    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_scrape_travel(
    url: str,
    destination: str,
    status_callback=None,
) -> list[dict]:
    if status_callback is None:
        status_callback = lambda _: None
    _ensure_windows_proactor_policy()
    ensure_playwright_chromium(status_callback)
    return asyncio.run(_scrape_async(url, destination, status_callback))


async def _scrape_async(url: str, destination: str, status_callback) -> list[dict]:
    chromium = get_chromium()
    if not chromium:
        status_callback("ℹ️ Không thấy Chromium hệ thống, dùng Chromium của Playwright.")

    # Step 1: Capture JWT via Playwright
    result = await _capture_jwt(url, chromium, status_callback)
    if not result:
        raise RuntimeError("Không thể lấy token xác thực từ travel.com.vn. Hãy thử lại.")

    jwt, client_id, first_resp = result
    status_callback(f"✅ Đã lấy được token xác thực")
    status_callback(
        "ℹ️ Giá đã gồm thuế/phí: ưu tiên field gross hoặc price+thuế từ API nếu có; "
        f"không thì ×{1 + TRAVEL_TAX_FEE_RATE:.2f} ({int(TRAVEL_TAX_FEE_RATE * 100)}% ước tính)."
    )

    # Derive city info from URL
    parsed_url = urlparse(url)
    slug = ""
    for part in reversed(parsed_url.path.rstrip("/").split("/")):
        if part.startswith("khach-san-tai-"):
            slug = part.replace("khach-san-tai-", "").replace(".aspx", "")
            break

    # HTTP session for all subsequent calls
    sess = _requests.Session()
    sess.headers.update({
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Origin": "https://travel.com.vn",
        "Referer": "https://travel.com.vn/",
        "Authorization": jwt,
        "ClientId": client_id,
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "vi-VN,vi;q=0.9",
    })

    results: list[dict] = []
    seen: set[str] = set()

    def _add(hotels: list[dict], city_slug: str, page_idx: int = 0) -> int:
        added = 0
        for i, h in enumerate(hotels):
            if not isinstance(h, dict):
                continue
            hid = str(h.get("hotelId") or "").strip()
            nm = str(h.get("hotelName") or "").strip().lower()
            addr = str(h.get("address") or "").strip().lower()[:56]
            if hid:
                key = f"id:{hid}"
            elif nm:
                key = f"n:{nm}|{addr}"
            else:
                key = f"p{page_idx}:i{i}:{addr}"
            if key not in seen:
                seen.add(key)
                results.append(_parse_hotel(h, destination, city_slug))
                added += 1
        return added

    def _extract_hotels(resp_json: dict) -> list[dict]:
        resp = resp_json.get("response") or {}
        if isinstance(resp, dict):
            rh = resp.get("resultHotels") or []
            return rh if isinstance(rh, list) else []
        if isinstance(resp, list):
            return resp
        return []

    def _travel_totals(resp_json: dict) -> tuple[int, int]:
        """
        totalRecord / totalPage thường nằm trong response (không phải root).
        Trả về (total_record, total_page).
        """
        if not isinstance(resp_json, dict):
            return 0, 0
        inner = resp_json.get("response")
        tr = int(resp_json.get("totalRecord") or resp_json.get("totalRecords") or 0)
        tp = int(resp_json.get("totalPage") or resp_json.get("totalPages") or 0)
        if isinstance(inner, dict):
            tr = int(inner.get("totalRecord") or inner.get("totalRecords") or tr or 0)
            tp = int(inner.get("totalPage") or inner.get("totalPages") or tp or 0)
        return tr, tp

    # Step 2: Process first response already captured
    base_payload = _url_to_payload(url, city_name_no_sign=slug, city_title=destination, page_index=1)
    total_record, total_pages_api = _travel_totals(first_resp)
    hotels_p1 = _extract_hotels(first_resp)
    added = _add(hotels_p1, slug, page_idx=1)
    status_callback(f"📄 Trang 1: {len(hotels_p1)} khách sạn, totalRecord≈{total_record}")

    if total_pages_api > 0:
        total_pages = total_pages_api
    elif total_record > 0:
        total_pages = max(1, (total_record + PAGE_SIZE - 1) // PAGE_SIZE)
    else:
        total_pages = 1

    if total_record > 0 and total_pages * PAGE_SIZE < total_record:
        total_pages = max(total_pages, (total_record + PAGE_SIZE - 1) // PAGE_SIZE)

    need_pages = (
        max(1, (total_record + PAGE_SIZE - 1) // PAGE_SIZE) if total_record > 0 else max(1, total_pages)
    )
    max_pages = min(max(total_pages, need_pages) + 25, 500)
    status_callback(
        f"📊 API: ~{total_record} KS, {total_pages} trang — lật tối đa {max_pages} trang"
    )

    # Step 3: Pages 2+
    consecutive_empty = 0
    pg = 2
    while pg <= max_pages:
        if len(results) >= 2000:
            status_callback("⚠️ Đạt giới hạn 2000.")
            break

        # Không dừng khi len >= totalRecord: API đôi khi trả totalRecord nhỏ/sai
        # (vd. 51) khiến chỉ gom được một phần — chỉ dùng totalRecord để ước lượng max_pages.

        if total_record <= 0 and pg > total_pages and consecutive_empty >= 4:
            break

        status_callback(f"📄 Trang {pg}/{total_pages} (mục tiêu ~{total_record or '?'})...")
        payload = dict(base_payload)
        payload["pageIndex"] = pg

        try:
            r = sess.post(API_URL, json=payload, timeout=35)
            r.raise_for_status()
            resp_json = r.json()
        except Exception as e:
            status_callback(f"  ⚠️ Lỗi trang {pg}: {e}")
            consecutive_empty += 1
            if consecutive_empty >= 5:
                break
            time.sleep(0.8)
            pg += 1
            continue

        tr2, tp2 = _travel_totals(resp_json)
        if tr2 > total_record:
            total_record = tr2
        if tp2 > total_pages:
            total_pages = tp2
            max_pages = min(max(max_pages, total_pages + 25), 500)

        hotels_pg = _extract_hotels(resp_json)
        added = _add(hotels_pg, slug, page_idx=pg)
        status_callback(f"  → +{added} mới (tổng: {len(results)})")

        if added > 0:
            consecutive_empty = 0
        else:
            consecutive_empty += 1
            if total_record > 0 and len(results) < total_record and pg <= total_pages + 2:
                status_callback("  ℹ️ Trang không thêm KS mới nhưng chưa đủ totalRecord — tiếp tục.")
            elif consecutive_empty >= 5:
                status_callback("⚠️ Nhiều trang liên tiếp không có KS mới, dừng.")
                break

        time.sleep(0.35)
        pg += 1

    status_callback(f"✅ Hoàn thành: {len(results)} khách sạn từ travel.com.vn")
    return results
