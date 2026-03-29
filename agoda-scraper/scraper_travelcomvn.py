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
# Travel.com.vn list API does not expose explicit tax/fee fields.
# Use a configurable estimate for gross price to align with "đã gồm thuế phí" display.
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
    gross_price = _price_with_tax_fee(base_price)
    return {
        "Tên khách sạn": hotel.get("hotelName", ""),
        "Link khách sạn": _hotel_url(hotel, city_slug),
        "Địa chỉ": (hotel.get("address") or "").strip(),
        "Hạng sao": _fmt_stars(hotel.get("starRating")),
        "Loại": hotel.get("typeTitle", ""),
        "Điểm đánh giá": _fmt_score(hotel.get("reviewScore")),
        "Số đánh giá": str(int(hotel.get("reviewCount") or 0)) if hotel.get("reviewCount") else "",
        "Giá/đêm (chưa gồm thuế phí)": _fmt_vnd(base_price),
        "Giá/đêm (VND)": _fmt_vnd(gross_price if gross_price is not None else base_price),
        "Thuế phí ước tính": f"{int(TRAVEL_TAX_FEE_RATE * 100)}%",
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
    status_callback("ℹ️ Giá Travel.com.vn được quy đổi sang đã gồm thuế phí theo mức ước tính 15%.")

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

    def _add(hotels: list[dict], city_slug: str) -> int:
        added = 0
        for h in hotels:
            key = (h.get("hotelId") or h.get("hotelName") or "").lower().strip()
            if key and key not in seen:
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

    # Step 2: Process first response already captured
    base_payload = _url_to_payload(url, city_name_no_sign=slug, city_title=destination, page_index=1)
    total_record = first_resp.get("totalRecord") or 0
    hotels_p1 = _extract_hotels(first_resp)
    added = _add(hotels_p1, slug)
    status_callback(f"📄 Trang 1: {len(hotels_p1)} khách sạn, tổng={total_record}")

    # Determine total pages
    total_pages_raw = first_resp.get("totalPage")
    if total_pages_raw:
        total_pages = int(total_pages_raw)
    elif total_record > 0:
        total_pages = max(1, (total_record + PAGE_SIZE - 1) // PAGE_SIZE)
    else:
        total_pages = 1

    status_callback(f"📊 Tổng {total_record} khách sạn — {total_pages} trang")

    # Step 3: Pages 2+
    consecutive_empty = 0
    for pg in range(2, min(total_pages + 1, 101)):
        if len(results) >= 2000:
            status_callback("⚠️ Đạt giới hạn 2000.")
            break

        status_callback(f"📄 Trang {pg}/{total_pages}...")
        payload = dict(base_payload)
        payload["pageIndex"] = pg

        try:
            r = sess.post(API_URL, json=payload, timeout=15)
            r.raise_for_status()
            resp_json = r.json()
        except Exception as e:
            status_callback(f"  ⚠️ Lỗi trang {pg}: {e}")
            consecutive_empty += 1
            if consecutive_empty >= 3:
                break
            continue

        hotels_pg = _extract_hotels(resp_json)
        added = _add(hotels_pg, slug)
        status_callback(f"  → +{added} mới (tổng: {len(results)})")

        if added > 0:
            consecutive_empty = 0
        else:
            consecutive_empty += 1
            if consecutive_empty >= 3:
                status_callback("⚠️ 3 trang liên tiếp không có kết quả mới, dừng.")
                break

        time.sleep(0.3)

    status_callback(f"✅ Hoàn thành: {len(results)} khách sạn từ travel.com.vn")
    return results
