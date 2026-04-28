"""
Mytour.vn hotel scraper.

Strategy:
1. Load mytour.vn to capture the live `apphash` header from real API requests.
2. Use that apphash + hard-coded province IDs to call the availability API
   directly from the page context (page.evaluate) — bypasses IP block.
3. Paginate (page=1,2,3...) until all hotels are collected.
4. Returns rich hotel data: name, address, stars, rating, price (VND), link.

Province ID mapping was discovered by probing the tripi.vn API.
"""

import asyncio
import re
import shutil
import sys
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from playwright_bootstrap import ensure_playwright_chromium

from geo_extract import scan_json_for_latlng

# ---------------------------------------------------------------------------
# Province ID mapping (Mytour/Tripi internal IDs, verified by API probe)
# ---------------------------------------------------------------------------
PROVINCE_IDS: dict[str, int] = {
    "thừa thiên - huế": 1,
    "thừa thiên huế": 1,
    "huế": 1,
    "kiên giang": 2,
    "phú quốc": 2,
    "hải phòng": 3,
    "gia lai": 4,
    "bình định": 5,
    "quy nhơn": 5,
    "an giang": 6,
    "nghệ an": 7,
    "hưng yên": 8,
    "bắc kạn": 9,
    "quảng ninh": 10,
    "hạ long": 10,
    "hà nội": 11,
    "quảng bình": 12,
    "quảng ngãi": 13,
    "bến tre": 14,
    "bà rịa - vũng tàu": 15,
    "bà rịa vũng tàu": 15,
    "vũng tàu": 15,
    "thanh hóa": 16,
    "ninh thuận": 17,
    "bạc liêu": 18,
    "cao bằng": 19,
    "lâm đồng": 20,
    "đà lạt": 20,
    "lào cai": 21,
    "sa pa": 21,
    "sapa": 21,
    "bình dương": 22,
    "bình thuận": 23,
    "phan thiết": 23,
    "mũi né": 23,
    "bắc giang": 24,
    "hà giang": 25,
    "bắc ninh": 26,
    "lạng sơn": 27,
    "quảng nam": 28,
    "hội an": 28,
    "sơn la": 29,
    "tây ninh": 30,
    "long an": 31,
    "đồng nai": 32,
    "hồ chí minh": 33,
    "tp. hồ chí minh": 33,
    "hcm": 33,
    "sài gòn": 33,
    "hải dương": 34,
    "bình phước": 35,
    "hà nam": 36,
    "vĩnh long": 37,
    "cần thơ": 38,
    "khánh hòa": 43,
    "nha trang": 43,
    "đà nẵng": 50,
    "ninh bình": None,  # not found yet, fallback to q-search
}

# ---------------------------------------------------------------------------
# City slug → (province_id, display_province_name) mapping
# ---------------------------------------------------------------------------
CITY_MAP: dict[str, tuple[int | None, str]] = {
    # Format: (province_id, province_display_name)
    "hà nội": (11, "Hà Nội"),
    "ha noi": (11, "Hà Nội"),
    "hanoi": (11, "Hà Nội"),
    "hồ chí minh": (33, "Hồ Chí Minh"),
    "ho chi minh": (33, "Hồ Chí Minh"),
    "tp. hồ chí minh": (33, "Hồ Chí Minh"),
    "hcm": (33, "Hồ Chí Minh"),
    "sài gòn": (33, "Hồ Chí Minh"),
    "saigon": (33, "Hồ Chí Minh"),
    "đà nẵng": (50, "Đà Nẵng"),
    "da nang": (50, "Đà Nẵng"),
    "danang": (50, "Đà Nẵng"),
    "nha trang": (43, "Nha Trang"),
    "khánh hòa": (43, "Khánh Hòa"),
    "phú quốc": (2, "Phú Quốc"),
    "phu quoc": (2, "Phú Quốc"),
    "kiên giang": (2, "Kiên Giang"),
    "vũng tàu": (15, "Vũng Tàu"),
    "vung tau": (15, "Vũng Tàu"),
    "hội an": (28, "Hội An"),
    "hoi an": (28, "Hội An"),
    "quảng nam": (28, "Quảng Nam"),
    "huế": (1, "Huế"),
    "hue": (1, "Huế"),
    "thừa thiên huế": (1, "Huế"),
    "hạ long": (10, "Hạ Long"),
    "ha long": (10, "Hạ Long"),
    "halong": (10, "Hạ Long"),
    "quảng ninh": (10, "Quảng Ninh"),
    "sa pa": (21, "Sa Pa"),
    "sapa": (21, "Sa Pa"),
    "lào cai": (21, "Lào Cai"),
    "đà lạt": (20, "Đà Lạt"),
    "da lat": (20, "Đà Lạt"),
    "dalat": (20, "Đà Lạt"),
    "lâm đồng": (20, "Lâm Đồng"),
    "phan thiết": (23, "Phan Thiết"),
    "phan thiet": (23, "Phan Thiết"),
    "mũi né": (23, "Mũi Né"),
    "mui ne": (23, "Mũi Né"),
    "bình thuận": (23, "Bình Thuận"),
    "quy nhơn": (5, "Quy Nhơn"),
    "quy nhon": (5, "Quy Nhơn"),
    "bình định": (5, "Bình Định"),
    "cần thơ": (38, "Cần Thơ"),
    "can tho": (38, "Cần Thơ"),
    "hải phòng": (3, "Hải Phòng"),
    "hai phong": (3, "Hải Phòng"),
    "ninh bình": (None, "Ninh Bình"),
    "ninh binh": (None, "Ninh Bình"),
    "thanh hóa": (16, "Thanh Hóa"),
    "sầm sơn": (16, "Sầm Sơn"),
    "quảng bình": (12, "Quảng Bình"),
    "đồng hới": (12, "Đồng Hới"),
}

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
AVAILABILITY_URL = "https://apis.tripi.vn/hotels/v3/hotels/availability"
PAGE_SIZE = 20  # items per page


def _normalize_mytour_paste_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return u
    if "://mytour.vn" in u and "://www.mytour.vn" not in u:
        u = u.replace("://mytour.vn", "://www.mytour.vn", 1)
    return u


async def _mytour_post_availability(page, apphash: str, referer: str, payload: dict) -> dict:
    """POST JSON tới Tripi availability; payload đã gồm page, provinceId hoặc aliasCode."""
    return await page.evaluate(
        """async ({ url, apphash, referer, payload }) => {
            try {
                const resp = await fetch(url, {
                    method: 'POST',
                    credentials: 'include',
                    headers: {
                        'Content-Type': 'application/json',
                        'appid': 'mytour-web',
                        'deviceinfo': 'PC-Web',
                        'lang': 'vi',
                        'currency': 'VND',
                        'countrycode': 'VN',
                        'caid': '17',
                        'platform': 'website',
                        'apphash': apphash,
                        'version': '1.0',
                        'origin': 'https://www.mytour.vn',
                        'referer': referer,
                    },
                    body: JSON.stringify(payload),
                });
                return await resp.json();
            } catch (e) {
                return { error: String(e.message || e), code: 0 };
            }
        }""",
        {"url": AVAILABILITY_URL, "apphash": apphash, "referer": referer, "payload": payload},
    )


def _ensure_windows_proactor_policy() -> None:
    """
    Playwright requires subprocess support; force Proactor loop policy on Windows.
    Some hosts set Selector policy, which raises NotImplementedError for subprocess.
    """
    if sys.platform.startswith("win") and hasattr(asyncio, "WindowsProactorEventLoopPolicy"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())


def get_chromium_path():
    for name in ["chromium-browser", "chromium", "google-chrome"]:
        p = shutil.which(name)
        if p:
            return p
    return None


def resolve_mytour_city(destination: str) -> tuple[int | None, str] | None:
    """Return (province_id, display_name) or None if city not found."""
    key = destination.strip().lower()
    if key in CITY_MAP:
        return CITY_MAP[key]
    for k, v in CITY_MAP.items():
        if key in k or k in key:
            return v
    return None


def build_mytour_url(city_slug: str, check_in: str, check_out: str,
                     rooms: int = 1, adults: int = 2, children: int = 0) -> str:
    """Build Mytour hotel search URL (used for display purposes)."""
    return (
        f"https://www.mytour.vn/khach-san?q={city_slug}"
        f"&checkIn={check_in}&checkOut={check_out}"
        f"&rooms={rooms}&adults={adults}&children={children}"
    )


# ---------------------------------------------------------------------------
# Hotel data extraction
# ---------------------------------------------------------------------------

def _mytour_coord_str(v) -> str:
    if v is None:
        return ""
    try:
        x = float(v)
        return f"{x:.8f}".rstrip("0").rstrip(".")
    except (TypeError, ValueError):
        return ""


def _mytour_lat_lng(item: dict) -> tuple[str, str]:
    if not isinstance(item, dict):
        return "", ""
    la = item.get("latitude") or item.get("lat")
    lo = item.get("longitude") or item.get("lng")
    lat_s, lng_s = _mytour_coord_str(la), _mytour_coord_str(lo)
    if lat_s and lng_s:
        return lat_s, lng_s
    addr = item.get("address") or {}
    if isinstance(addr, dict):
        la = addr.get("latitude") or addr.get("lat")
        lo = addr.get("longitude") or addr.get("lng")
        lat_s, lng_s = _mytour_coord_str(la), _mytour_coord_str(lo)
        if lat_s and lng_s:
            return lat_s, lng_s
    if not lat_s or not lng_s:
        lat_s, lng_s = scan_json_for_latlng(item)
    return lat_s, lng_s


def _extract_hotel(item: dict, destination: str, check_in: str, check_out: str) -> dict:
    hotel_id = item.get("id", "")
    name = item.get("name", "")
    lat_s, lng_s = _mytour_lat_lng(item)

    addr = item.get("address") or {}
    street = addr.get("streetName") or addr.get("address") or ""
    district = addr.get("districtName") or ""
    province = addr.get("provinceName") or ""
    address_parts = [p for p in [street, district, province] if p]
    address = ", ".join(address_parts)

    stars = item.get("starNumber")
    stars_str = f"{stars} sao" if stars else ""

    rating = item.get("ratingLevel") or item.get("taRating")
    rating_str = f"{rating}/5" if rating else ""

    num_reviews = item.get("numberOfReviews") or 0
    reviews_str = f"{num_reviews:,}" if num_reviews else ""

    price = item.get("price") or item.get("basePrice")
    if price and int(price) > 0:
        price_str = f"{int(price):,} VND"
    else:
        price_str = ""

    # Build hotel link
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower().strip()).strip("-")
    link = (f"https://www.mytour.vn/khach-san/{hotel_id}-{slug}.html"
            f"?checkIn={check_in}&checkOut={check_out}") if hotel_id else ""

    # Cancellation policy from tags/promotions
    cancel_policy = ""
    tags = item.get("tags") or item.get("promotions") or []
    if isinstance(tags, list):
        for t in tags:
            name_tag = (t.get("name") or "") if isinstance(t, dict) else str(t)
            if "hủy" in name_tag.lower() or "hoàn" in name_tag.lower():
                cancel_policy = name_tag
                break

    hid_str = str(hotel_id).strip() if hotel_id is not None else ""

    return {
        "Tỉnh thành / Điểm đến": destination,
        "Tên khách sạn": name,
        "Địa chỉ": address,
        "Quận/Huyện": district,
        "Tỉnh/Thành": province,
        "Hạng sao": stars_str,
        "Điểm đánh giá": rating_str,
        "Số đánh giá": reviews_str,
        "Gói bữa ăn": "",
        "Chính sách hoàn hủy": cancel_policy,
        "Giá/đêm (VND)": price_str,
        "Link khách sạn": link,
        "Mã Property Agoda": "",
        "ID khách sạn Travel": "",
        "Mã property (OTA)": hid_str,
        "Vĩ độ": lat_s,
        "Kinh độ": lng_s,
    }


# ---------------------------------------------------------------------------
# Async scrape core
# ---------------------------------------------------------------------------

async def _scrape_async(province_id: int, destination: str,
                        check_in: str, check_out: str,
                        rooms: int, adults: int, children: int,
                        status_callback) -> list[dict]:
    chromium = get_chromium_path()
    if not chromium:
        status_callback("ℹ️ Không thấy Chromium hệ thống, dùng Chromium của Playwright.")

    results: list[dict] = []
    seen_ids: set = set()

    async with async_playwright() as pw:
        launch_kwargs = {
            "headless": True,
            "args": ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
        }
        if chromium:
            launch_kwargs["executable_path"] = chromium
        browser = await pw.chromium.launch(**launch_kwargs)
        ctx = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1366, "height": 768},
            locale="vi-VN",
        )
        page = await ctx.new_page()

        # Step 1: Load Mytour to get fresh apphash + session cookies
        status_callback("🌐 Đang lấy phiên đăng nhập từ Mytour.vn...")
        apphash = ""
        apphash_data: dict = {}

        async def on_req(r):
            nonlocal apphash
            if "apis.tripi.vn" in r.url:
                h = r.headers
                if h.get("apphash") and not apphash:
                    apphash = h["apphash"]
                    apphash_data.update(h)

        page.on("request", on_req)

        try:
            await page.goto("https://www.mytour.vn/khach-san",
                            wait_until="networkidle", timeout=40000)
        except PlaywrightTimeoutError:
            await page.goto("https://www.mytour.vn/khach-san",
                            wait_until="domcontentloaded", timeout=30000)

        await asyncio.sleep(4)

        if not apphash:
            status_callback("⚠️ Không lấy được apphash, thử dùng giá trị mặc định...")
            apphash = "LnJCWsNPd7SMjCMm7dw5BlqIeoFiib3iUTjSC6rck6Y="

        status_callback(f"✅ Đã lấy phiên. Bắt đầu thu thập dữ liệu {destination}...")

        # Step 2: Paginate through availability API
        total_pages = 1
        current_page = 1

        while current_page <= total_pages:
            status_callback(f"📄 Đang tải trang {current_page}/{total_pages if total_pages > 1 else '?'}...")

            api_result = await page.evaluate(f"""async () => {{
                try {{
                    const resp = await fetch('{AVAILABILITY_URL}', {{
                        method: 'POST',
                        credentials: 'include',
                        headers: {{
                            'Content-Type': 'application/json',
                            'appid': 'mytour-web',
                            'deviceinfo': 'PC-Web',
                            'lang': 'vi',
                            'currency': 'VND',
                            'countrycode': 'VN',
                            'caid': '17',
                            'platform': 'website',
                            'apphash': '{apphash}',
                            'version': '1.0',
                            'origin': 'https://www.mytour.vn',
                            'referer': 'https://www.mytour.vn/khach-san'
                        }},
                        body: JSON.stringify({{
                            checkIn: '{check_in}',
                            checkOut: '{check_out}',
                            adults: {adults},
                            rooms: {rooms},
                            children: {children},
                            page: {current_page},
                            size: {PAGE_SIZE},
                            useBasePrice: true,
                            provinceId: {province_id}
                        }})
                    }});
                    return await resp.json();
                }} catch(e) {{
                    return {{error: e.message}};
                }}
            }}""")

            if api_result.get("error"):
                status_callback(f"❌ Lỗi API: {api_result['error']}")
                break

            code = api_result.get("code")
            if code == 3005:
                status_callback("⚠️ IP bị chặn tạm thời, dừng phân trang.")
                break
            if code != 200:
                status_callback(f"⚠️ API trả về code={code}, dừng.")
                break

            data = api_result.get("data") or {}
            items = data.get("items") or []
            total = data.get("total") or 0

            if not items:
                break

            # Calculate total pages on first run
            if current_page == 1:
                import math
                total_pages = max(1, math.ceil(total / PAGE_SIZE))
                status_callback(f"📊 Tổng cộng {total} khách sạn, {total_pages} trang")

            new_count = 0
            for item in items:
                hid = item.get("id")
                if hid and hid not in seen_ids:
                    seen_ids.add(hid)
                    results.append(_extract_hotel(item, destination, check_in, check_out))
                    new_count += 1

            status_callback(f"  → Trang {current_page}: +{new_count} khách sạn (tổng: {len(results)})")

            if not data.get("completed", True) and current_page < total_pages:
                # Wait slightly between pages
                await asyncio.sleep(0.8)

            current_page += 1

            # Safety cap: don't scrape more than 2000 hotels
            if len(results) >= 2000:
                status_callback("⚠️ Đã đạt giới hạn 2000 khách sạn, dừng.")
                break

        await browser.close()

    return results


# ---------------------------------------------------------------------------
# Fallback: intercept-based scrape (when province_id is None)
# ---------------------------------------------------------------------------

async def _scrape_intercept_async(direct_url: str, destination: str,
                                   check_in: str, check_out: str,
                                   province_fragments: list[str],
                                   status_callback) -> list[dict]:
    """
    Paste-URL mode:
    1. Navigate to the pasted URL to capture apphash (from request headers)
       and province_id (from first API response item).
    2. Parse guest params (adults, rooms, children, dates) from the URL.
    3. Then paginate through ALL pages via direct API calls (same as _scrape_async).
    """
    import json as _json
    import math
    from urllib.parse import urlparse, parse_qs

    chromium = get_chromium_path()
    if not chromium:
        status_callback("ℹ️ Không thấy Chromium hệ thống, dùng Chromium của Playwright.")

    direct_url = _normalize_mytour_paste_url(direct_url)

    # Parse guest params + dates from the pasted URL
    url_alias = ""
    traveller_type = None
    try:
        parsed_url = urlparse(direct_url)
        params = parse_qs(parsed_url.query)
        url_checkin = params.get("checkIn", [""])[0]
        url_checkout = params.get("checkOut", [""])[0]
        url_adults = int(params.get("adults", ["2"])[0])
        url_rooms = int(params.get("rooms", ["1"])[0])
        url_children = int(params.get("children", ["0"])[0])
        url_alias = (params.get("aliasCode", [""])[0] or "").strip()
        _tt = (params.get("travellerType", [""])[0] or "").strip()
        if _tt.isdigit():
            traveller_type = int(_tt)
    except Exception:
        url_checkin = check_in
        url_checkout = check_out
        url_adults, url_rooms, url_children = 2, 1, 0

    # Prefer dates from URL (Mytour uses DD-MM-YYYY)
    effective_checkin = url_checkin or check_in
    effective_checkout = url_checkout or check_out

    discovered: dict = {}  # apphash, province_id, alias_code, total

    async with async_playwright() as pw:
        launch_kwargs = {
            "headless": True,
            "args": ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
        }
        if chromium:
            launch_kwargs["executable_path"] = chromium
        browser = await pw.chromium.launch(**launch_kwargs)
        ctx = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1366, "height": 768},
            locale="vi-VN",
        )
        page = await ctx.new_page()

        async def on_request(request):
            if "apis.tripi.vn" in request.url:
                h = request.headers
                if h.get("apphash") and "apphash" not in discovered:
                    discovered["apphash"] = h["apphash"]
                try:
                    body = request.post_data or ""
                    if body:
                        payload = _json.loads(body)
                        if "province_id" not in discovered:
                            pid = payload.get("provinceId") or payload.get("province_id")
                            if pid:
                                discovered["province_id"] = int(pid)
                        ac = payload.get("aliasCode") or payload.get("alias_code")
                        if ac and not discovered.get("alias_code"):
                            discovered["alias_code"] = str(ac).strip()
                except Exception:
                    pass

        async def on_response(response):
            if AVAILABILITY_URL in response.url and response.status == 200 and "province_id" not in discovered:
                try:
                    body = await response.text()
                    data = _json.loads(body)
                    d = data.get("data") or {}
                    items = d.get("items") or []
                    total = d.get("total") or 0
                    if items:
                        addr = (items[0].get("address") or {})
                        pid = addr.get("provinceId") or items[0].get("provinceId")
                        if pid:
                            discovered["province_id"] = int(pid)
                            discovered["total"] = total
                except Exception:
                    pass

        page.on("request", on_request)
        page.on("response", on_response)

        status_callback("🌐 Đang mở trang Mytour để phát hiện tỉnh thành và apphash...")
        try:
            await page.goto(direct_url, wait_until="networkidle", timeout=50000)
        except PlaywrightTimeoutError:
            await page.goto(direct_url, wait_until="domcontentloaded", timeout=30000)

        await asyncio.sleep(5)

        apphash = discovered.get("apphash", "LnJCWsNPd7SMjCMm7dw5BlqIeoFiib3iUTjSC6rck6Y=")
        total_hint = discovered.get("total", 0)
        # URL có aliasCode → luôn gọi API theo alias (vd khu td104), không dùng provinceId bắt được từ request khác.
        if url_alias:
            alias_code = url_alias
            province_id = None
        else:
            province_id = discovered.get("province_id")
            alias_code = (discovered.get("alias_code") or "").strip()

        if province_id is None and not alias_code:
            status_callback(
                "❌ Không phát hiện province_id hoặc aliasCode. "
                "Với URL /khach-san/search?aliasCode=... hãy giữ nguyên aliasCode trên URL; hoặc dùng tab cấu hình theo tỉnh/thành."
            )
            await browser.close()
            return []

        if province_id is not None:
            status_callback(f"✅ Phát hiện province_id={province_id}, tổng ~{total_hint} KS. Thu thập…")
        else:
            status_callback(f"✅ Phát hiện aliasCode={alias_code}, tổng ~{total_hint or '?'} KS. Thu thập…")

        # --- Paginate via direct API calls ---
        results: list[dict] = []
        seen_ids: set = set()
        total_pages = max(1, math.ceil(total_hint / PAGE_SIZE)) if total_hint else 99
        current_page = 1

        def _build_payload(pg: int) -> dict:
            p: dict = {
                "checkIn": effective_checkin,
                "checkOut": effective_checkout,
                "adults": url_adults,
                "rooms": url_rooms,
                "children": url_children,
                "page": pg,
                "size": PAGE_SIZE,
                "useBasePrice": True,
            }
            if province_id is not None:
                p["provinceId"] = int(province_id)
            else:
                p["aliasCode"] = alias_code
                if traveller_type is not None:
                    p["travellerType"] = traveller_type
            return p

        while current_page <= total_pages:
            status_callback(f"📄 Đang tải trang {current_page}/{total_pages if total_hint else '?'}...")

            api_result = await _mytour_post_availability(
                page, apphash, direct_url, _build_payload(current_page)
            )

            if api_result.get("error"):
                status_callback(f"❌ Lỗi API: {api_result['error']}")
                break

            code = api_result.get("code")
            if code == 3005:
                status_callback("⚠️ IP bị chặn tạm thời, dừng phân trang.")
                break
            if code != 200:
                status_callback(f"⚠️ API trả về code={code}, dừng.")
                break

            data = api_result.get("data") or {}
            items = data.get("items") or []
            total = data.get("total") or 0

            if not items:
                break

            # Recalculate total pages from actual first response
            if current_page == 1 and total:
                total_pages = max(1, math.ceil(total / PAGE_SIZE))
                status_callback(f"📊 Tổng cộng {total} khách sạn, {total_pages} trang")

            new_count = 0
            for item in items:
                hid = item.get("id")
                if hid and hid not in seen_ids:
                    seen_ids.add(hid)
                    results.append(_extract_hotel(item, destination, effective_checkin, effective_checkout))
                    new_count += 1

            status_callback(f"  → Trang {current_page}: +{new_count} khách sạn (tổng: {len(results)})")

            if new_count == 0 or len(items) < PAGE_SIZE:
                break

            await asyncio.sleep(0.8)
            current_page += 1

            if len(results) >= 2000:
                status_callback("⚠️ Đã đạt giới hạn 2000 khách sạn, dừng.")
                break

        await browser.close()

    return results


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_scrape_mytour(url: str, destination: str, check_in: str, check_out: str,
                      province_fragments: list[str] = None,
                      province_id: int = None,
                      city_slug: str = "",
                      rooms: int = 1, adults: int = 2, children: int = 0,
                      status_callback=None) -> list[dict]:
    """
    Main scraping entry point.

    If province_id is provided → uses direct API calls (accurate & full).
    Otherwise → falls back to page-intercept mode (limited to featured hotels).
    """
    if status_callback is None:
        status_callback = print
    _ensure_windows_proactor_policy()
    ensure_playwright_chromium(status_callback)

    if province_id is not None:
        return asyncio.run(_scrape_async(
            province_id=province_id,
            destination=destination,
            check_in=check_in, check_out=check_out,
            rooms=rooms, adults=adults, children=children,
            status_callback=status_callback,
        ))
    else:
        # Use pasted URL directly (or fallback URL)
        direct_url = url or f"https://www.mytour.vn/khach-san?q={city_slug}&checkIn={check_in}&checkOut={check_out}&rooms={rooms}&adults={adults}&children={children}"
        return asyncio.run(_scrape_intercept_async(
            direct_url=direct_url,
            destination=destination,
            check_in=check_in, check_out=check_out,
            province_fragments=province_fragments or [],
            status_callback=status_callback,
        ))
