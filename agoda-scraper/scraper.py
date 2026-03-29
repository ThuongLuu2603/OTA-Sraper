import asyncio
import random
import re
import shutil
import urllib.request
import json
import sys
from datetime import datetime
from urllib.parse import quote
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError


# ---------------------------------------------------------------------------
# Browser / Chromium helpers
# ---------------------------------------------------------------------------

def get_chromium_path() -> str | None:
    for name in ["chromium-browser", "chromium", "google-chrome", "google-chrome-stable"]:
        p = shutil.which(name)
        if p:
            return p
    return None


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
]


def _ensure_windows_proactor_policy() -> None:
    """
    Playwright requires subprocess support; force Proactor loop policy on Windows.
    Some hosts set Selector policy, which raises NotImplementedError for subprocess.
    """
    if sys.platform.startswith("win") and hasattr(asyncio, "WindowsProactorEventLoopPolicy"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())


# ---------------------------------------------------------------------------
# City ID resolution
# ---------------------------------------------------------------------------

AGODA_SEARCH_BASE = "https://www.agoda.com/vi-vn/search"
AGODA_SUGGEST_API = (
    "https://www.agoda.com/api/cronos/search/GetUnifiedSuggestResult/3/24/24/0/vi-vn/"
    "?searchText={query}&guid=abc123&origin=VN&cid=-1&pageTypeId=1"
)


def resolve_city_id(destination: str) -> int:
    """Return Agoda city ID for a destination string (0 if not found)."""
    url = AGODA_SUGGEST_API.format(query=quote(destination))
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 Chrome/122.0.0.0",
            "Referer": "https://www.agoda.com/vi-vn/",
            "Accept": "application/json",
        }
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        items = data.get("ViewModelList", [])
        for item in items:
            if item.get("ObjectTypeId") == 1 and item.get("SearchType") == 1:
                cid = item.get("ObjectId") or item.get("CityId")
                if cid:
                    return cid
        for item in items:
            cid = item.get("ObjectId") or item.get("CityId")
            if cid and cid > 0:
                return cid
    except Exception:
        pass
    return 0


def build_agoda_url(
    destination: str,
    check_in: str,
    check_out: str,
    rooms: int = 1,
    adults: int = 2,
    children: int = 0,
    child_ages: list = None,
) -> str:
    """Build an Agoda search URL from form parameters."""
    if child_ages is None:
        child_ages = []

    check_in_dt = datetime.strptime(check_in, "%Y-%m-%d")
    check_out_dt = datetime.strptime(check_out, "%Y-%m-%d")
    nights = max(1, (check_out_dt - check_in_dt).days)

    city_id = resolve_city_id(destination)
    dest_param = f"city={city_id}" if city_id else f"textToSearch={quote(destination)}"

    params = (
        f"?{dest_param}"
        f"&checkIn={check_in}"
        f"&checkOut={check_out}"
        f"&rooms={rooms}"
        f"&adults={adults}"
        f"&children={children}"
        f"&los={nights}"
        f"&priceCur=VND"
        f"&currency=VND"
        f"&currencyCode=VND"
        f"&productType=-1"
    )
    if children > 0 and child_ages:
        params += f"&childAges={','.join(str(a) for a in child_ages[:children])}"
    return AGODA_SEARCH_BASE + params


# ---------------------------------------------------------------------------
# GraphQL data extraction helpers
# ---------------------------------------------------------------------------

def _safe_get(d, *keys, default=None):
    for k in keys:
        if not isinstance(d, dict):
            return default
        d = d.get(k)
        if d is None:
            return default
    return d if d is not None else default


def _collect_prices(hotel: dict) -> list[dict]:
    """
    Collect all (exclusive, inclusive, currency) price entries from all offers.
    Returns list of dicts sorted by exclusive price ascending (cheapest first).
    """
    entries = []
    try:
        offers = _safe_get(hotel, "pricing", "offers", default=[])
        for offer in offers:
            for ro in offer.get("roomOffers", []):
                for pe in _safe_get(ro, "room", "pricing", default=[]):
                    currency = pe.get("currency", "USD")
                    excl = _safe_get(pe, "price", "perNight", "exclusive", "display")
                    incl = _safe_get(pe, "price", "perNight", "inclusive", "display")
                    if excl is not None:
                        entries.append({"currency": currency, "excl": excl, "incl": incl or excl})
    except Exception:
        pass
    # Sort by exclusive price ascending → cheapest first
    entries.sort(key=lambda x: x["excl"])
    return entries


def _format_price(amount: float, currency: str) -> str:
    """Format a price amount with currency."""
    if currency in ("VND", "JPY", "KRW", "IDR"):
        return f"{amount:,.0f} {currency}"
    return f"{amount:,.2f} {currency}"


def _extract_price(hotel: dict) -> str:
    """Extract cheapest exclusive (before-tax) price per night."""
    entries = _collect_prices(hotel)
    if entries:
        e = entries[0]
        return _format_price(e["excl"], e["currency"])
    return ""


def _extract_price_inclusive(hotel: dict) -> str:
    """Extract cheapest inclusive (with-tax) price per night."""
    entries = _collect_prices(hotel)
    if entries:
        e = entries[0]
        return _format_price(e["incl"], e["currency"])
    return ""


CANCELLATION_TYPE_MAP = {
    "FreeCancellation": "Hủy miễn phí",
    "NonRefundable": "Không hoàn tiền",
    "PartiallyRefundable": "Hoàn tiền một phần",
    "Unknown": "",
}


def _extract_cancellation(hotel: dict) -> str:
    try:
        ct = _safe_get(hotel, "pricing", "payment", "cancellation", "cancellationType")
        if ct:
            label = CANCELLATION_TYPE_MAP.get(ct, ct)
            if ct == "FreeCancellation":
                free_date = _safe_get(
                    hotel, "pricing", "payment", "cancellation", "freeCancellationDate"
                )
                if free_date:
                    dt = free_date[:10]
                    return f"Hủy miễn phí trước {dt}"
                return label
            return label
    except Exception:
        pass
    return ""


def _extract_review_score(hotel: dict) -> str:
    try:
        score = _safe_get(hotel, "content", "reviewSummary", "overallScore")
        count = _safe_get(hotel, "content", "reviewSummary", "reviewCount")
        if score:
            return f"{score}/10 ({count} đánh giá)" if count else f"{score}/10"
    except Exception:
        pass
    return ""


MEAL_PLAN_BENEFIT_IDS = {
    4: "Bữa sáng",
    5: "Nửa ngày ăn",
    8: "Cả ngày ăn",
    11: "Bữa sáng + tối",
}


def _extract_meal_plan(hotel: dict) -> str:
    """Return meal plan label from hotel-level benefit IDs, or empty string."""
    benefits = _safe_get(hotel, "pricing", "benefits", default=[]) or []
    for bid in benefits:
        label = MEAL_PLAN_BENEFIT_IDS.get(bid)
        if label:
            return label
    return ""


def _extract_landmarks(hotel: dict) -> str:
    """Return nearest top landmark distance string (e.g. 'Cách Phố Cổ 279m')."""
    try:
        top = _safe_get(hotel, "content", "localInformation", "landmarks", "topLandmark", default=[]) or []
        transport = _safe_get(hotel, "content", "localInformation", "landmarks", "transportation", default=[]) or []
        items = []
        for lm in (top[:2] if top else []):
            name = lm.get("landmarkName", "")
            dist = lm.get("distanceInM", 0)
            if name and dist is not None:
                dist_str = f"{dist/1000:.1f}km" if dist >= 1000 else f"{int(dist)}m"
                items.append(f"Cách {name} {dist_str}")
        for lm in (transport[:1] if transport else []):
            name = lm.get("landmarkName", "")
            dist = lm.get("distanceInM", 0)
            if name and dist is not None:
                dist_str = f"{dist/1000:.1f}km" if dist >= 1000 else f"{int(dist)}m"
                items.append(f"Cách {name} {dist_str}")
        return " • ".join(items)
    except Exception:
        return ""


def parse_hotel_from_graphql(hotel: dict, destination: str) -> dict | None:
    """Extract all fields from a GraphQL hotel property object."""
    info = _safe_get(hotel, "content", "informationSummary", default={})
    name = info.get("displayName") or info.get("defaultName")
    if not name:
        return None

    city_name = _safe_get(info, "address", "city", "name", default="")
    area_name = _safe_get(info, "address", "area", "name", default="")
    address = f"{area_name}, {city_name}".strip(", ") if area_name else city_name

    landmarks = _extract_landmarks(hotel)

    stars_raw = info.get("rating")
    stars = f"{int(stars_raw)} sao" if stars_raw else ""

    price_excl = _extract_price(hotel)
    price_incl = _extract_price_inclusive(hotel)
    meal_plan = _extract_meal_plan(hotel)
    cancellation = _extract_cancellation(hotel)
    review = _extract_review_score(hotel)

    property_page = _safe_get(info, "propertyLinks", "propertyPage", default="")
    url = f"https://www.agoda.com{property_page}" if property_page else ""

    return {
        "Tỉnh thành / Điểm đến": destination,
        "Tên khách sạn": name,
        "Địa chỉ": address,
        "Địa điểm nổi bật": landmarks,
        "Hạng sao": stars,
        "Điểm đánh giá": review,
        "Gói bữa ăn": meal_plan,
        "Giá/đêm (chưa gồm thuế)": price_excl,
        "Giá/đêm (đã gồm thuế)": price_incl,
        "Chính sách hoàn hủy": cancellation,
        "Link khách sạn": url,
    }


# ---------------------------------------------------------------------------
# Pagination helpers (DOM-based, only for page navigation)
# ---------------------------------------------------------------------------

async def get_total_pages(page) -> int:
    try:
        el = page.locator("[data-selenium='pagination-text']")
        if await el.count() > 0:
            text = (await el.text_content() or "").strip()
            m = re.search(r"trên\s+(\d+)", text)
            if m:
                return int(m.group(1))
    except Exception:
        pass
    return 1


async def click_next_page(page) -> bool:
    try:
        btn = page.locator("[data-selenium='pagination-next-btn']")
        if await btn.count() > 0:
            disabled = await btn.get_attribute("disabled")
            if disabled is not None:
                return False
            await btn.click()
            return True
    except Exception:
        pass
    return False


# ---------------------------------------------------------------------------
# Main scraping engine — GraphQL interception (fast path)
# ---------------------------------------------------------------------------

async def scrape_agoda(url: str, destination: str, status_callback=None) -> list:
    """
    Fast scraper: navigates pages with the browser but extracts hotel data
    directly from intercepted GraphQL responses (no DOM scraping, no scrolling).
    """
    results = []

    async with async_playwright() as pw:
        launch_kwargs = dict(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--window-size=1366,768",
            ]
        )
        sys_chromium = get_chromium_path()
        if sys_chromium:
            launch_kwargs["executable_path"] = sys_chromium

        browser = await pw.chromium.launch(**launch_kwargs)
        ua = random.choice(USER_AGENTS)
        context = await browser.new_context(
            user_agent=ua,
            viewport={"width": 1366, "height": 768},
            locale="vi-VN",
            extra_http_headers={
                "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
            }
        )
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'languages', { get: () => ['vi-VN', 'vi', 'en-US'] });
            window.chrome = { runtime: {} };
        """)
        page = await context.new_page()

        # --------------- GraphQL response queue ---------------
        gql_queue: asyncio.Queue = asyncio.Queue()

        async def on_response(response):
            if "graphql/search" in response.url:
                try:
                    body = await response.text()
                    data = json.loads(body)
                    if "citySearch" in data.get("data", {}):
                        await gql_queue.put(data["data"]["citySearch"])
                except Exception:
                    pass

        page.on("response", on_response)

        try:
            if status_callback:
                status_callback("Đang mở trang Agoda...")

            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(3)

            # Wait for first GraphQL response (max 20s)
            try:
                city_search_data = await asyncio.wait_for(gql_queue.get(), timeout=20)
            except asyncio.TimeoutError:
                if status_callback:
                    status_callback("Không nhận được dữ liệu từ Agoda. Kiểm tra URL.")
                await browser.close()
                return results

            total_pages = await get_total_pages(page)
            if status_callback:
                status_callback(f"Tìm thấy {total_pages} trang kết quả. Bắt đầu thu thập dữ liệu nhanh...")

            current_page = 1

            while True:
                properties = city_search_data.get("properties", [])
                if status_callback:
                    status_callback(
                        f"📄 Trang {current_page}/{total_pages} — "
                        f"{len(properties)} khách sạn — Tổng: {len(results) + len(properties)}"
                    )

                for hotel in properties:
                    record = parse_hotel_from_graphql(hotel, destination)
                    if record:
                        results.append(record)

                if current_page >= total_pages:
                    break

                # Navigate to next page
                await page.evaluate("window.scrollTo(0, 0)")
                await asyncio.sleep(0.5)
                moved = await click_next_page(page)
                if not moved:
                    if status_callback:
                        status_callback("Không thể chuyển trang tiếp theo. Dừng.")
                    break

                current_page += 1

                # Wait for next GraphQL response (max 30s)
                try:
                    city_search_data = await asyncio.wait_for(gql_queue.get(), timeout=30)
                except asyncio.TimeoutError:
                    if status_callback:
                        status_callback(f"Timeout trang {current_page}. Thử lại...")
                    # Try scrolling a tiny bit to trigger re-fetch
                    await page.evaluate("window.scrollBy(0, 100)")
                    await asyncio.sleep(2)
                    try:
                        city_search_data = await asyncio.wait_for(gql_queue.get(), timeout=15)
                    except asyncio.TimeoutError:
                        if status_callback:
                            status_callback(f"Không lấy được dữ liệu trang {current_page}. Dừng.")
                        break

                await asyncio.sleep(random.uniform(0.5, 1.2))

        except PlaywrightTimeoutError:
            if status_callback:
                status_callback("Timeout khi tải trang. Kiểm tra URL hoặc kết nối.")
        except Exception as e:
            if status_callback:
                status_callback(f"Lỗi: {e}")
        finally:
            await browser.close()

    return results


def run_scrape(url: str, destination: str, status_callback=None) -> list:
    """Synchronous wrapper."""
    _ensure_windows_proactor_policy()
    return asyncio.run(scrape_agoda(url, destination, status_callback))
