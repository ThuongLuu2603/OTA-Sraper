import asyncio
import random
import re
import time
import shutil
import urllib.request
import json
import sys
from datetime import datetime
from urllib.parse import quote
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from playwright_bootstrap import ensure_playwright_chromium


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


def _collect_city_search_nodes(data) -> list[dict]:
    """Recursively collect all `citySearch` dict nodes from a JSON payload."""
    found: list[dict] = []

    def walk(node):
        if isinstance(node, dict):
            cs = node.get("citySearch")
            if isinstance(cs, dict):
                found.append(cs)
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(data)
    return found


def _extract_total_pages_from_city_search(city_search: dict, fallback_pages: int = 1) -> int:
    """
    Derive total pages from citySearch metadata if available.
    Falls back to provided value when metadata is absent.
    """
    candidate_page_keys = ("totalPage", "totalPages", "pageCount", "pages", "lastPage")
    candidate_total_keys = ("totalCount", "totalResults", "resultCount", "hotelCount", "propertyCount")
    candidate_size_keys = ("pageSize", "size", "perPage", "itemsPerPage")

    explicit_pages = 0
    total_items = 0
    page_size = 0

    stack = [city_search]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            for k, v in node.items():
                lk = k.lower()
                if isinstance(v, (int, float)):
                    iv = int(v)
                    if lk in (x.lower() for x in candidate_page_keys) and iv > explicit_pages:
                        explicit_pages = iv
                    if lk in (x.lower() for x in candidate_total_keys) and iv > total_items:
                        total_items = iv
                    if lk in (x.lower() for x in candidate_size_keys) and iv > page_size:
                        page_size = iv
                elif isinstance(v, (dict, list)):
                    stack.append(v)
        elif isinstance(node, list):
            stack.extend(node)

    if explicit_pages > 0:
        return max(explicit_pages, fallback_pages)
    if total_items > 0 and page_size > 0:
        return max((total_items + page_size - 1) // page_size, fallback_pages)
    return fallback_pages


def _extract_properties_from_city_search(city_search: dict) -> list[dict]:
    """
    Extract Agoda property records from known and fallback locations in citySearch.
    """
    result: list[dict] = []
    known_keys = ("properties", "featuredProperties", "otherProperties", "sponsoredProperties")
    for key in known_keys:
        val = city_search.get(key)
        if isinstance(val, list):
            result.extend([x for x in val if isinstance(x, dict)])

    if result:
        return result

    # Fallback: recursively scan for objects that look like Agoda property nodes.
    seen = set()
    stack = [city_search]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            if isinstance(node.get("content"), dict) and isinstance(node.get("pricing"), dict):
                pid = _agoda_property_id_from_hotel(node)
                key = pid if pid else str(id(node))
                if key not in seen:
                    seen.add(key)
                    result.append(node)
            for v in node.values():
                if isinstance(v, (dict, list)):
                    stack.append(v)
        elif isinstance(node, list):
            stack.extend(node)
    return result


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


def _fmt_coord_value(raw) -> str:
    if raw is None:
        return ""
    try:
        x = float(raw)
        s = f"{x:.8f}".rstrip("0").rstrip(".")
        return s
    except (TypeError, ValueError):
        return str(raw).strip()


def _lat_lng_from_geo_dict(d: dict) -> tuple[str, str]:
    if not isinstance(d, dict):
        return "", ""
    pairs = (
        ("latitude", "longitude"),
        ("lat", "lng"),
        ("Lat", "Lng"),
    )
    for a, b in pairs:
        if a in d and b in d:
            la, lo = _fmt_coord_value(d.get(a)), _fmt_coord_value(d.get(b))
            if la and lo:
                return la, lo
    return "", ""


_AGODA_ID_URL_RES = (
    re.compile(r"[?&]hid=(\d{4,})", re.I),
    re.compile(r"[?&]hotel_?id=(\d{4,})", re.I),
    re.compile(r"[?&]hotelIds?=(\d{4,})", re.I),
    re.compile(r"[?&]property_?id=(\d{4,})", re.I),
    re.compile(r"[?&]propertyIds?=(\d{4,})", re.I),
)

# Keys Agoda / GraphQL có thể dùng (không khớp chữ hoa/thường khi so sánh)
_AGODA_ID_FIELD_KEYS = frozenset(
    {
        "propertyid",
        "masterpropertyid",
        "hotelid",
        "hid",
        "hotelhid",
        "agodapropertyid",
    }
)


def _normalize_agoda_property_id_value(val) -> str:
    if val is None or isinstance(val, bool):
        return ""
    if isinstance(val, int):
        s = str(val)
        return s if len(s) >= 4 else ""
    if isinstance(val, float):
        if val != val:  # NaN
            return ""
        if val == int(val):
            s = str(int(val))
            return s if len(s) >= 4 else ""
    s = str(val).strip()
    if s.isdigit() and len(s) >= 4:
        return s
    m = re.search(r"\b(\d{5,12})\b", s)
    return m.group(1) if m else ""


def _agoda_property_id_from_urls(links: dict) -> str:
    if not isinstance(links, dict):
        return ""
    for sub in (
        "propertyPage",
        "propertyUrl",
        "mobileDeepLink",
        "desktopUrl",
        "seoUrl",
        "deepLink",
    ):
        u = links.get(sub)
        if not isinstance(u, str) or not u.strip():
            continue
        for rx in _AGODA_ID_URL_RES:
            m = rx.search(u)
            if m:
                return m.group(1)
    return ""


def _agoda_property_id_scan_content(content, max_nodes: int = 450) -> str:
    """Tìm property id trong cây JSON (payload citySearch đổi tên field theo thời điểm)."""
    if not isinstance(content, dict):
        return ""
    stack = [(content, 0)]
    seen_nodes = 0
    hits: list[tuple[int, int, str]] = []
    while stack and seen_nodes < max_nodes:
        node, depth = stack.pop()
        seen_nodes += 1
        if not isinstance(node, dict):
            continue
        for k, val in node.items():
            lk = str(k).lower().replace("_", "")
            if isinstance(val, dict) and depth < 12:
                stack.append((val, depth + 1))
            elif isinstance(val, list) and depth < 10:
                for it in val[:30]:
                    if isinstance(it, dict):
                        stack.append((it, depth + 1))
            else:
                if lk in _AGODA_ID_FIELD_KEYS or lk.endswith("propertyid") and "ids" not in lk:
                    got = _normalize_agoda_property_id_value(val)
                    if got:
                        pri = 0 if "information" in lk or lk == "propertyid" else 1
                        hits.append((pri, depth, got))
        if len(hits) >= 5:
            break
    if not hits:
        return ""
    hits.sort(key=lambda t: (t[0], t[1]))
    return hits[0][2]


def _agoda_property_id_from_hotel(hotel: dict) -> str:
    info = _safe_get(hotel, "content", "informationSummary", default={}) or {}
    if not isinstance(info, dict):
        info = {}

    for key in (
        "propertyId",
        "propertyID",
        "PropertyId",
        "PropertyID",
        "masterPropertyId",
        "masterPropertyID",
        "hotelId",
        "hotelID",
        "HotelId",
        "agodaPropertyId",
        "hid",
        "hotelHid",
    ):
        got = _normalize_agoda_property_id_value(info.get(key))
        if got:
            return got

    if isinstance(hotel, dict):
        for key in ("propertyId", "propertyID", "hotelId"):
            got = _normalize_agoda_property_id_value(hotel.get(key))
            if got:
                return got

    links = info.get("propertyLinks")
    got = _agoda_property_id_from_urls(links if isinstance(links, dict) else {})
    if got:
        return got

    pricing = hotel.get("pricing") if isinstance(hotel, dict) else None
    if isinstance(pricing, dict):
        for key in ("propertyId", "propertyID", "hotelId"):
            got = _normalize_agoda_property_id_value(pricing.get(key))
            if got:
                return got

    content = hotel.get("content") if isinstance(hotel, dict) else None
    return _agoda_property_id_scan_content(content) if isinstance(content, dict) else ""


def _scan_hotel_node_for_latlng(hotel: dict, max_visits: int = 600) -> tuple[str, str]:
    """Duyệt giới hạn toàn bộ node property để tìm cặp lat/lng (citySearch thường lồng sâu)."""
    stack = [hotel]
    visits = 0
    while stack and visits < max_visits:
        node = stack.pop()
        visits += 1
        if isinstance(node, dict):
            la, lo = _lat_lng_from_geo_dict(node)
            if la and lo:
                return la, lo
            mlat = node.get("mapLat") or node.get("mapLatitude")
            mlng = node.get("mapLng") or node.get("mapLongitude")
            la, lo = _fmt_coord_value(mlat), _fmt_coord_value(mlng)
            if la and lo:
                return la, lo
            for v in node.values():
                if isinstance(v, (dict, list)):
                    stack.append(v)
        elif isinstance(node, list):
            for it in node[:50]:
                if isinstance(it, (dict, list)):
                    stack.append(it)
    return "", ""


def _extract_agoda_property_geo(hotel: dict) -> tuple[str, str, str]:
    """propertyId + lat/lng từ node GraphQL (nhiều dạng field + URL + quét JSON)."""
    info = _safe_get(hotel, "content", "informationSummary", default={}) or {}
    pid_str = _agoda_property_id_from_hotel(hotel)

    lat_s, lng_s = "", ""
    for block in (
        _safe_get(info, "address", "geoPoint", default=None),
        info.get("geoPoint"),
        _safe_get(info, "address", "coordinates", default=None),
        info.get("coordinates"),
    ):
        if isinstance(block, dict):
            lat_s, lng_s = _lat_lng_from_geo_dict(block)
            if lat_s and lng_s:
                break

    if not lat_s or not lng_s:
        la = info.get("latitude") or info.get("lat") or info.get("geoLatitude")
        lo = info.get("longitude") or info.get("lng") or info.get("lon") or info.get("geoLongitude")
        lat_s, lng_s = _fmt_coord_value(la), _fmt_coord_value(lo)

    if not lat_s or not lng_s:
        content = hotel.get("content")
        if isinstance(content, dict):
            for key in ("geoInformation", "localGeo", "map", "location"):
                block = content.get(key)
                if not isinstance(block, dict):
                    continue
                la, lo = _lat_lng_from_geo_dict(block)
                if not la:
                    la = _fmt_coord_value(block.get("latitude") or block.get("lat"))
                    lo = _fmt_coord_value(block.get("longitude") or block.get("lng"))
                if la and lo:
                    lat_s, lng_s = la, lo
                    break

    if (not lat_s or not lng_s) and isinstance(hotel, dict):
        la, lo = _scan_hotel_node_for_latlng(hotel)
        if la and lo:
            lat_s, lng_s = la, lo

    return pid_str, lat_s, lng_s


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

    pid_str, lat_s, lng_s = _extract_agoda_property_geo(hotel)

    return {
        "Tỉnh thành / Điểm đến": destination,
        "Tên khách sạn": name,
        "Địa chỉ": address,
        "Mã Property Agoda": pid_str,
        "ID khách sạn Travel": "",
        "Mã property (OTA)": "",
        "Vĩ độ": lat_s,
        "Kinh độ": lng_s,
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


def _next_btn_locator(page):
    return page.locator("[data-selenium='pagination-next-btn']")


async def _next_button_clickable(page) -> bool | None:
    """
    True  = có Next và bấm được.
    False = có Next nhưng disabled (hết trang).
    None  = chưa thấy nút (UI chưa kịp render — không được coi là hết trang).
    """
    try:
        btn = _next_btn_locator(page)
        if await btn.count() == 0:
            return None
        el = btn.first
        dis = await el.get_attribute("disabled")
        aria = (await el.get_attribute("aria-disabled") or "").strip().lower()
        if dis is not None or aria in ("true", "1"):
            return False
        return True
    except Exception:
        return None


async def scroll_pagination_into_view(page) -> None:
    """Kéo xuống cuối danh sách để Agoda mount thanh phân trang."""
    try:
        await page.evaluate(
            """() => {
                const y = Math.max(0, (document.body.scrollHeight || 0) - 900);
                window.scrollTo({ top: y, behavior: 'instant' });
            }"""
        )
    except Exception:
        pass


async def pagination_next_available(
    page, total_pages_hint: int, current_page: int, props_on_page: int = 0
) -> bool:
    """
    Còn trang sau không. Chờ nút Next xuất hiện (tránh dừng nhầm ở trang 1).
    Nếu API/DOM báo còn trang mà nút chưa thấy, vẫn trả True để thử click.
    Trang ~45–50 KS mà metadata báo 1 trang: vẫn thử Next (Agoda hay báo sai).
    """
    await scroll_pagination_into_view(page)
    await asyncio.sleep(0.45)

    state: bool | None = None
    for _ in range(18):
        state = await _next_button_clickable(page)
        if state is not None:
            break
        await asyncio.sleep(0.35)

    if state is True:
        return True
    if state is False:
        return False
    if current_page < total_pages_hint:
        return True
    if props_on_page >= 42:
        return True
    return False


async def click_next_page(page) -> bool:
    try:
        btn = _next_btn_locator(page)
        if await btn.count() > 0:
            el = btn.first
            try:
                await el.scroll_into_view_if_needed(timeout=5000)
            except Exception:
                pass
            await asyncio.sleep(0.2)
            disabled = await el.get_attribute("disabled")
            aria = (await el.get_attribute("aria-disabled") or "").strip().lower()
            if disabled is not None or aria in ("true", "1"):
                return False
            await el.click()
            return True
    except Exception:
        pass
    return False


async def _poll_first_city_search(gql_queue: asyncio.Queue, page, status_callback, max_wait: float = 75.0):
    """
    Chờ gói GraphQL đầu tiên. Một nhịp 15s rồi các nhịp ngắn + scroll để nhanh hơn khi mạng tốt.
    """
    try:
        return await asyncio.wait_for(gql_queue.get(), timeout=15.0)
    except asyncio.TimeoutError:
        if status_callback:
            status_callback("Đang chờ Agoda (lần 1/15s)…")

    deadline = time.monotonic() + max_wait
    attempt = 0
    while time.monotonic() < deadline:
        attempt += 1
        slice_sec = min(5.0, max(2.0, deadline - time.monotonic()))
        if slice_sec <= 0:
            break
        try:
            return await asyncio.wait_for(gql_queue.get(), timeout=slice_sec)
        except asyncio.TimeoutError:
            if status_callback and attempt % 3 == 0:
                status_callback(f"Đang chờ Agoda tải kết quả… ({attempt})")
            try:
                await page.evaluate(
                    "window.scrollTo(0, Math.min(1200, document.body.scrollHeight || 1200))"
                )
            except Exception:
                pass
            await asyncio.sleep(0.5)
    raise asyncio.TimeoutError()


async def _collect_extra_city_searches(gql_queue: asyncio.Queue, settle: float = 1.35) -> list:
    """Gom thêm payload citySearch bắn nối tiếp trong cửa sổ ngắn (trang 1 hay gửi nhiều gói)."""
    extra: list = []
    end = time.monotonic() + settle
    while time.monotonic() < end:
        try:
            extra.append(gql_queue.get_nowait())
        except asyncio.QueueEmpty:
            await asyncio.sleep(0.06)
    return extra


def _properties_from_city_search_nodes(nodes: list) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for n in nodes:
        if not isinstance(n, dict):
            continue
        for h in _extract_properties_from_city_search(n):
            pid = _agoda_property_id_from_hotel(h)
            key = pid if pid else str(id(h))
            if key in seen:
                continue
            seen.add(key)
            out.append(h)
    return out


def _max_total_pages_from_nodes(nodes: list, dom_pages: int) -> int:
    m = max(1, dom_pages)
    for n in nodes:
        if isinstance(n, dict):
            m = max(m, _extract_total_pages_from_city_search(n, m))
    return m


# ---------------------------------------------------------------------------
# Main scraping engine — GraphQL interception (fast path)
# ---------------------------------------------------------------------------

async def _agoda_route_skip_heavy_assets(route) -> None:
    """Chặn ảnh / font / media để tải trang nhẹ hơn; giữ script & stylesheet cho UI phân trang."""
    if route.request.resource_type in ("image", "media", "font"):
        await route.abort()
    else:
        await route.continue_()


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
        await page.route("**/*", _agoda_route_skip_heavy_assets)

        # --------------- GraphQL response queue ---------------
        gql_queue: asyncio.Queue = asyncio.Queue()

        async def on_response(response):
            ct = (response.headers.get("content-type") or "").lower()
            if "json" not in ct:
                return
            try:
                body = await response.text()
                if "citySearch" not in body:
                    return
                data = json.loads(body)
                nodes = _collect_city_search_nodes(data)
                for node in nodes:
                    await gql_queue.put(node)
            except Exception:
                pass

        page.on("response", on_response)

        try:
            if status_callback:
                status_callback("Đang mở trang Agoda...")

            for goto_try in range(1, 4):
                try:
                    await page.goto(
                        url,
                        wait_until="domcontentloaded",
                        timeout=90000,
                    )
                    break
                except PlaywrightTimeoutError:
                    if status_callback:
                        status_callback(f"Goto Agoda chậm, thử lại ({goto_try}/3)…")
                    if goto_try >= 3:
                        raise
                    await asyncio.sleep(2.0)

            try:
                await page.wait_for_load_state("load", timeout=25000)
            except Exception:
                pass
            await asyncio.sleep(2.0)

            try:
                first_node = await _poll_first_city_search(
                    gql_queue, page, status_callback, max_wait=75.0
                )
            except asyncio.TimeoutError:
                if status_callback:
                    status_callback("Không nhận được dữ liệu GraphQL từ Agoda sau ~90s. Kiểm tra URL/mạng.")
                await browser.close()
                return results

            first_batch = [first_node] + await _collect_extra_city_searches(gql_queue, settle=1.5)
            dom_pages = await get_total_pages(page)
            total_pages = _max_total_pages_from_nodes(first_batch, dom_pages)
            if status_callback:
                status_callback(
                    f"Tìm thấy {total_pages} trang kết quả (DOM + API). Bắt đầu thu thập…"
                )

            current_page = 1
            seen_property_ids: set[str] = set()

            while True:
                try:
                    dom_p = await get_total_pages(page)
                    total_pages = max(total_pages, dom_p)
                except Exception:
                    pass

                if current_page == 1:
                    properties = _properties_from_city_search_nodes(first_batch)
                else:
                    properties = _extract_properties_from_city_search(city_search_data)
                    total_pages = max(
                        total_pages,
                        _extract_total_pages_from_city_search(city_search_data, total_pages),
                    )
                if status_callback:
                    status_callback(
                        f"📄 Trang {current_page}/{total_pages} — "
                        f"{len(properties)} khách sạn — Đã gom: {len(results)} (unique id)"
                    )

                for hotel in properties:
                    pid_key = _agoda_property_id_from_hotel(hotel)
                    if pid_key:
                        if pid_key in seen_property_ids:
                            continue
                        seen_property_ids.add(pid_key)
                    record = parse_hotel_from_graphql(hotel, destination)
                    if record:
                        results.append(record)

                if current_page >= 150:
                    if status_callback:
                        status_callback("⚠️ Đạt giới hạn 150 trang Agoda.")
                    break

                # Phân trang Agoda render muộn: phải chờ nút Next, không coi "chưa thấy" = hết trang.
                if not await pagination_next_available(
                    page, total_pages, current_page, len(properties)
                ):
                    if status_callback:
                        status_callback(
                            f"✅ Hết phân trang. Tổng {len(results)} dòng kết quả."
                        )
                    break

                # Navigate to next page
                await page.evaluate("window.scrollTo(0, 0)")
                await asyncio.sleep(0.35)
                moved = await click_next_page(page)
                if not moved:
                    if status_callback:
                        status_callback("Không bấm được Next. Dừng.")
                    break

                current_page += 1

                try:
                    city_search_data = await asyncio.wait_for(gql_queue.get(), timeout=50)
                except asyncio.TimeoutError:
                    if status_callback:
                        status_callback(f"Timeout chờ GraphQL trang {current_page}, thử scroll…")
                    await page.evaluate("window.scrollBy(0, 400)")
                    await asyncio.sleep(2.0)
                    try:
                        city_search_data = await asyncio.wait_for(gql_queue.get(), timeout=35)
                    except asyncio.TimeoutError:
                        if status_callback:
                            status_callback(f"Không lấy được dữ liệu trang {current_page}. Dừng.")
                        break

                extra_next = await _collect_extra_city_searches(gql_queue, settle=0.9)
                if extra_next:
                    merged_nodes = [city_search_data] + extra_next
                    total_pages = max(
                        total_pages,
                        _max_total_pages_from_nodes(merged_nodes, total_pages),
                    )
                    combined = _properties_from_city_search_nodes(merged_nodes)
                    city_search_data = (
                        {"properties": combined} if combined else merged_nodes[0]
                    )

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
    ensure_playwright_chromium(status_callback)
    return asyncio.run(scrape_agoda(url, destination, status_callback))
