"""
Trip.com hotel scraper — DOM-based extraction.
Uses Playwright to load hotel list pages and parse hotel cards.
"""

import asyncio
import re
import shutil
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ---------------------------------------------------------------------------
# Known Vietnamese city IDs on Trip.com (cityId, countryId=111)
# ---------------------------------------------------------------------------
KNOWN_CITY_IDS = {
    "hà nội": (286, 111),
    "hanoi": (286, 111),
    "ha noi": (286, 111),
    "tp. hồ chí minh": (301, 111),
    "hồ chí minh": (301, 111),
    "ho chi minh": (301, 111),
    "hcm": (301, 111),
    "sài gòn": (301, 111),
    "saigon": (301, 111),
    "đà nẵng": (1356, 111),
    "da nang": (1356, 111),
    "đà lạt": (5204, 111),
    "da lat": (5204, 111),
    "dalat": (5204, 111),
    "nha trang": (1777, 111),
    "phú quốc": (5649, 111),
    "phu quoc": (5649, 111),
    "vũng tàu": (7529, 111),
    "vung tau": (7529, 111),
    "hội an": (5206, 111),
    "hoi an": (5206, 111),
    "huế": (5207, 111),
    "hue": (5207, 111),
    "hạ long": (5201, 111),
    "ha long": (5201, 111),
    "halong": (5201, 111),
    "quy nhơn": (5210, 111),
    "quy nhon": (5210, 111),
    "phan thiết": (5216, 111),
    "phan thiet": (5216, 111),
    "mũi né": (5216, 111),
    "mui ne": (5216, 111),
    "châu đốc": (5202, 111),
    "chau doc": (5202, 111),
    "cần thơ": (5203, 111),
    "can tho": (5203, 111),
    "sapa": (5213, 111),
    "sa pa": (5213, 111),
    "ninh bình": (5211, 111),
    "ninh binh": (5211, 111),
    "quảng bình": (5215, 111),
    "quang binh": (5215, 111),
    "phong nha": (5215, 111),
}

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"


def get_chromium_path() -> str | None:
    for name in ["chromium-browser", "chromium", "google-chrome"]:
        p = shutil.which(name)
        if p:
            return p
    return None


def resolve_trip_city(destination: str) -> tuple[int, int] | None:
    """Return (cityId, countryId) for a destination string, or None if not found."""
    key = destination.strip().lower()
    if key in KNOWN_CITY_IDS:
        return KNOWN_CITY_IDS[key]
    # Partial match
    for k, v in KNOWN_CITY_IDS.items():
        if key in k or k in key:
            return v
    return None


def build_tripcom_url(city_id: int, check_in: str, check_out: str,
                      rooms: int = 1, adults: int = 2, children: int = 0) -> str:
    return (
        f"https://vn.trip.com/hotels/list?city={city_id}"
        f"&checkin={check_in}&checkout={check_out}"
        f"&adult={adults}&children={children}&rooms={rooms}"
        f"&curr=VND&locale=vi-VN"
    )


# ---------------------------------------------------------------------------
# Price extraction helpers
# ---------------------------------------------------------------------------

def _clean_card_text(text: str) -> str:
    """Remove known badge phrases that corrupt other field parsing."""
    for pattern in [
        r'Mới dùng Trip\.com',
        r'Khai trương năm \d{4}',
        r'Được nâng cấp năm \d{4}',
        r'Hàng Top \d+ tại .+?(?=\d|\n|$)',
    ]:
        text = re.sub(pattern, ' ', text)
    return text


def _parse_vnd_price(text: str) -> str:
    """Extract cheapest VND price from text, formatted as 'X VND'."""
    amounts = re.findall(r'([\d.]+)₫', text.replace(',', '.'))
    if not amounts:
        return ""
    # Parse all numeric values; skip unrealistically low amounts (< 50,000 VND)
    nums = []
    for a in amounts:
        try:
            v = int(a.replace('.', ''))
            if v >= 50_000:
                nums.append(v)
        except Exception:
            pass
    if not nums:
        return ""
    # The discounted (lowest) price
    price = min(nums)
    return f"{price:,} VND".replace(',', '.')


def _parse_score(text: str) -> str:
    """Extract score like '8,5/10' or '9.2/10' from text (1-2 digit integer part only)."""
    m = re.search(r'(?<!\d)(\d{1,2}[,\.]\d+)\s*/\s*10', text)
    if m:
        score = float(m.group(1).replace(',', '.'))
        if 0 <= score <= 10:
            return f"{score}/10"
    return ""


def _parse_cancellation(text: str) -> str:
    if re.search(r'hủy\s*miễn\s*phí', text, re.IGNORECASE):
        return "Hủy miễn phí"
    if re.search(r'không\s*hoàn\s*tiền', text, re.IGNORECASE):
        return "Không hoàn tiền"
    return ""


def _parse_meal_plan(text: str) -> str:
    """Detect breakfast / meal plan info from card text."""
    t = text.lower()
    if any(kw in t for kw in [
        "bữa sáng miễn phí", "có bữa sáng", "bao gồm bữa sáng",
        "free breakfast", "breakfast included",
    ]):
        return "Có bữa sáng"
    if any(kw in t for kw in ["bữa sáng", "breakfast"]):
        return "Bữa sáng"
    if any(kw in t for kw in ["bán phần", "half board"]):
        return "Bán phần"
    if any(kw in t for kw in ["nguyên phần", "full board"]):
        return "Nguyên phần"
    return ""


def _parse_stars(text: str) -> str:
    m = re.search(r'(\d)\s*sao', text, re.IGNORECASE)
    if m:
        return f"{m.group(1)} sao"
    # Count star symbols
    stars = text.count('★')
    if stars:
        return f"{stars} sao"
    return ""


def _parse_location(text: str) -> str:
    """Extract 'Gần X' location hint — skip customer review quotes first."""
    # Remove quoted customer review snippets like "Gần sân bay""Dễ đi lại"
    no_quotes = re.sub(r'"[^"]*"', ' ', text)
    m = re.search(
        r'Gần\s+(.+?)(?:Xem trên bản đồ|Phòng |Hủy|Giảm|\d+₫|\n|$)',
        no_quotes, re.IGNORECASE
    )
    if m:
        loc = m.group(1).strip().rstrip(' ,')
        # Collapse multiple whitespace into separator
        loc = re.sub(r'\s{2,}', ' · ', loc)
        return loc[:100]
    return ""


# ---------------------------------------------------------------------------
# DOM extraction
# ---------------------------------------------------------------------------

HOTEL_CARD_JS = """() => {
    // Try multiple possible card container selectors (Trip.com changes class names)
    const CARD_SELECTORS = [
        'div.hotel-card',
        'div[class*="hotel-card"]',
        'li[class*="hotel-item"]',
        'div[class*="hotel-item"]',
        '[class*="hotelListItem"]',
        '[class*="hotel-list-item"]',
        '[data-testid*="hotel"]',
        '[class*="propertyCard"]',
        '[class*="HotelList"] > div',
        '[class*="hotel_list"] > div',
    ];

    let cards = [];
    let usedSel = '';
    for (const sel of CARD_SELECTORS) {
        const found = document.querySelectorAll(sel);
        if (found.length > 0) {
            cards = Array.from(found);
            usedSel = sel;
            break;
        }
    }

    const results = [];
    const seen = new Set();

    cards.forEach(card => {
        // Link: try hotel detail link first
        const linkEl = card.querySelector('a[href*="/hotels/detail/"]') ||
                       card.querySelector('a[href*="/hotel/"]') ||
                       card.querySelector('a[href*="trip.com"]');
        const link = linkEl ? (linkEl.href || '') : '';

        // Hotel ID: from card.id, or extracted from link URL
        let hotelId = card.id || '';
        if (!hotelId && link) {
            const m = link.match(/[\/\-](\d{5,})/);
            if (m) hotelId = m[1];
        }
        if (!hotelId) {
            // Use link as unique key fallback
            if (link) hotelId = link;
            else return;  // Skip if truly no identifier
        }
        if (seen.has(hotelId)) return;
        seen.add(hotelId);

        const fullText = card.textContent || '';

        // Name: try structured selectors first
        const nameSelectors = [
            '[class*="hotel-name"]', '[class*="hotelName"]',
            '[class*="name__"]', 'h2', 'h3', 'h4',
            '[class*="title"]', '[class*="hotel-title"]',
        ];
        let name = '';
        for (const sel of nameSelectors) {
            const el = card.querySelector(sel);
            if (el && el.textContent.trim().length > 2) {
                name = el.textContent.trim();
                break;
            }
        }
        if (!name && link) {
            // Last resort: first meaningful text line
            const lines = fullText.split('\\n').map(l => l.trim()).filter(l => l.length > 3);
            if (lines.length) name = lines[0].substring(0, 120);
        }
        if (!name) return;

        results.push({ hotelId, name, fullText, link });
    });

    return { results, debug: { usedSel, cardCount: cards.length } };
}"""


DIAG_JS = """() => {
    const sels = ['div.hotel-card','div[class*="hotel-card"]','div[class*="hotel-item"]',
                  'li[class*="hotel"]','[class*="hotelListItem"]','[class*="propertyCard"]'];
    const counts = {};
    sels.forEach(s => { counts[s] = document.querySelectorAll(s).length; });
    const allDivs = document.querySelectorAll('div[class]');
    const classes = new Set();
    allDivs.forEach(d => d.className.split(' ').forEach(c => {
        if (c.toLowerCase().includes('hotel')) classes.add(c);
    }));
    return { counts, hotelClasses: Array.from(classes).slice(0, 20),
             bodySnippet: document.body.innerText.substring(0, 300) };
}"""


async def _extract_page_hotels(page, destination: str, status_callback=None) -> list[dict]:
    """Extract all hotel cards visible on the current page."""
    payload = await page.evaluate(HOTEL_CARD_JS)
    # New format: { results: [...], debug: {...} }
    if isinstance(payload, dict):
        raw = payload.get("results", [])
        dbg = payload.get("debug", {})
        if dbg and status_callback:
            status_callback(f"🔍 DOM: selector='{dbg.get('usedSel','?')}' cards={dbg.get('cardCount',0)}")
    else:
        raw = payload or []

    hotels = []
    for r in raw:
        name = r.get("name", "").strip()
        text = r.get("fullText", "")
        link = r.get("link", "")
        hotel_id = r.get("hotelId", "")

        if not name or not hotel_id:
            continue

        # Clean up name — remove badge suffixes anywhere in the name
        name = re.sub(r'\s*(Mới dùng Trip\.com|Khai trương năm \d{4}|Được nâng cấp năm \d{4})', '', name).strip()

        # Use badge-cleaned text for numeric field parsing to avoid contamination
        clean = _clean_card_text(text)

        price = _parse_vnd_price(clean)
        score = _parse_score(clean)
        cancellation = _parse_cancellation(clean)
        stars = _parse_stars(clean)
        location = _parse_location(clean)

        hotels.append({
            "Tỉnh thành / Điểm đến": destination,
            "Tên khách sạn": name,
            "Địa chỉ": location,
            "Hạng sao": stars,
            "Điểm đánh giá": score,
            "Gói bữa ăn": _parse_meal_plan(clean),
            "Giá/đêm (VND)": price,
            "Chính sách hoàn hủy": cancellation,
            "Link khách sạn": link,
        })
    return hotels


def _make_page_url(base_url: str, page_idx: int) -> str:
    """
    Build URL for Trip.com with 0-indexed pageIndex param.
    page_idx=0 → base URL (first page)
    page_idx=1 → &pageIndex=1 (second page)
    """
    url = re.sub(r'[?&]pageIndex=\d+', '', base_url).rstrip('&').rstrip('?')
    url = re.sub(r'[?&]page=\d+', '', url).rstrip('&').rstrip('?')
    sep = '&' if '?' in url else '?'
    if page_idx == 0:
        return url
    return f"{url}{sep}pageIndex={page_idx}"


async def _detect_total_hotels(page) -> int:
    """Detect total hotel count from visible page text."""
    try:
        body = await page.inner_text("body")
        # Trip.com VN shows: "123 khách sạn", "456 hotels", or "Tìm thấy 789"
        for pattern in [
            r'(?:Tìm thấy|tổng)\s+([\d,\.]+)\s*(?:khách sạn|hotel)',
            r'([\d,\.]+)\s+(?:khách sạn|kết quả|hotel)',
            r'(?:khách sạn|hotel)[^\d]*([\d,\.]+)',
        ]:
            m = re.search(pattern, body, re.IGNORECASE)
            if m:
                raw = m.group(1).replace(',', '').replace('.', '')
                n = int(raw)
                if 1 <= n <= 5000:
                    return n
    except Exception:
        pass
    return 0


# ---------------------------------------------------------------------------
# Main scrape runner — URL-based pagination (more reliable than clicking buttons)
# ---------------------------------------------------------------------------

def _clean_tripcom_url(raw_url: str) -> str:
    """
    Strip marketing/filter/UI params from a Trip.com URL and rebuild a clean
    scraper-friendly URL that renders the standard hotel list page.
    Keeps: city/cityId, provinceId/districtId, countryId, checkin/checkout,
           adult/children/crn, searchType, searchWord, searchValue, curr/locale.
    Removes: listFilters, flexType, fixedDate, old, ctm_ref, searchBoxArg,
             travelPurpose, domestic, searchCoordinate, lat/lon, etc.
    """
    from urllib.parse import urlparse, parse_qs, urlencode

    KEEP_PARAMS = {
        "city", "cityid", "cityname",
        "provinceid", "districtid", "countryid",
        "checkin", "checkout", "checkIn", "checkOut",
        "adult", "adults", "children", "crn", "rooms",
        "searchtype", "searchword", "searchvalue",
        "searchname", "destname",
        "curr", "barcurr", "locale",
    }

    try:
        parsed = urlparse(raw_url)
        params = parse_qs(parsed.query, keep_blank_values=False)
        clean = {k: v for k, v in params.items() if k.lower() in KEEP_PARAMS}
        # Ensure VND currency
        if "curr" not in clean and "barCurr" not in clean:
            clean["curr"] = ["VND"]
        if "locale" not in clean:
            clean["locale"] = ["vi-VN"]
        new_query = urlencode({k: v[0] for k, v in clean.items()})
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{new_query}"
    except Exception:
        return raw_url


async def _scroll_and_extract(page, destination: str, status_callback, page_label: str = "") -> list[dict]:
    """
    Scroll through the full page to ensure all content is rendered,
    then extract all hotel cards in a single pass.
    Trip.com renders hotel cards server-side (not infinite-scroll), so we
    just need to scroll enough for lazy images/prices to populate.
    """
    # Phase 1: scroll to bottom repeatedly until page height stabilises
    prev_h = 0
    for _ in range(12):
        h = await page.evaluate("document.body.scrollHeight")
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(1.2)
        new_h = await page.evaluate("document.body.scrollHeight")
        if new_h == prev_h:
            break
        prev_h = new_h

    # Phase 2: scroll back to top so viewport covers cards from the start
    await page.evaluate("window.scrollTo(0, 0)")
    await asyncio.sleep(0.5)

    # Phase 3: single extraction pass
    payload = await page.evaluate(HOTEL_CARD_JS)
    if isinstance(payload, dict):
        raw = payload.get("results", [])
        dbg = payload.get("debug", {})
        sel = dbg.get("usedSel", "?")
        n_cards = dbg.get("cardCount", 0)
        status_callback(f"  🔍 {page_label}selector='{sel}' raw_cards={n_cards} valid={len(raw)}")
    else:
        raw = payload or []
        status_callback(f"  🔍 {page_label}raw_cards={len(raw)}")

    hotels = []
    seen_ids: set[str] = set()
    for r in raw:
        hid = r.get("hotelId", "")
        name = r.get("name", "").strip()
        if not name:
            continue
        # Use link as fallback ID if card.id absent
        key = hid or r.get("link", "") or name
        if not key or key in seen_ids:
            continue
        seen_ids.add(key)

        text = r.get("fullText", "")
        link = r.get("link", "")
        clean = _clean_card_text(text)
        hotels.append({
            "Tỉnh thành / Điểm đến": destination,
            "Tên khách sạn": re.sub(
                r'\s*(Mới dùng Trip\.com|Khai trương năm \d{4}|Được nâng cấp năm \d{4})',
                '', name).strip(),
            "Địa chỉ": _parse_location(clean),
            "Hạng sao": _parse_stars(clean),
            "Điểm đánh giá": _parse_score(clean),
            "Gói bữa ăn": _parse_meal_plan(clean),
            "Giá/đêm (VND)": _parse_vnd_price(clean),
            "Chính sách hoàn hủy": _parse_cancellation(clean),
            "Link khách sạn": link,
        })
    return hotels


def _find_hotel_list_in_json(data) -> list:
    """
    Recursively search a JSON blob for a list that looks like hotel records.
    Returns the list if found (len >= 5), else [].
    """
    if isinstance(data, list) and len(data) >= 5:
        # Check if items look like hotel records
        sample = data[0] if data else {}
        if isinstance(sample, dict):
            hotel_keys = {"hotelId", "hotelName", "name", "id", "price", "star",
                          "score", "rating", "address", "cityId"}
            if len(hotel_keys & set(sample.keys())) >= 2:
                return data
    if isinstance(data, dict):
        # Try common wrapper keys first
        for key in ("hotelList", "hotels", "hotelInfoList", "result", "data",
                    "list", "items", "records"):
            sub = data.get(key)
            if sub:
                found = _find_hotel_list_in_json(sub)
                if found:
                    return found
        # Recurse into all dict values
        for v in data.values():
            if isinstance(v, (dict, list)):
                found = _find_hotel_list_in_json(v)
                if found:
                    return found
    return []


def _find_page_key_in_body(body_obj: dict) -> tuple[str, int]:
    """Return (key_name, current_page_value) for the pagination key in a request body."""
    for key in ("pageIndex", "pageNum", "page", "pageNo", "currentPage"):
        if key in body_obj:
            return key, body_obj[key]
    return "", 0


def _hotels_from_api_list(items: list, destination: str) -> list[dict]:
    """Convert raw API hotel list items → standard hotel dicts."""
    hotels = []
    for item in items:
        if not isinstance(item, dict):
            continue
        name = (item.get("hotelName") or item.get("name") or
                item.get("hotelNameEn") or "").strip()
        hid = str(item.get("hotelId") or item.get("id") or "")
        if not name or not hid:
            continue

        # Price
        raw_price = (item.get("price") or item.get("lowPrice") or
                     item.get("minPrice") or item.get("displayPrice") or
                     item.get("roomPrice") or 0)
        try:
            price_int = int(float(str(raw_price).replace(",", "").replace(".", "")))
            price = f"{price_int:,.0f} VND".replace(",", ".") if price_int >= 10000 else ""
        except Exception:
            price = ""

        # Score
        score_raw = item.get("score") or item.get("rating") or item.get("commentScore") or ""
        score = f"{score_raw}/10" if score_raw else ""

        # Stars
        star_raw = item.get("star") or item.get("starLevel") or ""
        stars = f"{int(float(str(star_raw)))} sao" if star_raw else ""

        # Link
        link = item.get("hotelUrl") or item.get("url") or item.get("detailUrl") or ""
        if link and not link.startswith("http"):
            link = "https://vn.trip.com" + link
        if not link and hid:
            link = f"https://vn.trip.com/hotels/detail/?hotelId={hid}"

        # Meal plan
        meal_raw = (str(item.get("breakfastDesc") or item.get("mealType") or
                        item.get("breakfast") or "")).lower()
        if "sáng" in meal_raw or "breakfast" in meal_raw or meal_raw == "1":
            meal = "Có bữa sáng"
        else:
            meal = ""

        hotels.append({
            "Tỉnh thành / Điểm đến": destination,
            "Tên khách sạn": name,
            "Địa chỉ": (item.get("address") or item.get("positionDesc") or
                         item.get("zoneName") or ""),
            "Hạng sao": stars,
            "Điểm đánh giá": score,
            "Gói bữa ăn": meal,
            "Giá/đêm (VND)": price,
            "Chính sách hoàn hủy": "",
            "Link khách sạn": link,
        })
    return hotels


async def _scrape_async(url: str, destination: str, status_callback) -> list[dict]:
    """
    Trip.com scraper — dual strategy:
    A. API Interception (primary): capture Trip.com's XHR hotel-search API during
       page 1 load, then replay with incremented pageIndex for all subsequent pages.
       Detected by response CONTENT (>= 10 hotels), not fragile URL keywords.
    B. DOM + click-next (fallback): if API not found, click the pagination
       next-page button and extract from DOM each time.
    """
    import json as _json
    import math

    chromium = get_chromium_path()
    if not chromium:
        raise RuntimeError("Không tìm thấy Chromium. Vui lòng cài đặt.")

    cleaned = _clean_tripcom_url(url)
    if cleaned != url:
        status_callback("🧹 URL đã được làm sạch")
    url = cleaned

    results: list[dict] = []
    seen_keys: set[str] = set()
    api_cap: dict = {}   # captured API: url, method, headers, body_str, total

    # ── helper: capture any JSON response with >= 10 hotels ─────────────────
    async def on_response(resp):
        if api_cap.get("ready"):
            return
        if resp.status != 200:
            return
        ct = resp.headers.get("content-type", "")
        if "json" not in ct:
            return
        try:
            body = await resp.text()
            if len(body) < 300:
                return
            data = _json.loads(body)
            hotel_list = _find_hotel_list_in_json(data)
            if len(hotel_list) >= 10:
                req = resp.request
                api_cap["url"] = resp.url
                api_cap["method"] = req.method
                api_cap["headers"] = dict(req.headers)
                api_cap["body_str"] = req.post_data or ""
                api_cap["first_list"] = hotel_list
                api_cap["first_data"] = data
                # Extract total from API (more reliable than DOM)
                api_total = _extract_total(data)
                if api_total >= 10:
                    api_cap["api_total"] = api_total
                api_cap["ready"] = True
        except Exception:
            pass

    # ── next-page button JS ──────────────────────────────────────────────────
    NEXT_PAGE_JS = """() => {
        const candidates = [
            document.querySelector('.m-pager .next'),
            document.querySelector('[class*="paginationNext"]'),
            document.querySelector('[class*="next-btn"]'),
            document.querySelector('[class*="nextBtn"]'),
            document.querySelector('button[aria-label*="Next"]'),
            document.querySelector('button[aria-label*="next"]'),
            ...Array.from(document.querySelectorAll('[class*="pagination"] a, [class*="pager"] a'))
                .filter(a => a.textContent.trim() === '›' || a.getAttribute('aria-label') === 'next'),
        ].filter(Boolean);
        for (const btn of candidates) {
            if (!btn.disabled && !btn.classList.contains('disabled') && btn.offsetParent !== null) {
                btn.click();
                return 'clicked:' + (btn.className || btn.tagName);
            }
        }
        return '';
    }"""

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            executable_path=chromium,
            args=[
                "--no-sandbox", "--disable-setuid-sandbox",
                "--disable-dev-shm-usage", "--no-first-run",
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
            ]
        )
        ctx = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1366, "height": 768},
            locale="vi-VN",
            extra_http_headers={"Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7"},
        )
        page = await ctx.new_page()
        await page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        page.on("response", on_response)

        # ── Load page 1 ─────────────────────────────────────────────────────
        status_callback("🌐 Đang mở trang Trip.com...")
        try:
            await page.goto(url, wait_until="networkidle", timeout=60000)
        except PlaywrightTimeoutError:
            await page.goto(url, wait_until="domcontentloaded", timeout=35000)

        await asyncio.sleep(5)
        total_hotels = await _detect_total_hotels(page)
        if total_hotels:
            status_callback(f"📊 Trip.com: tổng ~{total_hotels} khách sạn")

        # Extract page 1 from DOM (always reliable for first page)
        p1_hotels = await _scroll_and_extract(page, destination, status_callback, "p1 ")
        _add_new(p1_hotels, results, seen_keys)
        status_callback(f"📄 Trang 1: {len(p1_hotels)} khách sạn — Tổng: {len(results)}")

        # Wait a bit for API interception to capture
        if not api_cap.get("ready"):
            await asyncio.sleep(2)

        # Update total from API response if available (more reliable than DOM)
        if api_cap.get("api_total", 0) > total_hotels:
            total_hotels = api_cap["api_total"]

        if api_cap.get("ready"):
            method = api_cap.get("method", "POST")
            has_body = bool(api_cap.get("body_str"))
            status_callback(
                f"🔗 API ({method}, body={has_body}) — "
                f"{len(api_cap['first_list'])} hotels/page — "
                f"total={total_hotels or '?'}"
            )
        else:
            status_callback("⚠️ Không bắt được API, thử click next-page...")

        # ── Determine total pages (generous cap — stop via empty API response) ──
        page_size = max(len(p1_hotels), 10) if p1_hotels else 25
        if total_hotels >= 10:
            total_pages = math.ceil(total_hotels / page_size)
        else:
            total_pages = 80  # loop until API says empty
        total_pages = min(total_pages, 80)
        status_callback(f"📊 Ước tính {total_pages} trang × ~{page_size} hotels")

        # ── Pages 2+ ─────────────────────────────────────────────────────────
        consecutive_empty = 0

        for pg in range(2, total_pages + 1):
            if len(results) >= 2000:
                status_callback("⚠️ Đạt giới hạn 2000.")
                break

            status_callback(f"📄 Đang lấy trang {pg}/{total_pages}...")
            got_new = False

            # ── Strategy A: replay API call ──────────────────────────────────
            if api_cap.get("ready"):
                try:
                    method = api_cap["method"]
                    api_url = api_cap["url"]
                    hdr_json = _json.dumps(api_cap["headers"])

                    if method == "GET" or not api_cap.get("body_str"):
                        # GET request: modify pageIndex in URL query string
                        pg_url = re.sub(r'[?&]pageIndex=\d+', '', api_url)
                        pg_url = re.sub(r'[?&]page=\d+', '', pg_url).rstrip("&?")
                        sep = "&" if "?" in pg_url else "?"
                        pg_url = f"{pg_url}{sep}pageIndex={pg - 1}"
                        fetch_js = f"""async () => {{
                            const r = await fetch({_json.dumps(pg_url)}, {{
                                method: 'GET',
                                headers: {hdr_json},
                                credentials: 'include'
                            }});
                            return await r.text();
                        }}"""
                    else:
                        # POST request: modify pageIndex in body
                        body_obj = _json.loads(api_cap["body_str"])
                        pg_key, pg_val = _find_page_key_in_body(body_obj)
                        if pg_key:
                            body_obj[pg_key] = (pg - 1) if pg_val == 0 else pg
                        else:
                            body_obj["pageIndex"] = pg - 1
                        new_body = _json.dumps(body_obj)
                        fetch_js = f"""async () => {{
                            const r = await fetch({_json.dumps(api_url)}, {{
                                method: 'POST',
                                headers: {hdr_json},
                                body: {_json.dumps(new_body)},
                                credentials: 'include'
                            }});
                            return await r.text();
                        }}"""

                    raw_text = await page.evaluate(fetch_js)
                    api_data = _json.loads(raw_text)
                    hotel_list = _find_hotel_list_in_json(api_data)

                    if hotel_list:
                        hotels_api = _hotels_from_api_list(hotel_list, destination)
                        added = _add_new(hotels_api, results, seen_keys)
                        status_callback(f"  → API: {len(hotel_list)} raw, +{added} mới (tổng: {len(results)})")
                        if added > 0:
                            got_new = True
                            consecutive_empty = 0
                        else:
                            # Same hotels returned — likely end of pagination
                            consecutive_empty += 1
                            if consecutive_empty >= 2:
                                status_callback("⚠️ API liên tục trả hotel đã có, dừng.")
                                break
                    else:
                        status_callback(f"  ⚠️ API trả về rỗng/lỗi (snippet: {raw_text[:120]})")
                        consecutive_empty += 1
                        if consecutive_empty >= 2:
                            status_callback("⚠️ API rỗng 2 lần, dừng.")
                            break

                except Exception as e:
                    status_callback(f"  ⚠️ API lỗi: {str(e)[:100]}")
                    consecutive_empty += 1
                    if consecutive_empty >= 3:
                        break

            # ── Strategy B: click next-page button ──────────────────────────
            if not got_new and not api_cap.get("ready"):
                clicked = await page.evaluate(NEXT_PAGE_JS)
                if clicked:
                    status_callback(f"  → Clicked: {clicked}")
                    await asyncio.sleep(4)
                    pg_hotels = await _scroll_and_extract(page, destination, status_callback, f"p{pg} ")
                    added = _add_new(pg_hotels, results, seen_keys)
                    status_callback(f"  → DOM: +{added} mới (tổng: {len(results)})")
                    if added == 0:
                        consecutive_empty += 1
                        if consecutive_empty >= 2:
                            status_callback("⚠️ Không có hotel mới, dừng.")
                            break
                    else:
                        consecutive_empty = 0
                else:
                    status_callback("⚠️ Không tìm được next-page button, dừng.")
                    break

        await browser.close()

    return results


def _add_new(hotels: list[dict], results: list[dict], seen_keys: set[str]) -> int:
    """Add hotels not yet in seen_keys to results. Returns count added."""
    added = 0
    for h in hotels:
        key = h.get("Link khách sạn") or h.get("Tên khách sạn") or ""
        if key and key not in seen_keys:
            seen_keys.add(key)
            results.append(h)
            added += 1
    return added


def _extract_total(data: dict) -> int:
    """Extract total hotel count from Trip.com API response."""
    try:
        for key in ("total", "totalCount", "totalNum", "count"):
            v = data.get(key)
            if isinstance(v, int) and v > 0:
                return v
        # Nested: data.data.total etc.
        inner = data.get("data") or data.get("result") or {}
        if isinstance(inner, dict):
            for key in ("total", "totalCount", "totalNum", "count"):
                v = inner.get(key)
                if isinstance(v, int) and v > 0:
                    return v
    except Exception:
        pass
    return 0


def _parse_hotels_from_api(data: dict, destination: str) -> list[dict]:
    """Parse hotel list from a Trip.com API JSON response."""
    hotels = []
    try:
        # Try various nested paths
        items = None
        for path in [
            lambda d: d.get("hotelList"),
            lambda d: d.get("data", {}).get("hotelList"),
            lambda d: d.get("data", {}).get("hotels"),
            lambda d: d.get("result", {}).get("hotelList"),
            lambda d: d.get("hotels"),
        ]:
            try:
                items = path(data)
                if items:
                    break
            except Exception:
                pass

        if not items:
            return []

        for item in items:
            hid = str(item.get("hotelId") or item.get("id") or "")
            name = item.get("hotelName") or item.get("name") or ""
            if not name or not hid:
                continue

            # Price — try common field names
            price_val = (item.get("price") or item.get("lowPrice") or
                         item.get("minPrice") or item.get("displayPrice") or 0)
            try:
                price_int = int(float(str(price_val).replace(",", "")))
                price = f"{price_int:,} VND".replace(",", ".") if price_int >= 50000 else ""
            except Exception:
                price = ""

            # Score
            score_val = item.get("score") or item.get("rating") or item.get("commentScore") or ""
            score = f"{score_val}/10" if score_val else ""

            # Stars
            star_val = item.get("star") or item.get("starLevel") or ""
            stars = f"{star_val} sao" if star_val else ""

            # Link
            link = item.get("hotelUrl") or item.get("url") or ""
            if link and not link.startswith("http"):
                link = "https://vn.trip.com" + link

            hotels.append({
                "Tỉnh thành / Điểm đến": destination,
                "Tên khách sạn": name,
                "Địa chỉ": item.get("address") or item.get("positionDesc") or "",
                "Hạng sao": stars,
                "Điểm đánh giá": score,
                "Gói bữa ăn": "",
                "Giá/đêm (VND)": price,
                "Chính sách hoàn hủy": "",
                "Link khách sạn": link,
            })
    except Exception:
        pass
    return hotels


def run_scrape_tripcom(url: str, destination: str, status_callback=None) -> list[dict]:
    """Synchronous entry point for Trip.com scraping."""
    if status_callback is None:
        status_callback = print
    return asyncio.run(_scrape_async(url, destination, status_callback))
