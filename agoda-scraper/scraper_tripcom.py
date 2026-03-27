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
            "Gói bữa ăn": "",
            "Giá/đêm (VND)": price,
            "Chính sách hoàn hủy": cancellation,
            "Link khách sạn": link,
        })
    return hotels


def _make_page_url(base_url: str, page_num: int) -> str:
    """Build URL for a specific page number by setting the page= param."""
    url = re.sub(r'([?&])page=\d+', '', base_url).rstrip('&').rstrip('?')
    sep = '&' if '?' in url else '?'
    if page_num == 1:
        return url
    return f"{url}{sep}page={page_num}"


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


async def _scrape_async(url: str, destination: str, status_callback) -> list[dict]:
    chromium = get_chromium_path()
    if not chromium:
        raise RuntimeError("Không tìm thấy Chromium. Vui lòng cài đặt.")

    # Clean URL: remove marketing/filter params that change page rendering
    cleaned = _clean_tripcom_url(url)
    if cleaned != url:
        status_callback(f"🧹 URL đã được làm sạch để tránh lỗi tải trang")
    url = cleaned

    results = []
    seen_links: set[str] = set()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            executable_path=chromium,
            args=[
                "--no-sandbox", "--disable-setuid-sandbox",
                "--disable-dev-shm-usage", "--no-first-run",
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--window-size=1366,768",
            ]
        )
        ctx = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1366, "height": 768},
            locale="vi-VN",
            extra_http_headers={
                "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            }
        )
        page = await ctx.new_page()
        # Hide webdriver fingerprint
        await page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        status_callback("🌐 Đang mở trang Trip.com...")
        page_num = 1
        total_hotels = 0
        hotels_per_page = 15  # Trip.com default; recalculated after page 1

        while True:
            page_url = _make_page_url(url, page_num)
            try:
                await page.goto(page_url, wait_until="networkidle", timeout=55000)
            except PlaywrightTimeoutError:
                try:
                    await page.goto(page_url, wait_until="domcontentloaded", timeout=30000)
                except Exception:
                    status_callback(f"⚠️ Không thể tải trang {page_num}, dừng.")
                    break

            await asyncio.sleep(4)

            # Scroll to trigger lazy loading
            for _ in range(5):
                await page.evaluate("window.scrollBy(0, 700)")
                await asyncio.sleep(0.8)
            await asyncio.sleep(1)

            # On first page: detect total hotel count
            if page_num == 1:
                total_hotels = await _detect_total_hotels(page)
                if total_hotels:
                    status_callback(f"📊 Trip.com báo tổng {total_hotels} khách sạn. Bắt đầu thu thập...")
                else:
                    status_callback("📄 Bắt đầu thu thập (không phát hiện được tổng số)...")

            hotels = await _extract_page_hotels(page, destination, status_callback)
            new = [h for h in hotels if h["Link khách sạn"] not in seen_links]
            for h in new:
                if h["Link khách sạn"]:
                    seen_links.add(h["Link khách sạn"])
            results.extend(new)

            status_callback(f"📄 Trang {page_num} — +{len(new)} mới — Tổng: {len(results)}")

            # Recalculate hotels_per_page from first non-empty result
            if page_num == 1 and hotels:
                hotels_per_page = max(len(hotels), 10)

            # Stop conditions
            if len(new) == 0:
                # Run diagnostic to understand what's on the page
                try:
                    diag = await page.evaluate(DIAG_JS)
                    status_callback(f"🔍 Diagnostic: {diag.get('counts',{})} | classes: {diag.get('hotelClasses',[])} | page: {diag.get('bodySnippet','')[:150]}")
                except Exception:
                    pass
                # Try once more after extra wait (sometimes lazy-loading is slow)
                await asyncio.sleep(3)
                hotels2 = await _extract_page_hotels(page, destination, status_callback)
                new2 = [h for h in hotels2 if h["Link khách sạn"] not in seen_links]
                if not new2:
                    status_callback("⚠️ Trang không có khách sạn mới, dừng phân trang.")
                    break
                for h in new2:
                    if h["Link khách sạn"]:
                        seen_links.add(h["Link khách sạn"])
                results.extend(new2)
                status_callback(f"  → Retry: +{len(new2)} mới (tổng: {len(results)})")

            # Check if we've collected enough based on total
            if total_hotels and len(results) >= total_hotels:
                status_callback(f"✅ Đã thu thập đủ {len(results)} khách sạn.")
                break

            # Estimate if more pages remain
            estimated_total_pages = max(1, (total_hotels + hotels_per_page - 1) // hotels_per_page) if total_hotels else 50
            if page_num >= estimated_total_pages:
                break

            # Safety cap
            if len(results) >= 2000 or page_num >= 50:
                status_callback("⚠️ Đã đạt giới hạn, dừng.")
                break

            page_num += 1

        await browser.close()

    return results


def run_scrape_tripcom(url: str, destination: str, status_callback=None) -> list[dict]:
    """Synchronous entry point for Trip.com scraping."""
    if status_callback is None:
        status_callback = print
    return asyncio.run(_scrape_async(url, destination, status_callback))
