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
    const cards = document.querySelectorAll('div.hotel-card');
    const results = [];
    cards.forEach(card => {
        const hotelId = card.id;
        if (!hotelId) return;
        const fullText = card.textContent || '';
        
        // Name: first meaningful text node
        const nameSelectors = [
            '[class*="hotel-name"]', '[class*="hotelName"]',
            '[class*="name__"]', 'h2', 'h3',
            '[class*="title"]'
        ];
        let name = '';
        for (const sel of nameSelectors) {
            const el = card.querySelector(sel);
            if (el && el.textContent.trim().length > 2) {
                name = el.textContent.trim();
                break;
            }
        }
        
        // Link
        const linkEl = card.querySelector('a[href*="/hotels/detail/"]');
        const link = linkEl ? linkEl.href : '';
        
        results.push({ hotelId, name, fullText, link });
    });
    return results;
}"""


async def _extract_page_hotels(page, destination: str) -> list[dict]:
    """Extract all hotel cards visible on the current page."""
    raw = await page.evaluate(HOTEL_CARD_JS)
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


async def _get_total_pages(page) -> int:
    """Try to detect number of pages from pagination."""
    try:
        # Trip.com shows "Page X of Y" or similar
        body = await page.inner_text("body")
        m = re.search(r'(\d+)\s*(?:khách sạn|hotel)', body, re.IGNORECASE)
        if m:
            total_hotels = int(m.group(1))
            return max(1, (total_hotels + 9) // 10)
    except Exception:
        pass
    return 5  # default: try up to 5 pages


async def _go_next_page(page) -> bool:
    """Click the next page button. Returns True if successful."""
    try:
        next_btn_selectors = [
            "[class*='pagination'] [class*='next']:not([disabled])",
            "[class*='next-page']:not([disabled])",
            "button[aria-label*='next']:not([disabled])",
            "[class*='pagination'] li:last-child a",
        ]
        for sel in next_btn_selectors:
            el = await page.query_selector(sel)
            if el:
                is_disabled = await el.get_attribute("disabled")
                if is_disabled is None:
                    await el.click()
                    await asyncio.sleep(3)
                    return True
    except Exception:
        pass
    return False


# ---------------------------------------------------------------------------
# Main scrape runner
# ---------------------------------------------------------------------------

async def _scrape_async(url: str, destination: str, status_callback) -> list[dict]:
    chromium = get_chromium_path()
    if not chromium:
        raise RuntimeError("Không tìm thấy Chromium. Vui lòng cài đặt.")

    results = []
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            executable_path=chromium,
            args=["--no-sandbox", "--disable-setuid-sandbox",
                  "--disable-dev-shm-usage", "--no-first-run"]
        )
        ctx = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1366, "height": 768},
            locale="vi-VN",
        )
        page = await ctx.new_page()

        status_callback("🌐 Đang mở trang Trip.com...")
        try:
            await page.goto(url, wait_until="networkidle", timeout=55000)
        except PlaywrightTimeoutError:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)

        await asyncio.sleep(5)

        # Scroll to trigger lazy loading
        for _ in range(4):
            await page.evaluate("window.scrollBy(0, 600)")
            await asyncio.sleep(1)

        # Detect total pages
        total_pages = await _get_total_pages(page)
        total_pages = min(total_pages, 20)  # cap at 20 pages
        status_callback(f"📄 Phát hiện ~{total_pages} trang. Bắt đầu thu thập...")

        seen_ids: set[str] = set()
        for page_num in range(1, total_pages + 1):
            hotels = await _extract_page_hotels(page, destination)

            # Deduplicate
            new = [h for h in hotels if h["Link khách sạn"] not in seen_ids]
            for h in new:
                seen_ids.add(h["Link khách sạn"])
            results.extend(new)

            status_callback(f"📄 Trang {page_num}/{total_pages} — {len(new)} khách sạn — Tổng: {len(results)}")

            if page_num < total_pages:
                went = await _go_next_page(page)
                if not went:
                    status_callback("⚠️ Không tìm thấy nút trang tiếp theo, dừng.")
                    break
                await asyncio.sleep(3)
                for _ in range(4):
                    await page.evaluate("window.scrollBy(0, 600)")
                    await asyncio.sleep(1)

        await browser.close()

    return results


def run_scrape_tripcom(url: str, destination: str, status_callback=None) -> list[dict]:
    """Synchronous entry point for Trip.com scraping."""
    if status_callback is None:
        status_callback = print
    return asyncio.run(_scrape_async(url, destination, status_callback))
