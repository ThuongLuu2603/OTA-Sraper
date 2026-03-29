"""
travel.com.vn hotel scraper.

Strategy:
1. Load page with Playwright (handles any JS-rendered content).
2. Extract hotel cards from DOM using flexible selectors.
3. Detect total pages from pagination; navigate page=2, page=3, …
4. Returns: name, link, address, stars, rating, review count, price (VND), policy, source.

URL format: https://travel.com.vn/hotels/khach-san-tai-{slug}.aspx
            ?room=1&in=DD-MM-YYYY&out=DD-MM-YYYY&adults=2&children=0
            &hid={city_id}&cid={city_id}&htitle={city_name}
"""

import asyncio
import re
import shutil
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, quote
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

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

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


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
    """
    Build a travel.com.vn hotel listing URL.
    check_in / check_out: DD-MM-YYYY
    """
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
# DOM extraction helpers
# ---------------------------------------------------------------------------

HOTEL_CARD_SELECTORS = [
    ".hotel-item",
    ".item-hotel",
    ".hotel-card",
    ".result-item",
    "[class*='hotel-item']",
    "[class*='hotelItem']",
    ".list-hotel > div",
    ".hotel-list .item",
    "li.hotel",
    ".search-result-item",
]

NAME_SELECTORS = [
    "h2 a", "h3 a", ".hotel-name a", ".hotel-title a",
    ".name a", ".title a", "a.hotel-name", "a.name",
    ".hotel-name", ".hotel-title", "h2", "h3",
]

PRICE_SELECTORS = [
    ".price-room", ".hotel-price", ".price strong",
    ".price-sale", ".price-current", ".room-price",
    ".price", "strong.price", ".sale-price",
    "[class*='price']",
]

STAR_SELECTORS = [
    ".star-rating", ".hotel-star", ".stars",
    "[class*='star']",
]

RATING_SELECTORS = [
    ".rating-score", ".score", ".point",
    ".hotel-rating .score", ".rating-value",
    "[class*='rating']", "[class*='score']",
]

ADDR_SELECTORS = [
    ".hotel-address", ".address", ".location",
    "[class*='address']", "[class*='location']",
]


def _text(el) -> str:
    """Safe innerText strip."""
    try:
        return (el.inner_text() or "").strip()
    except Exception:
        return ""


async def _try_first(card, selectors: list[str]) -> str:
    """Try a list of CSS selectors, return text of first match."""
    for sel in selectors:
        try:
            el = card.locator(sel).first
            if await el.count() > 0:
                t = (await el.inner_text()).strip()
                if t:
                    return t
        except Exception:
            pass
    return ""


async def _try_attr(card, selectors: list[str], attr: str) -> str:
    for sel in selectors:
        try:
            el = card.locator(sel).first
            if await el.count() > 0:
                v = await el.get_attribute(attr)
                if v and v.strip():
                    return v.strip()
        except Exception:
            pass
    return ""


def _parse_vnd(text: str) -> str:
    """Extract cheapest VND price from text."""
    if not text:
        return ""
    nums = re.findall(r'[\d.,]+', text.replace("đ", "").replace("₫", ""))
    parsed = []
    for n in nums:
        clean = n.replace(".", "").replace(",", "")
        try:
            v = int(clean)
            if v >= 50000:
                parsed.append(v)
        except Exception:
            pass
    if not parsed:
        return ""
    return f"{min(parsed):,.0f} VND".replace(",", ".")


def _parse_stars(text: str) -> str:
    if not text:
        return ""
    m = re.search(r'(\d(?:[.,]\d)?)\s*[★⭐sao*]', text, re.I)
    if m:
        return f"{m.group(1)} sao"
    # Count star characters
    count = text.count("★") or text.count("⭐") or text.count("*")
    if count:
        return f"{count} sao"
    return ""


def _parse_score(text: str) -> str:
    if not text:
        return ""
    m = re.search(r'(\d+(?:[.,]\d+)?)', text)
    if m:
        v = float(m.group(1).replace(",", "."))
        if 1 <= v <= 10:
            return f"{v}/10"
        if 10 < v <= 100:
            return f"{v/10:.1f}/10"
    return ""


async def _extract_hotels_from_page(page, destination: str, status_callback) -> list[dict]:
    """Extract all hotel cards from current page DOM."""
    hotels = []
    card_sel = None

    for sel in HOTEL_CARD_SELECTORS:
        try:
            count = await page.locator(sel).count()
            if count >= 2:
                card_sel = sel
                break
        except Exception:
            pass

    if not card_sel:
        status_callback("  ⚠️ Không tìm được hotel card selector")
        return []

    cards = page.locator(card_sel)
    n = await cards.count()
    status_callback(f"  🃏 Tìm thấy {n} cards ({card_sel})")

    for i in range(n):
        try:
            card = cards.nth(i)

            # Name + link
            name = ""
            link = ""
            for sel in NAME_SELECTORS:
                try:
                    el = card.locator(sel).first
                    if await el.count() > 0:
                        name = (await el.inner_text()).strip()
                        href = await el.get_attribute("href")
                        if href:
                            link = href if href.startswith("http") else f"https://travel.com.vn{href}"
                        if name:
                            break
                except Exception:
                    pass

            if not name:
                continue

            price_raw = await _try_first(card, PRICE_SELECTORS)
            price = _parse_vnd(price_raw)

            stars_raw = await _try_attr(card, STAR_SELECTORS, "data-star") or \
                        await _try_attr(card, STAR_SELECTORS, "class") or \
                        await _try_first(card, STAR_SELECTORS)
            stars = _parse_stars(stars_raw)

            rating_raw = await _try_first(card, RATING_SELECTORS)
            rating = _parse_score(rating_raw)

            addr = await _try_first(card, ADDR_SELECTORS)

            # Review count
            review_raw = await _try_first(card, [".review-count", ".num-review", ".count-review",
                                                  "[class*='review']", ".comment-count"])
            review_count = ""
            m = re.search(r'(\d+)', review_raw)
            if m:
                review_count = m.group(1)

            # Cancellation
            policy_raw = await _try_first(card, [".cancel-policy", ".free-cancel", ".policy",
                                                  "[class*='cancel']", "[class*='policy']"])
            if "miễn phí" in policy_raw.lower() or "free" in policy_raw.lower():
                policy = "Hủy miễn phí"
            elif policy_raw:
                policy = policy_raw[:60]
            else:
                policy = ""

            hotels.append({
                "Tên khách sạn": name,
                "Link khách sạn": link,
                "Địa chỉ": addr,
                "Hạng sao": stars,
                "Điểm đánh giá": rating,
                "Số đánh giá": review_count,
                "Giá/đêm (VND)": price,
                "Chính sách hoàn hủy": policy,
                "Nguồn": "travel.com.vn",
                "Điểm đến": destination,
            })
        except Exception:
            continue

    return hotels


async def _detect_total_pages(page) -> int:
    """Detect the last page number from pagination links."""
    try:
        # Try to find pagination and get last page number
        pager = page.locator(".pagination, .pager, [class*='paging'], [class*='paginat']")
        if await pager.count() > 0:
            pager_text = await pager.first.inner_text()
            nums = re.findall(r'\b(\d+)\b', pager_text)
            if nums:
                return max(int(n) for n in nums)
        # Try page links directly
        links = page.locator("a[href*='page='], a[href*='Page='], a[href*='p=']")
        if await links.count() > 0:
            nums = []
            for idx in range(await links.count()):
                href = await links.nth(idx).get_attribute("href") or ""
                m = re.search(r'[Pp]age=(\d+)|[&?]p=(\d+)', href)
                if m:
                    nums.append(int(m.group(1) or m.group(2)))
            if nums:
                return max(nums)
    except Exception:
        pass
    return 1


def _make_page_n_url(base_url: str, page_num: int) -> str:
    """Add or replace page parameter in URL."""
    parsed = urlparse(base_url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    params["page"] = [str(page_num)]
    new_query = urlencode({k: v[0] for k, v in params.items()})
    return urlunparse(parsed._replace(query=new_query))


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
    return asyncio.run(_scrape_async(url, destination, status_callback))


async def _scrape_async(url: str, destination: str, status_callback) -> list[dict]:
    chromium = get_chromium()
    if not chromium:
        raise RuntimeError("Không tìm thấy Chromium. Hãy cài 'chromium-browser'.")

    results: list[dict] = []
    seen_keys: set[str] = set()

    def _add(hotels: list[dict]) -> int:
        added = 0
        for h in hotels:
            key = h.get("Link khách sạn") or h.get("Tên khách sạn") or ""
            key = key.lower().strip()
            if key and key not in seen_keys:
                seen_keys.add(key)
                results.append(h)
                added += 1
        return added

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            executable_path=chromium,
            args=[
                "--no-sandbox", "--disable-setuid-sandbox",
                "--disable-dev-shm-usage", "--no-first-run",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        ctx = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1366, "height": 768},
            locale="vi-VN",
            extra_http_headers={"Accept-Language": "vi-VN,vi;q=0.9"},
        )
        page = await ctx.new_page()
        await page.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
        )

        # ── Page 1 ──────────────────────────────────────────────────────────
        status_callback("🌐 Đang mở travel.com.vn...")
        try:
            await page.goto(url, wait_until="networkidle", timeout=45000)
        except PlaywrightTimeoutError:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)

        await asyncio.sleep(3)

        total_pages = await _detect_total_pages(page)
        status_callback(f"📊 Phát hiện {total_pages} trang")

        hotels_p1 = await _extract_hotels_from_page(page, destination, status_callback)
        added = _add(hotels_p1)
        status_callback(f"📄 Trang 1: {len(hotels_p1)} khách sạn, +{added} mới (tổng: {len(results)})")

        # ── Pages 2+ ─────────────────────────────────────────────────────────
        consecutive_empty = 0
        for pg in range(2, min(total_pages + 1, 81)):
            if len(results) >= 2000:
                status_callback("⚠️ Đạt giới hạn 2000.")
                break

            status_callback(f"📄 Trang {pg}/{total_pages}...")
            pg_url = _make_page_n_url(url, pg)

            try:
                await page.goto(pg_url, wait_until="networkidle", timeout=30000)
            except PlaywrightTimeoutError:
                await page.goto(pg_url, wait_until="domcontentloaded", timeout=20000)

            await asyncio.sleep(2)

            hotels_pg = await _extract_hotels_from_page(page, destination, status_callback)
            added = _add(hotels_pg)
            status_callback(f"  → +{added} mới (tổng: {len(results)})")

            if added > 0:
                consecutive_empty = 0
            else:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    status_callback("⚠️ 3 trang liên tiếp không có mới, dừng.")
                    break

        await browser.close()

    return results
