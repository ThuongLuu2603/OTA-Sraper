import asyncio
import random
import re
import shutil
import urllib.request
import json
from datetime import datetime
from urllib.parse import quote
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError


def get_chromium_path() -> str | None:
    """Find a working system Chromium binary."""
    for name in ["chromium-browser", "chromium", "google-chrome", "google-chrome-stable"]:
        path = shutil.which(name)
        if path:
            return path
    return None


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]

AGODA_SEARCH_BASE = "https://www.agoda.com/vi-vn/search"
AGODA_SUGGEST_API = (
    "https://www.agoda.com/api/cronos/search/GetUnifiedSuggestResult/3/24/24/0/vi-vn/"
    "?searchText={query}&guid=abc123&origin=VN&cid=-1&pageTypeId=1"
)


def resolve_city_id(destination: str) -> tuple[int, str]:
    """
    Resolve a destination name to an Agoda city ID using the suggest API.
    Returns (city_id, object_type) where object_type is 'city' or 'area'.
    """
    query = quote(destination)
    url = AGODA_SUGGEST_API.format(query=query)
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
            "Referer": "https://www.agoda.com/vi-vn/",
            "Accept": "application/json",
        }
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        items = data.get("ViewModelList", [])
        # Prefer city-level results (ObjectTypeId == 1, SearchType == 1)
        for item in items:
            if item.get("ObjectTypeId") == 1 and item.get("SearchType") == 1:
                city_id = item.get("ObjectId") or item.get("CityId")
                if city_id:
                    return city_id, "city"
        # Fallback: any result with a city ID
        for item in items:
            city_id = item.get("ObjectId") or item.get("CityId")
            if city_id and city_id > 0:
                return city_id, "area"
    except Exception:
        pass
    return 0, ""


def build_agoda_url(
    destination: str,
    check_in: str,
    check_out: str,
    rooms: int = 1,
    adults: int = 2,
    children: int = 0,
    child_ages: list = None
) -> str:
    """
    Build an Agoda search URL from form parameters.
    Resolves the destination name to a city ID via Agoda's suggest API.
    """
    if child_ages is None:
        child_ages = []

    check_in_dt = datetime.strptime(check_in, "%Y-%m-%d")
    check_out_dt = datetime.strptime(check_out, "%Y-%m-%d")
    nights = max(1, (check_out_dt - check_in_dt).days)

    city_id, obj_type = resolve_city_id(destination)

    if city_id:
        dest_param = f"city={city_id}"
    else:
        # Fallback: encode name (may not work but better than nothing)
        dest_param = f"textToSearch={quote(destination)}"

    params = (
        f"?{dest_param}"
        f"&checkIn={check_in}"
        f"&checkOut={check_out}"
        f"&rooms={rooms}"
        f"&adults={adults}"
        f"&children={children}"
        f"&los={nights}"
        f"&priceCur=VND"
        f"&productType=-1"
    )

    if children > 0 and child_ages:
        ages_str = ",".join(str(a) for a in child_ages[:children])
        params += f"&childAges={ages_str}"

    return AGODA_SEARCH_BASE + params


def clean_text(text: str) -> str:
    """Clean and normalize whitespace in text."""
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.strip())


async def scroll_to_load_cards(page, status_callback=None) -> int:
    """
    Scroll gradually to trigger lazy-loading of all hotel cards on the current page.
    Returns the final count of hotel cards found.
    """
    scroll_step = 400
    pause = 1.3
    stable_count = 0
    last_count = 0
    min_scrolls = 25  # Always do at least this many scrolls

    for i in range(70):
        await page.evaluate(f"window.scrollBy(0, {scroll_step})")
        await asyncio.sleep(pause)

        current_count = await page.locator("[data-selenium='hotel-item']").count()

        if current_count != last_count:
            last_count = current_count
            stable_count = 0
            if status_callback:
                status_callback(f"Đang tải trang... {current_count} khách sạn")
        else:
            stable_count += 1
            # Only stop early if we have done enough scrolls and count is stable
            if i >= min_scrolls and stable_count >= 5:
                break

    return await page.locator("[data-selenium='hotel-item']").count()


async def get_total_pages(page) -> int:
    """Parse the pagination text to get total number of pages."""
    try:
        pg_text_el = page.locator("[data-selenium='pagination-text']")
        if await pg_text_el.count() > 0:
            text = (await pg_text_el.text_content() or "").strip()
            # text like "Trang 1 trên 94"
            match = re.search(r"trên\s+(\d+)", text)
            if match:
                return int(match.group(1))
    except Exception:
        pass
    return 1


async def go_to_next_page(page) -> bool:
    """Click the next-page button. Returns True if successful."""
    try:
        next_btn = page.locator("[data-selenium='pagination-next-btn']")
        if await next_btn.count() > 0:
            is_disabled = await next_btn.get_attribute("disabled")
            if is_disabled is not None:
                return False
            await next_btn.click()
            await asyncio.sleep(random.uniform(2.5, 4.0))
            return True
    except Exception:
        pass
    return False


async def extract_star_rating(card) -> str:
    """
    Count filled star icons (a7de9-fill-product-rating-hotels) in a card.
    Agoda uses CSS masked SVGs for stars.
    """
    try:
        # Try finding a text like "X sao trên 5" (review score)
        rating_el = card.locator("[class*='rating-container']").first
        if await rating_el.count() > 0:
            aria = await rating_el.get_attribute("aria-label") or ""
            if aria:
                return clean_text(aria)

        # Count filled star icons (hotel category stars)
        filled = await card.locator("[class*='fill-product-rating-hotels']").count()
        if filled > 0:
            return f"{filled} sao"

        # Try generic star count via SVG icons
        stars_total = await card.locator("svg[class*='star'], [class*='StarRating'] svg").count()
        if stars_total > 0:
            return f"{stars_total} sao"
    except Exception:
        pass
    return ""


async def extract_card_data(card, destination: str) -> dict | None:
    """Extract all fields from a single hotel card. Returns None if no name found."""

    # --- Hotel name ---
    name = ""
    try:
        el = card.locator("[data-selenium='hotel-name']").first
        if await el.count() > 0:
            name = clean_text(await el.text_content() or "")
    except Exception:
        pass
    if not name:
        return None

    # --- Address ---
    address = ""
    try:
        el = card.locator("[data-selenium='area-city-text']").first
        if await el.count() > 0:
            address = clean_text(await el.text_content() or "")
    except Exception:
        pass

    # --- Star rating ---
    stars = await extract_star_rating(card)

    # --- Price (before tax) + currency ---
    price = ""
    try:
        price_val = ""
        currency = ""
        el_price = card.locator("[data-selenium='display-price']").first
        el_curr = card.locator("[data-selenium='hotel-currency']").first
        if await el_price.count() > 0:
            price_val = clean_text(await el_price.text_content() or "")
        if await el_curr.count() > 0:
            currency = clean_text(await el_curr.text_content() or "")
        if price_val:
            price = f"{price_val} {currency}".strip()
        # Fallback to PropertyCardPrice class
        if not price:
            el_fallback = card.locator("[class*='PropertyCardPrice']").first
            if await el_fallback.count() > 0:
                price = clean_text(await el_fallback.text_content() or "")
    except Exception:
        pass

    # --- Room benefit = cancellation policy ---
    cancellation = ""
    try:
        el = card.locator("[data-selenium='room-benefit']").first
        if await el.count() > 0:
            cancellation = clean_text(await el.text_content() or "")
    except Exception:
        pass

    # --- Meal plan ---
    meal_plan = ""
    try:
        # Try common meal plan selectors
        for sel in [
            "[data-selenium='meal-plan']",
            "[class*='MealPlan']",
            "[class*='meal-plan']",
            "[class*='breakfast']",
            "[class*='BoardBasis']",
        ]:
            el = card.locator(sel).first
            if await el.count() > 0:
                txt = clean_text(await el.text_content() or "")
                if txt:
                    meal_plan = txt
                    break
        # Sometimes meal info is inside room-benefit text (e.g. "Bữa sáng miễn phí")
        if not meal_plan and cancellation:
            lower = cancellation.lower()
            if "sáng" in lower or "ăn" in lower or "breakfast" in lower:
                meal_plan = cancellation
                cancellation = ""
    except Exception:
        pass

    return {
        "Tỉnh thành / Điểm đến": destination,
        "Tên khách sạn": name,
        "Địa chỉ": address,
        "Hạng sao": stars,
        "Giá thấp nhất (chưa gồm thuế)": price,
        "Meal Plan": meal_plan,
        "Chính sách hoàn hủy": cancellation,
    }


async def scrape_agoda(url: str, destination: str, status_callback=None) -> list:
    """
    Main scraping function.
    Iterates through all pages of Agoda search results and extracts hotel data.
    """
    results = []

    async with async_playwright() as pw:
        launch_kwargs = dict(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--window-size=1366,768",
            ]
        )
        system_chromium = get_chromium_path()
        if system_chromium:
            launch_kwargs["executable_path"] = system_chromium

        browser = await pw.chromium.launch(**launch_kwargs)

        ua = random.choice(USER_AGENTS)
        context = await browser.new_context(
            user_agent=ua,
            viewport={"width": 1366, "height": 768},
            locale="vi-VN",
            extra_http_headers={
                "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            }
        )
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            Object.defineProperty(navigator, 'languages', { get: () => ['vi-VN', 'vi', 'en-US', 'en'] });
            window.chrome = { runtime: {} };
        """)

        page = await context.new_page()

        try:
            if status_callback:
                status_callback("Đang mở trang Agoda...")

            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(random.uniform(3.0, 5.0))

            # Check if page loaded properly
            if await page.locator("[data-selenium='hotel-item']").count() == 0:
                # Try waiting for network to settle
                try:
                    await page.wait_for_load_state("networkidle", timeout=15000)
                except Exception:
                    pass
                await asyncio.sleep(3)

            total_pages = await get_total_pages(page)
            if status_callback:
                status_callback(f"Tìm thấy {total_pages} trang kết quả. Bắt đầu thu thập...")

            current_page = 1

            while True:
                if status_callback:
                    status_callback(f"Đang xử lý trang {current_page}/{total_pages}...")

                # Scroll to load all cards on this page
                card_count = await scroll_to_load_cards(page, status_callback)

                if card_count == 0:
                    if status_callback:
                        status_callback(f"Trang {current_page}: không tìm thấy khách sạn.")
                    break

                if status_callback:
                    status_callback(f"Trang {current_page}: tìm thấy {card_count} khách sạn. Đang trích xuất...")

                # Extract data from each card
                cards = page.locator("[data-selenium='hotel-item']")
                for i in range(card_count):
                    try:
                        card = cards.nth(i)
                        data = await extract_card_data(card, destination)
                        if data:
                            results.append(data)
                        if status_callback and (i + 1) % 10 == 0:
                            status_callback(f"  Đã xử lý {i+1}/{card_count} khách sạn (trang {current_page})")
                        await asyncio.sleep(0.05)
                    except Exception:
                        continue

                if status_callback:
                    status_callback(f"✅ Trang {current_page}: thu thập {card_count} khách sạn. Tổng: {len(results)}")

                # Navigate to next page
                if current_page >= total_pages:
                    break

                if status_callback:
                    status_callback(f"Chuyển sang trang {current_page + 1}...")

                # Scroll back to top before clicking next
                await page.evaluate("window.scrollTo(0, 0)")
                await asyncio.sleep(1)

                moved = await go_to_next_page(page)
                if not moved:
                    if status_callback:
                        status_callback("Không thể chuyển trang. Dừng.")
                    break

                current_page += 1
                await asyncio.sleep(random.uniform(1.5, 2.5))

        except PlaywrightTimeoutError:
            if status_callback:
                status_callback("Timeout khi tải trang. Kiểm tra URL hoặc kết nối mạng.")
        except Exception as e:
            if status_callback:
                status_callback(f"Lỗi: {str(e)}")
        finally:
            await browser.close()

    return results


def run_scrape(url: str, destination: str, status_callback=None) -> list:
    """Synchronous wrapper for the async scraping function."""
    return asyncio.run(scrape_agoda(url, destination, status_callback))
