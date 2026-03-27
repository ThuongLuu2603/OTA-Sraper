import asyncio
import random
import re
import shutil
from datetime import datetime
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

AGODA_SEARCH_BASE = "https://www.agoda.com/search"


def build_agoda_url(
    destination: str,
    check_in: str,
    check_out: str,
    rooms: int = 1,
    adults: int = 2,
    children: int = 0,
    child_ages: list = None
) -> str:
    """Build Agoda search URL from parameters."""
    if child_ages is None:
        child_ages = []

    check_in_dt = datetime.strptime(check_in, "%Y-%m-%d")
    check_out_dt = datetime.strptime(check_out, "%Y-%m-%d")
    nights = (check_out_dt - check_in_dt).days

    params = (
        f"?city={destination}"
        f"&checkIn={check_in}"
        f"&checkOut={check_out}"
        f"&rooms={rooms}"
        f"&adults={adults}"
        f"&children={children}"
        f"&los={nights}"
        f"&priceCur=VND"
        f"&localised=true"
    )

    if children > 0 and child_ages:
        ages_str = ",".join(str(a) for a in child_ages[:children])
        params += f"&childAges={ages_str}"

    return AGODA_SEARCH_BASE + params


async def scroll_to_load_all(page, status_callback=None, max_scrolls: int = 50):
    """Scroll down repeatedly to trigger lazy-loading of all hotel cards."""
    last_height = await page.evaluate("document.body.scrollHeight")
    no_change_count = 0

    for _ in range(max_scrolls):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(random.uniform(1.5, 2.8))

        # Try clicking "Load more" button if present
        try:
            load_more_btn = page.locator(
                "button:has-text('Load more'), button:has-text('Xem thêm'), [data-element-name='load-more-button']"
            )
            if await load_more_btn.count() > 0:
                await load_more_btn.first.click()
                await asyncio.sleep(random.uniform(2.0, 3.5))
        except Exception:
            pass

        new_height = await page.evaluate("document.body.scrollHeight")

        if new_height == last_height:
            no_change_count += 1
            if no_change_count >= 3:
                break
        else:
            no_change_count = 0
            last_height = new_height

        if status_callback:
            count = await page.locator("[data-selenium='hotel-item'], [data-hotelid], .PropertyCard").count()
            status_callback(f"Đang tải... Đã tìm thấy khoảng {count} khách sạn")


def clean_price(price_text: str) -> str:
    """Normalize price text."""
    if not price_text:
        return ""
    return re.sub(r"\s+", " ", price_text.strip())


async def extract_hotel_data(card, destination: str) -> dict:
    """Extract data from a single hotel card element."""
    data = {
        "Tỉnh thành / Điểm đến": destination,
        "Tên khách sạn": "",
        "Địa chỉ": "",
        "Hạng sao": "",
        "Giá thấp nhất (đã gồm thuế & phí)": "",
        "Meal Plan": "",
        "Chính sách hoàn hủy": "",
    }

    # Hotel name
    try:
        for sel in [
            "[data-selenium='hotel-name']",
            "h3[class*='PropertyCard']",
            "h3[class*='hotel-name']",
            ".PropertyCard__HotelName",
            "span[data-selenium='hotel-name']",
            "h3",
        ]:
            el = card.locator(sel).first
            if await el.count() > 0:
                text = (await el.text_content() or "").strip()
                if text:
                    data["Tên khách sạn"] = text
                    break
    except Exception:
        pass

    # Address
    try:
        for sel in [
            "[data-selenium='area-city-text']",
            ".PropertyCard__Address",
            "[class*='PropertyCardAddress']",
            "[class*='location']",
            "span[class*='area']",
        ]:
            el = card.locator(sel).first
            if await el.count() > 0:
                text = (await el.text_content() or "").strip()
                if text:
                    data["Địa chỉ"] = text
                    break
    except Exception:
        pass

    # Star rating
    try:
        for sel in [
            "[data-selenium='hotel-star-rating']",
            "[class*='star-rating']",
            "[aria-label*='star']",
            "[class*='StarRating']",
            "span[class*='stars']",
        ]:
            el = card.locator(sel).first
            if await el.count() > 0:
                text = (await el.text_content() or "").strip()
                if not text:
                    text = await el.get_attribute("aria-label") or ""
                if text:
                    data["Hạng sao"] = text
                    break
        if not data["Hạng sao"]:
            stars = await card.locator("[class*='star'][class*='filled'], .star.filled, svg[class*='star']").count()
            if stars > 0:
                data["Hạng sao"] = f"{stars} sao"
    except Exception:
        pass

    # Price (lowest, including taxes)
    try:
        for sel in [
            "[data-selenium='display-price']",
            "[class*='PriceDisplay']",
            "[class*='price-display']",
            "[class*='finalPrice']",
            "[data-element-name='final-price']",
            "span[class*='Price']",
            "[class*='total-price']",
        ]:
            el = card.locator(sel).first
            if await el.count() > 0:
                text = (await el.text_content() or "").strip()
                if text and any(c.isdigit() for c in text):
                    data["Giá thấp nhất (đã gồm thuế & phí)"] = clean_price(text)
                    break
        if not data["Giá thấp nhất (đã gồm thuế & phí)"]:
            el = card.locator("[class*='price'], [class*='Price']").first
            if await el.count() > 0:
                text = (await el.text_content() or "").strip()
                if text and any(c.isdigit() for c in text):
                    data["Giá thấp nhất (đã gồm thuế & phí)"] = clean_price(text)
    except Exception:
        pass

    # Meal plan
    try:
        for sel in [
            "[data-selenium='meal-plan']",
            "[class*='MealPlan']",
            "[class*='meal-plan']",
            "[class*='breakfast']",
            "span[class*='BoardBasis']",
            "[data-element-name='board-basis']",
        ]:
            el = card.locator(sel).first
            if await el.count() > 0:
                text = (await el.text_content() or "").strip()
                if text:
                    data["Meal Plan"] = text
                    break
    except Exception:
        pass

    # Cancellation policy
    try:
        for sel in [
            "[data-selenium='cancellation-policy']",
            "[class*='CancellationPolicy']",
            "[class*='cancellation']",
            "[class*='refundable']",
            "[class*='FreeCancellation']",
            "span[class*='cancel']",
        ]:
            el = card.locator(sel).first
            if await el.count() > 0:
                text = (await el.text_content() or "").strip()
                if text:
                    data["Chính sách hoàn hủy"] = text
                    break
    except Exception:
        pass

    return data


async def scrape_agoda(url: str, destination: str, status_callback=None) -> list:
    """
    Main scraping function. Returns ALL hotels found on the page.
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
            await asyncio.sleep(random.uniform(2.5, 4.0))

            if status_callback:
                status_callback("Trang đã tải, đang scroll để load toàn bộ khách sạn...")

            await scroll_to_load_all(page, status_callback=status_callback)

            if status_callback:
                status_callback("Đang phân tích dữ liệu khách sạn...")

            # Try multiple selectors to find hotel cards
            cards = None
            for sel in [
                "[data-selenium='hotel-item']",
                "[data-hotelid]",
                ".PropertyCard",
                "[class*='PropertyCard']",
                "li[class*='hotel']",
                "[data-element-name='property-card']",
            ]:
                candidate = page.locator(sel)
                count = await candidate.count()
                if count > 0:
                    cards = candidate
                    if status_callback:
                        status_callback(f"Tìm thấy {count} khách sạn, đang trích xuất dữ liệu...")
                    break

            if cards is None or await cards.count() == 0:
                if status_callback:
                    html = await page.content()
                    status_callback(f"Không tìm thấy thẻ khách sạn. HTML dài {len(html)} ký tự.")
                await browser.close()
                return []

            total_cards = await cards.count()

            for i in range(total_cards):
                try:
                    card = cards.nth(i)
                    hotel_data = await extract_hotel_data(card, destination)
                    if hotel_data["Tên khách sạn"]:
                        results.append(hotel_data)
                    if status_callback and (i + 1) % 5 == 0:
                        status_callback(f"Đã xử lý {i + 1}/{total_cards} khách sạn...")
                    await asyncio.sleep(random.uniform(0.05, 0.15))
                except Exception:
                    continue

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
