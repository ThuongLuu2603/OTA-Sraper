import asyncio
import random
import time
import re
from datetime import datetime
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

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

    check_in_month = check_in_dt.month
    check_in_day = check_in_dt.day
    check_in_year = check_in_dt.year

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

    url = AGODA_SEARCH_BASE + params
    return url


async def scroll_to_load_all(page, status_callback=None, max_scrolls: int = 30):
    """Scroll down repeatedly to trigger lazy-loading of hotel cards."""
    last_height = await page.evaluate("document.body.scrollHeight")
    scroll_count = 0

    while scroll_count < max_scrolls:
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(random.uniform(1.5, 2.8))

        new_height = await page.evaluate("document.body.scrollHeight")

        try:
            load_more_btn = page.locator(
                "button:has-text('Load more'), button:has-text('Xem thêm'), [data-element-name='load-more-button']"
            )
            if await load_more_btn.count() > 0:
                await load_more_btn.first.click()
                await asyncio.sleep(random.uniform(2.0, 3.5))
        except Exception:
            pass

        if new_height == last_height:
            scroll_count += 1
            if scroll_count >= 3:
                break
        else:
            scroll_count = 0
            last_height = new_height

        if status_callback:
            count = await page.locator("[data-selenium='hotel-item'], [data-hotelid], .PropertyCard").count()
            status_callback(f"Đang tải... Đã tìm thấy khoảng {count} khách sạn")


def clean_price(price_text: str) -> str:
    """Normalize price text."""
    if not price_text:
        return ""
    price_text = price_text.strip()
    price_text = re.sub(r"\s+", " ", price_text)
    return price_text


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

    try:
        name_selectors = [
            "[data-selenium='hotel-name']",
            "h3[class*='PropertyCard']",
            "h3[class*='hotel-name']",
            ".PropertyCard__HotelName",
            "span[data-selenium='hotel-name']",
            "h3",
        ]
        for sel in name_selectors:
            el = card.locator(sel).first
            if await el.count() > 0:
                data["Tên khách sạn"] = (await el.text_content() or "").strip()
                if data["Tên khách sạn"]:
                    break
    except Exception:
        pass

    try:
        addr_selectors = [
            "[data-selenium='area-city-text']",
            ".PropertyCard__Address",
            "[class*='PropertyCardAddress']",
            "[class*='location']",
            "span[class*='area']",
        ]
        for sel in addr_selectors:
            el = card.locator(sel).first
            if await el.count() > 0:
                data["Địa chỉ"] = (await el.text_content() or "").strip()
                if data["Địa chỉ"]:
                    break
    except Exception:
        pass

    try:
        star_selectors = [
            "[data-selenium='hotel-star-rating']",
            "[class*='star-rating']",
            "[aria-label*='star']",
            "[class*='StarRating']",
            "span[class*='stars']",
        ]
        for sel in star_selectors:
            el = card.locator(sel).first
            if await el.count() > 0:
                star_text = (await el.text_content() or "").strip()
                if not star_text:
                    aria = await el.get_attribute("aria-label") or ""
                    star_text = aria
                if star_text:
                    data["Hạng sao"] = star_text
                    break

        if not data["Hạng sao"]:
            stars = await card.locator("[class*='star'][class*='filled'], .star.filled, svg[class*='star']").count()
            if stars > 0:
                data["Hạng sao"] = f"{stars} sao"
    except Exception:
        pass

    try:
        price_selectors = [
            "[data-selenium='display-price']",
            "[class*='PriceDisplay']",
            "[class*='price-display']",
            "[class*='finalPrice']",
            "[data-element-name='final-price']",
            "span[class*='Price']",
            "[class*='total-price']",
        ]
        for sel in price_selectors:
            el = card.locator(sel).first
            if await el.count() > 0:
                price_text = (await el.text_content() or "").strip()
                if price_text and any(c.isdigit() for c in price_text):
                    data["Giá thấp nhất (đã gồm thuế & phí)"] = clean_price(price_text)
                    break

        if not data["Giá thấp nhất (đã gồm thuế & phí)"]:
            price_el = card.locator("[class*='price'], [class*='Price']").first
            if await price_el.count() > 0:
                price_text = (await price_el.text_content() or "").strip()
                if price_text and any(c.isdigit() for c in price_text):
                    data["Giá thấp nhất (đã gồm thuế & phí)"] = clean_price(price_text)
    except Exception:
        pass

    try:
        meal_selectors = [
            "[data-selenium='meal-plan']",
            "[class*='MealPlan']",
            "[class*='meal-plan']",
            "[class*='breakfast']",
            "span[class*='BoardBasis']",
            "[data-element-name='board-basis']",
        ]
        for sel in meal_selectors:
            el = card.locator(sel).first
            if await el.count() > 0:
                meal_text = (await el.text_content() or "").strip()
                if meal_text:
                    data["Meal Plan"] = meal_text
                    break
    except Exception:
        pass

    try:
        cancel_selectors = [
            "[data-selenium='cancellation-policy']",
            "[class*='CancellationPolicy']",
            "[class*='cancellation']",
            "[class*='refundable']",
            "[class*='FreeCancellation']",
            "span[class*='cancel']",
        ]
        for sel in cancel_selectors:
            el = card.locator(sel).first
            if await el.count() > 0:
                cancel_text = (await el.text_content() or "").strip()
                if cancel_text:
                    data["Chính sách hoàn hủy"] = cancel_text
                    break
    except Exception:
        pass

    return data


async def scrape_agoda(
    url: str,
    destination: str,
    status_callback=None,
    max_hotels: int = 200
) -> list:
    """
    Main scraping function.
    Returns list of dicts with hotel data.
    """
    results = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--window-size=1366,768",
            ]
        )

        ua = random.choice(USER_AGENTS)
        context = await browser.new_context(
            user_agent=ua,
            viewport={"width": 1366, "height": 768},
            locale="vi-VN",
            extra_http_headers={
                "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
                "Accept": "text/html,application/xhtml+xml,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
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
                status_callback("Trang đã tải, đang scroll để load thêm khách sạn...")

            await scroll_to_load_all(page, status_callback=status_callback)

            if status_callback:
                status_callback("Đang phân tích dữ liệu khách sạn...")

            card_selectors = [
                "[data-selenium='hotel-item']",
                "[data-hotelid]",
                ".PropertyCard",
                "[class*='PropertyCard']",
                "li[class*='hotel']",
                "[data-element-name='property-card']",
            ]

            cards = None
            for sel in card_selectors:
                cards = page.locator(sel)
                count = await cards.count()
                if count > 0:
                    if status_callback:
                        status_callback(f"Tìm thấy {count} khách sạn với selector: {sel}")
                    break

            if cards is None or await cards.count() == 0:
                if status_callback:
                    status_callback("Không tìm thấy danh sách khách sạn. Thử lấy HTML thô...")
                html = await page.content()
                if status_callback:
                    status_callback(f"Độ dài HTML: {len(html)} ký tự")
                await browser.close()
                return []

            total_cards = await cards.count()
            total_cards = min(total_cards, max_hotels)

            for i in range(total_cards):
                try:
                    card = cards.nth(i)
                    hotel_data = await extract_hotel_data(card, destination)

                    if hotel_data["Tên khách sạn"]:
                        results.append(hotel_data)

                    if status_callback and (i + 1) % 5 == 0:
                        status_callback(f"Đã xử lý {i + 1}/{total_cards} khách sạn...")

                    await asyncio.sleep(random.uniform(0.05, 0.2))

                except Exception as e:
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


def run_scrape(url: str, destination: str, status_callback=None, max_hotels: int = 200) -> list:
    """Synchronous wrapper for the async scraping function."""
    return asyncio.run(scrape_agoda(url, destination, status_callback, max_hotels))
