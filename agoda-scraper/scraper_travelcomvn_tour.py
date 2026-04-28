import asyncio
import re
import sys
from datetime import datetime
from urllib.parse import parse_qs, quote, urlparse

from playwright.async_api import async_playwright

from playwright_bootstrap import ensure_playwright_chromium


def _ensure_windows_proactor_policy() -> None:
    if sys.platform.startswith("win") and hasattr(asyncio, "WindowsProactorEventLoopPolicy"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())


TOUR_SLUG_MAP = {
    "trung quốc": "trung-quoc",
    "trung quoc": "trung-quoc",
    "china": "trung-quoc",
    "hàn quốc": "han-quoc",
    "han quoc": "han-quoc",
    "korea": "han-quoc",
    "japan": "nhat-ban",
    "nhật bản": "nhat-ban",
    "nhat ban": "nhat-ban",
}


def _safe(v, default=""):
    if v is None:
        return default
    return str(v).strip()


def _slugify_text(txt: str) -> str:
    txt = _safe(txt).lower()
    if not txt:
        return ""
    for src, dst in [
        ("đ", "d"),
        ("á", "a"), ("à", "a"), ("ả", "a"), ("ã", "a"), ("ạ", "a"),
        ("ă", "a"), ("ắ", "a"), ("ằ", "a"), ("ẳ", "a"), ("ẵ", "a"), ("ặ", "a"),
        ("â", "a"), ("ấ", "a"), ("ầ", "a"), ("ẩ", "a"), ("ẫ", "a"), ("ậ", "a"),
        ("é", "e"), ("è", "e"), ("ẻ", "e"), ("ẽ", "e"), ("ẹ", "e"),
        ("ê", "e"), ("ế", "e"), ("ề", "e"), ("ể", "e"), ("ễ", "e"), ("ệ", "e"),
        ("í", "i"), ("ì", "i"), ("ỉ", "i"), ("ĩ", "i"), ("ị", "i"),
        ("ó", "o"), ("ò", "o"), ("ỏ", "o"), ("õ", "o"), ("ọ", "o"),
        ("ô", "o"), ("ố", "o"), ("ồ", "o"), ("ổ", "o"), ("ỗ", "o"), ("ộ", "o"),
        ("ơ", "o"), ("ớ", "o"), ("ờ", "o"), ("ở", "o"), ("ỡ", "o"), ("ợ", "o"),
        ("ú", "u"), ("ù", "u"), ("ủ", "u"), ("ũ", "u"), ("ụ", "u"),
        ("ư", "u"), ("ứ", "u"), ("ừ", "u"), ("ử", "u"), ("ữ", "u"), ("ự", "u"),
        ("ý", "y"), ("ỳ", "y"), ("ỷ", "y"), ("ỹ", "y"), ("ỵ", "y"),
    ]:
        txt = txt.replace(src, dst)
    txt = re.sub(r"[^a-z0-9]+", "-", txt).strip("-")
    return txt


def resolve_travel_tour_slug(destination: str) -> str:
    key = _safe(destination).lower()
    if not key:
        return ""
    if key in TOUR_SLUG_MAP:
        return TOUR_SLUG_MAP[key]
    for k, v in TOUR_SLUG_MAP.items():
        if key in k or k in key:
            return v
    return _slugify_text(key)


def build_travel_tour_url(destination_slug: str, from_date: str) -> str:
    slug = _safe(destination_slug).strip("/")
    if not slug:
        slug = "trung-quoc"
    return f"https://travel.com.vn/du-lich-{quote(slug)}.aspx?fromDate={_safe(from_date)}"


def _extract_from_year(url: str) -> int:
    parsed = urlparse(_safe(url))
    qs = parse_qs(parsed.query or "")
    from_date = (qs.get("fromDate") or [""])[0]
    try:
        dt = datetime.strptime(from_date, "%Y-%m-%d")
        return dt.year
    except Exception:
        return datetime.now().year


def _parse_departure_dates(chunk: str, base_year: int) -> str:
    pairs = re.findall(r"(\d{2})/(\d{2})", _safe(chunk))
    if not pairs:
        return ""
    out = []
    year = base_year
    prev_month = None
    for dd, mm in pairs:
        d = int(dd)
        m = int(mm)
        if prev_month is not None and m < prev_month:
            year += 1
        prev_month = m
        out.append(f"{d:02d}/{m:02d}/{year}")
    # de-dup preserve order
    seen = set()
    dedup = []
    for x in out:
        if x not in seen:
            seen.add(x)
            dedup.append(x)
    return " | ".join(dedup)


def _parse_card(card: dict, base_year: int) -> dict | None:
    href = _safe(card.get("href"))
    if not href:
        return None
    context = _safe(card.get("context"))
    if "Mã tour:" not in context:
        return None

    head = context.split("Mã tour:", 1)[0].strip()
    for prefix in ("Tiêu chuẩn", "Tiết kiệm", "Giá tốt", "Cao cấp"):
        if head.startswith(prefix):
            head = head[len(prefix):].strip()
            break

    tour_code = ""
    m = re.search(r"Mã tour:\s*([A-Za-z0-9-]+?)\s*Khởi hành:", context, flags=re.I)
    if m:
        tour_code = _safe(m.group(1)).upper()

    departure_place = ""
    m = re.search(r"Khởi hành:\s*(.*?)\s*Thời gian:", context, flags=re.I)
    if m:
        departure_place = _safe(m.group(1))

    duration = ""
    m = re.search(r"Thời gian:\s*(.*?)\s*Phương tiện:", context, flags=re.I)
    if m:
        duration = _safe(m.group(1))

    transport = ""
    m = re.search(r"Phương tiện:\s*(.*?)\s*Ngày khởi hành:", context, flags=re.I)
    if m:
        transport = _safe(m.group(1))

    departure_dates = ""
    m = re.search(r"Ngày khởi hành:\s*(.*?)\s*Giá từ:", context, flags=re.I)
    if m:
        departure_dates = _parse_departure_dates(m.group(1), base_year)

    price = ""
    m = re.search(r"Giá từ:\s*([0-9\.\,]+)", context, flags=re.I)
    if m:
        raw_p = _safe(m.group(1)).replace(" ", "")
        if re.fullmatch(r"\d{1,3}(?:\.\d{3})+", raw_p):
            price = f"{int(raw_p.replace('.', '')):,} VND"
        elif re.fullmatch(r"\d{1,3}(?:,\d{3})+", raw_p):
            price = f"{int(raw_p.replace(',', '')):,} VND"
        elif raw_p.isdigit():
            price = f"{int(raw_p):,} VND"
        else:
            price = _safe(m.group(1))

    if not head and not tour_code:
        return None

    full_link = href if href.startswith("http") else f"https://travel.com.vn{href}"
    return {
        "Phân khúc": "Tour",
        "Nguồn": "Travel.com.vn",
        "Quốc gia": "",
        "Tên tour": head,
        "Mã tour": tour_code,
        "Công ty lữ hành": "Vietravel",
        "Thời lượng (ngày)": duration,
        "Giá từ": price,
        "Tiền tệ": "VND",
        "Điểm đánh giá": "",
        "Xếp hạng nội bộ": "",
        "Ngày khởi hành": departure_dates,
        "Link tour": full_link,
        "Điểm khởi hành": departure_place,
        "Phương tiện": transport,
    }


async def _scrape_async(url: str, status_callback) -> list:
    base_year = _extract_from_year(url)
    launch_args = ["--disable-blink-features=AutomationControlled"]
    if not sys.platform.startswith("win"):
        launch_args.extend(["--no-sandbox", "--disable-setuid-sandbox"])

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=launch_args)
        page = await browser.new_page()
        status_callback("🌐 Đang mở trang tour Travel.com.vn...")
        await page.goto(url, wait_until="domcontentloaded", timeout=70000)
        await page.wait_for_timeout(10000)

        cards = await page.evaluate(
            """() => {
                const out = [];
                const anchors = Array.from(document.querySelectorAll('a'))
                    .filter(a => ((a.getAttribute('href') || '').includes('/chuong-trinh/')));
                for (const a of anchors) {
                    const href = a.getAttribute('href') || '';
                    let node = a;
                    let longest = '';
                    for (let i = 0; i < 7 && node; i++) {
                        const t = (node.textContent || '').replace(/\\s+/g, ' ').trim();
                        if (t.length > longest.length) longest = t;
                        node = node.parentElement;
                    }
                    out.push({
                        href,
                        text: (a.textContent || '').replace(/\\s+/g, ' ').trim(),
                        context: longest,
                    });
                }
                return out;
            }"""
        )
        await browser.close()

    status_callback(f"📄 Tìm thấy {len(cards)} card tour thô.")
    seen = set()
    rows = []
    for c in cards:
        row = _parse_card(c, base_year=base_year)
        if not row:
            continue
        key = row.get("Mã tour") or row.get("Link tour")
        if key and key not in seen:
            seen.add(key)
            rows.append(row)
    status_callback(f"✅ Travel Tour: {len(rows)} tour unique.")
    return rows


def run_scrape_travel_tour(url: str, status_callback=None) -> list:
    if status_callback is None:
        status_callback = lambda _msg: None
    _ensure_windows_proactor_policy()
    ensure_playwright_chromium(status_callback)
    return asyncio.run(_scrape_async(url, status_callback))

