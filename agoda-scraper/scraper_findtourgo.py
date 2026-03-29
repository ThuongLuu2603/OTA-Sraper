import math
import re
from datetime import datetime
from urllib.parse import parse_qs, urlencode, urlparse

import requests


BASE_SITE = "https://findtourgo.com"
SEARCH_API = "https://api-v2.findtourgo.com/v1/search/tours"
DETAIL_API = "https://api-v2.findtourgo.com/v1/public/tours/{tour_code}"


def _safe_text(v, default=""):
    if v is None:
        return default
    return str(v).strip()


def _to_int(v, default=0):
    try:
        return int(v)
    except Exception:
        return default


def _fmt_money(v):
    try:
        n = float(v or 0)
        return f"{n:,.0f}"
    except Exception:
        return ""


def _build_public_tour_url(locale: str, tour_code: str, slug: str) -> str:
    locale = _safe_text(locale, "vi") or "vi"
    tour_code = _safe_text(tour_code)
    slug = _safe_text(slug)
    if not tour_code:
        return ""
    if slug:
        return f"{BASE_SITE}/{locale}/tours/{tour_code}/{slug}"
    return f"{BASE_SITE}/{locale}/tours/{tour_code}"


def _extract_departure_dates(detail_json: dict) -> str:
    schedules = detail_json.get("tourSchedules") or []
    dates = []
    for sch in schedules:
        if not isinstance(sch, dict):
            continue
        for d in sch.get("departureSpecifiedDates") or []:
            txt = _safe_text(d)
            if txt:
                dates.append(txt)
        # Keep extra schedule hints if explicit dates are empty.
        if not dates:
            start_date = _safe_text(sch.get("startDate"))
            end_date = _safe_text(sch.get("endDate"))
            if start_date or end_date:
                dates.append(" - ".join([x for x in [start_date, end_date] if x]))
            weekdays = sch.get("departureWeekdays") or []
            if weekdays:
                wd_text = ", ".join([_safe_text(x) for x in weekdays if _safe_text(x)])
                if wd_text:
                    dates.append(f"Theo thứ: {wd_text}")
    # Deduplicate while preserving order.
    seen = set()
    ordered = []
    for d in dates:
        if d not in seen:
            seen.add(d)
            ordered.append(d)
    return " | ".join(ordered)


def _parse_country_and_dates(url: str):
    parsed = urlparse(_safe_text(url))
    q = parse_qs(parsed.query)
    country_code = (q.get("where") or q.get("countryCode") or [""])[0].upper()
    period_start = (q.get("tourPeriodStart") or [""])[0]
    period_end = (q.get("tourPeriodEnd") or [""])[0]
    currency = (q.get("currency") or ["USD"])[0].upper()
    locale = "vi"
    parts = [p for p in parsed.path.split("/") if p]
    if parts and len(parts[0]) == 2:
        locale = parts[0].lower()
    return country_code, period_start, period_end, currency, locale


def _normalize_country_code(raw: str) -> str:
    txt = _safe_text(raw).upper()
    if not txt:
        return ""
    # Keep only letters for robust matching.
    letters = re.sub(r"[^A-Z]", "", txt)
    if len(letters) == 2:
        return letters
    aliases = {
        "TRUNGQUOC": "CN",
        "TRUUNGQUOC": "CN",
        "CHINA": "CN",
        "VIETNAM": "VN",
        "VIETNAMM": "VN",
        "NHATBAN": "JP",
        "JAPAN": "JP",
        "THAILAN": "TH",
        "THAILAND": "TH",
        "HANQUOC": "KR",
        "KOREA": "KR",
        "SINGAPORE": "SG",
        "MALAYSIA": "MY",
        "INDONESIA": "ID",
    }
    return aliases.get(letters, "")


def build_findtourgo_url(
    country_code: str,
    tour_period_start: str,
    tour_period_end: str,
    currency: str = "USD",
    locale: str = "vi",
) -> str:
    query = urlencode(
        {
            "tourPeriodEnd": _safe_text(tour_period_end),
            "tourPeriodStart": _safe_text(tour_period_start),
            "where": _safe_text(country_code).upper(),
            "currency": _safe_text(currency).upper() or "USD",
        }
    )
    locale = _safe_text(locale, "vi") or "vi"
    country_slug = _safe_text(country_code).lower() or "country"
    return f"{BASE_SITE}/{locale}/country/{country_slug}?{query}"


def run_scrape_findtourgo_tours(
    url: str,
    country_code: str = "",
    period_start: str = "",
    period_end: str = "",
    currency: str = "USD",
    locale: str = "vi",
    page_size: int = 50,
    max_pages: int = 20,
    status_callback=None,
) -> list:
    if status_callback is None:
        status_callback = lambda _msg: None

    url = _safe_text(url)
    country_code = _normalize_country_code(country_code)
    period_start = _safe_text(period_start)
    period_end = _safe_text(period_end)
    currency = _safe_text(currency).upper()
    locale = _safe_text(locale).lower()

    if url:
        c2, s2, e2, cur2, loc2 = _parse_country_and_dates(url)
        country_code = country_code or _normalize_country_code(c2)
        period_start = period_start or s2
        period_end = period_end or e2
        if not currency and cur2:
            currency = cur2
        if not locale and loc2:
            locale = loc2

    currency = currency or "USD"
    locale = locale or "vi"

    if not country_code:
        raise ValueError("Thiếu hoặc sai country code (where). Dùng mã 2 ký tự như CN, VN, JP...")
    if not period_start or not period_end:
        raise ValueError("Thiếu tourPeriodStart hoặc tourPeriodEnd.")

    status_callback(f"🌐 FindTourGo: country={country_code} | {period_start} -> {period_end}")

    sess = requests.Session()
    sess.headers.update(
        {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        }
    )

    rows = []
    seen_codes = set()
    page = 0
    total_pages = None
    page_size = max(10, min(_to_int(page_size, 50), 100))
    max_pages = max(1, _to_int(max_pages, 20))

    while page < max_pages:
        params = {
            "countryCode": country_code,
            "page": page,
            "pageSize": page_size,
            "tourPeriodStart": period_start,
            "tourPeriodEnd": period_end,
            "locale": locale,
        }
        r = sess.get(SEARCH_API, params=params, timeout=30)
        r.raise_for_status()
        payload = r.json() if r.text else {}

        items = payload.get("items") or []
        if total_pages is None:
            total_items = _to_int(payload.get("totalItems"), 0)
            total_pages = _to_int(payload.get("totalPage"), 0)
            if total_pages <= 0 and total_items > 0:
                total_pages = int(math.ceil(total_items / float(page_size)))
            status_callback(f"📊 FindTourGo: tổng ~{total_items} tour, {max(total_pages, 1)} trang")

        if not items:
            break

        for item in items:
            if not isinstance(item, dict):
                continue
            tour_code = _safe_text(item.get("tourCode"))
            if not tour_code or tour_code in seen_codes:
                continue
            seen_codes.add(tour_code)

            detail_url = DETAIL_API.format(tour_code=tour_code)
            departure_text = ""
            try:
                dr = sess.get(detail_url, timeout=30)
                if dr.status_code == 200 and dr.text:
                    departure_text = _extract_departure_dates(dr.json())
            except Exception:
                departure_text = ""

            agency = item.get("travelAgency") or {}
            sale_price = item.get("salePrice")
            regular_price = item.get("regularPrice")
            price_val = sale_price if sale_price and float(sale_price) > 0 else regular_price
            stars = item.get("score")
            score = item.get("ratingScore")

            rows.append(
                {
                    "Phân khúc": "Tour",
                    "Nguồn": "FindTourGo",
                    "Quốc gia": country_code,
                    "Tên tour": _safe_text(item.get("name")),
                    "Mã tour": tour_code,
                    "Công ty lữ hành": _safe_text(agency.get("name")),
                    "Thời lượng (ngày)": _to_int(item.get("duration"), 0),
                    "Giá từ": _fmt_money(price_val),
                    "Tiền tệ": _safe_text(currency, "USD"),
                    "Điểm đánh giá": _safe_text(score),
                    "Xếp hạng nội bộ": _safe_text(stars),
                    "Ngày khởi hành": departure_text,
                    "Link tour": _build_public_tour_url(locale=locale, tour_code=tour_code, slug=_safe_text(item.get("slug"))),
                }
            )

        status_callback(f"📄 Trang {page + 1}: +{len(items)} item thô, tổng unique={len(rows)}")
        page += 1
        if total_pages and page >= total_pages:
            break

    return rows

