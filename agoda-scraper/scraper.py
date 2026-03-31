import asyncio
import os
import random
import re
import time
import shutil
import urllib.request
import json
import sys
import unicodedata
from collections import Counter
from datetime import datetime
from urllib.parse import parse_qs, quote, unquote_plus, urlparse
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from playwright_bootstrap import ensure_playwright_chromium
from geo_extract import scan_json_for_latlng


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


def _price_display_to_float(v) -> float | None:
    if v is None:
        return None
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v)
    s = str(v).strip().replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


def _per_night_side_amount(pn: dict, side: str) -> float | None:
    """Lấy số từ perNight.exclusive|inclusive (display, amount, hoặc số thẳng)."""
    if not isinstance(pn, dict):
        return None
    branch = pn.get(side)
    if isinstance(branch, dict):
        for k in ("display", "amount", "value", "raw"):
            v = _price_display_to_float(branch.get(k))
            if v is not None:
                return v
        return None
    return _price_display_to_float(branch)


def _append_per_night_entry(
    entries: list[dict],
    seen: set[tuple],
    currency: str,
    excl: float | None,
    incl: float | None,
) -> None:
    if excl is None and incl is None:
        return
    if excl is None:
        excl = incl
    if incl is None:
        incl = excl
    cur = str(currency or "USD").strip().upper() or "USD"
    key = (cur, round(float(incl), 2), round(float(excl), 2))
    if key in seen:
        return
    seen.add(key)
    entries.append({"currency": cur, "excl": float(excl), "incl": float(incl)})


_SKIP_PER_NIGHT_WALK_KEYS = frozenset(
    {
        "reviews",
        "review",
        "guestReviews",
        "reviewComments",
        "qna",
        "questionsAndAnswers",
        "similarProperties",
        "featuredComments",
    }
)


def _looks_like_hotel_nightly_money(amount: float | None) -> bool:
    if amount is None or amount != amount:  # NaN
        return False
    return 25_000 <= float(amount) <= 999_999_999


def _walk_inclusive_exclusive_price_blocks(
    node, entries: list[dict], seen: set[tuple], depth: int = 0
) -> None:
    """
    Một số payload Agoda đặt giá list-card trong object có sẵn inclusive/exclusive (không bọc price.perNight).
    """
    if depth > 18:
        return
    if isinstance(node, dict):
        inc_o = node.get("inclusive")
        exc_o = node.get("exclusive")
        if isinstance(inc_o, dict) and isinstance(exc_o, dict):
            faux = {"inclusive": inc_o, "exclusive": exc_o}
            incl = _per_night_side_amount(faux, "inclusive")
            excl = _per_night_side_amount(faux, "exclusive")
            if _looks_like_hotel_nightly_money(incl) or _looks_like_hotel_nightly_money(excl):
                cur = node.get("currency") or node.get("currencyCode")
                cur_s = str(cur).strip().upper() if cur else ""
                if not cur_s or cur_s.isdigit():
                    cur_s = "VND"
                _append_per_night_entry(entries, seen, cur_s, excl, incl)
        for k, v in node.items():
            if isinstance(k, str) and k in _SKIP_PER_NIGHT_WALK_KEYS:
                continue
            _walk_inclusive_exclusive_price_blocks(v, entries, seen, depth + 1)
    elif isinstance(node, list):
        for it in node[:150]:
            _walk_inclusive_exclusive_price_blocks(it, entries, seen, depth + 1)


def _walk_collect_per_night_prices(node, entries: list[dict], seen: set[tuple], depth: int = 0) -> None:
    """
    Quét sâu toàn property: mọi dict có price.perNight (Agoda đặt giá list-card / room ở nhiều nhánh).
    Không đọc price.total để tránh nhầm tổng kỳ với giá/đêm.
    """
    if depth > 18:
        return
    if isinstance(node, dict):
        price = node.get("price")
        if isinstance(price, dict) and isinstance(price.get("perNight"), dict):
            pn = price["perNight"]
            cur = node.get("currency") or price.get("currency") or "USD"
            excl = _per_night_side_amount(pn, "exclusive")
            incl = _per_night_side_amount(pn, "inclusive")
            _append_per_night_entry(entries, seen, cur, excl, incl)
        for k, v in node.items():
            if isinstance(k, str) and k in _SKIP_PER_NIGHT_WALK_KEYS:
                continue
            _walk_collect_per_night_prices(v, entries, seen, depth + 1)
    elif isinstance(node, list):
        for it in node[:150]:
            _walk_collect_per_night_prices(it, entries, seen, depth + 1)


def _parse_json_display_money_vnd(s: str) -> float | None:
    """Chuỗi kiểu '364,609 VND' / '1.458.437' trong JSON display."""
    if not s:
        return None
    t = s.replace("\xa0", " ").strip()
    t = re.sub(r"\s+VND\s*$", "", t, flags=re.I).strip()
    if re.fullmatch(r"\d{1,3}(?:,\d{3})+", t):
        v = float(t.replace(",", ""))
    elif re.fullmatch(r"\d{1,3}(?:\.\d{3})+", t):
        v = float(t.replace(".", ""))
    else:
        m = re.match(r"([\d]{1,3}(?:[.,]\d{3})+)", t)
        if not m:
            return None
        num = m.group(1)
        if "." in num and "," not in num:
            v = float(num.replace(".", ""))
        elif "," in num:
            v = float(num.replace(",", ""))
        else:
            return None
    return v if 80_000 <= v <= 99_000_000 else None


def _mine_per_night_display_pairs_vnd(hotel: dict) -> list[tuple[float, float]]:
    """
    Gom mọi cặp (exclusive, inclusive) từ chuỗi display trong từng khối "perNight"
    trên **toàn bộ** JSON property (promo thường nằm nhánh không được walker dict tới).
    """
    if not isinstance(hotel, dict):
        return []
    try:
        s = json.dumps(hotel, ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError):
        return []
    max_l = 2_600_000
    if len(s) > max_l:
        s = s[:max_l]
    pairs: list[tuple[float, float]] = []
    start = 0
    while True:
        i = s.find('"perNight"', start)
        if i < 0:
            break
        chunk = s[i : i + 8000]
        im = re.search(
            r'"inclusive"\s*:\s*\{.*?"display"\s*:\s*"([^"]+)"',
            chunk,
            re.I | re.DOTALL,
        )
        em = re.search(
            r'"exclusive"\s*:\s*\{.*?"display"\s*:\s*"([^"]+)"',
            chunk,
            re.I | re.DOTALL,
        )
        inc = _parse_json_display_money_vnd(im.group(1)) if im else None
        exc = _parse_json_display_money_vnd(em.group(1)) if em else None
        if inc is None:
            am_i = re.search(
                r'"inclusive"\s*:\s*\{.*?"amount"\s*:\s*([0-9]+(?:\.[0-9]+)?)',
                chunk,
                re.I | re.DOTALL,
            )
            am_e = re.search(
                r'"exclusive"\s*:\s*\{.*?"amount"\s*:\s*([0-9]+(?:\.[0-9]+)?)',
                chunk,
                re.I | re.DOTALL,
            )
            if not am_i:
                am_i = re.search(
                    r'"inclusive"\s*:\s*\{.*?"value"\s*:\s*([0-9]+(?:\.[0-9]+)?)',
                    chunk,
                    re.I | re.DOTALL,
                )
            if not am_e:
                am_e = re.search(
                    r'"exclusive"\s*:\s*\{.*?"value"\s*:\s*([0-9]+(?:\.[0-9]+)?)',
                    chunk,
                    re.I | re.DOTALL,
                )
            if am_i:
                try:
                    inc = float(am_i.group(1))
                except ValueError:
                    inc = None
            if am_e:
                try:
                    exc = float(am_e.group(1))
                except ValueError:
                    pass
        if inc is not None and 80_000 <= float(inc) <= 99_000_000:
            exf = float(exc) if exc is not None else float(inc)
            pairs.append((exf, float(inc)))
        start = i + 12
    return pairs


def _filter_per_night_outliers(entries: list[dict]) -> list[dict]:
    """
    Bỏ các mức inclusive rõ ràng là rác parse (vd. ~104k khi cùng property có ~647k).
    Không dùng ngưỡng tỷ lệ trên max (mx*0.18): sẽ xoá nhầm giá khuyến mãi thật (364k vs rack 2,5M).
    """
    if len(entries) < 2:
        return entries
    mx = max(e["incl"] for e in entries)
    mn = min(e["incl"] for e in entries)
    if mx <= 0 or mn <= 0:
        return entries
    if mx / mn < 4.5:
        return entries
    # Chỉ loại mức "vi mô" so với max; giá promo hợp lệ (thường > ~200k) không bị đụng.
    kept = [
        e
        for e in entries
        if not (
            e["incl"] > 0
            and e["incl"] < 220_000
            and mx / e["incl"] > 5.0
        )
    ]
    return kept if kept else entries


def _collect_prices(hotel: dict, prefer_currency: str = "VND") -> list[dict]:
    """
    Quét toàn bộ node property (bỏ nhánh review) để gom mọi `price.perNight` trong GraphQL.
    Một số payload chỉ đặt giá list-card / promo ở nhánh sâu, không chỉ `pricing`/`content`.
    """
    entries: list[dict] = []
    seen: set[tuple] = set()
    try:
        if isinstance(hotel, dict):
            _walk_collect_per_night_prices(hotel, entries, seen, 0)
            _walk_inclusive_exclusive_price_blocks(hotel, entries, seen, 0)
            for exc_m, inc_m in _mine_per_night_display_pairs_vnd(hotel):
                _append_per_night_entry(entries, seen, "VND", exc_m, inc_m)
    except Exception:
        pass

    if not entries:
        return []

    pref = (prefer_currency or "VND").strip().upper()
    vnd_rows = [e for e in entries if e["currency"] == pref]
    if vnd_rows:
        entries = vnd_rows
    else:
        dom = Counter(e["currency"] for e in entries).most_common(1)
        if dom:
            c0 = dom[0][0]
            entries = [e for e in entries if e["currency"] == c0]

    entries = _filter_per_night_outliers(entries)
    entries.sort(key=lambda x: (x["incl"], x["excl"]))
    return entries


def _format_price(amount: float, currency: str) -> str:
    """Format a price amount with currency."""
    if currency in ("VND", "JPY", "KRW", "IDR"):
        return f"{amount:,.0f} {currency}"
    return f"{amount:,.2f} {currency}"


def _parse_agoda_price_amount_vnd(text: str) -> float | None:
    """Parse số từ chuỗi kiểu '680,867 VND' hoặc '364.609 đ' (VN dùng . phân cách nghìn)."""
    if not text:
        return None
    t = text.replace("\xa0", " ").strip()
    m = re.search(r"([\d][\d.,\s]*)\s*(?:VND|đ|₫)", t, re.I)
    if not m:
        return None
    num = m.group(1).replace(" ", "")
    if re.fullmatch(r"\d{1,3}(?:\.\d{3})+", num):
        num = num.replace(".", "")
    elif re.fullmatch(r"\d{1,3}(?:,\d{3})+", num):
        num = num.replace(",", "")
    else:
        num = num.replace(",", "").replace(".", "")
    try:
        v = float(num)
        return v if v >= 10_000 else None
    except ValueError:
        return None


def _agoda_search_hotel_name_filter(url: str) -> str:
    """Giá trị query hotelName= khi user lọc tìm 1 khách sạn (URL của bạn có tham số này)."""
    try:
        q = parse_qs(urlparse(url).query)
        for key in ("hotelName", "hotelname"):
            vals = q.get(key)
            if vals and vals[0]:
                return unquote_plus(vals[0]).strip()
    except Exception:
        pass
    return ""


def _norm_hotel_filter_text(s: str) -> str:
    if not s:
        return ""
    t = unicodedata.normalize("NFKD", s)
    t = "".join(c for c in t if not unicodedata.combining(c))
    t = t.lower()
    for ch in ".,'()/-":
        t = t.replace(ch, " ")
    return " ".join(t.split())


def _card_text_matches_hotel_filter(card_norm: str, filt_norm: str) -> bool:
    if not filt_norm or not card_norm:
        return False
    if filt_norm in card_norm:
        return True
    parts = [p for p in filt_norm.split() if len(p) > 2]
    if not parts:
        return filt_norm in card_norm
    return all(p in card_norm for p in parts)


def _hotel_name_matches_search_filter(display_name: str, filter_raw: str) -> bool:
    """Khớp tên dòng GraphQL với hotelName= trên URL (hai chiều, có chuẩn hoá dấu)."""
    a = _norm_hotel_filter_text(display_name)
    b = _norm_hotel_filter_text(filter_raw)
    if not a or not b:
        return False
    return _card_text_matches_hotel_filter(a, b) or _card_text_matches_hotel_filter(b, a)


async def _agoda_ui_price_for_name_filter(page, filter_raw: str) -> float | None:
    """
    Khi URL có hotelName=, lấy giá trên thẻ có nội dung khớp tên (không phụ thuộc map property id).
    Khớp tên trên **toàn bộ** innerText thẻ (1200 ký tự đầu thường không chứa tên KS).
    Ưu tiên thẻ nằm trong search-web-accommodation-card, theo thứ tự DOM.
    """
    filt = _norm_hotel_filter_text(filter_raw)
    if not filt:
        return None

    async def _scan_cards_v2() -> float | None:
        """(thứ tự DOM, giá, is_accom) — lấy thẻ khớp đầu tiên trong nhóm ưu tiên."""
        hits: list[tuple[int, float, bool]] = []
        dom_i = 0
        for frame in page.frames:
            try:
                loc = frame.locator("[data-property-id]")
                n = await loc.count()
                for i in range(n):
                    el = loc.nth(i)
                    try:
                        has_accom = await el.evaluate(
                            """(el) => !!(el.closest(
                                '[data-selenium="search-web-accommodation-card"]'
                            ))"""
                        )
                        blob = await el.evaluate(
                            """(el) => {
                            const r =
                                el.closest('[data-selenium="search-web-accommodation-card"]') ||
                                el.closest('[data-selenium="hotel-item"]') ||
                                el.closest('li') ||
                                el;
                            return (r && r.innerText) ? r.innerText : (el.innerText || '');
                        }"""
                        )
                    except Exception:
                        dom_i += 1
                        continue
                    cn = _norm_hotel_filter_text(blob or "")
                    if not _card_text_matches_hotel_filter(cn, filt):
                        dom_i += 1
                        continue
                    v = _parse_agoda_card_ui_final_vnd(blob or "")
                    if v is None:
                        dom_i += 1
                        continue
                    hits.append((dom_i, float(v), bool(has_accom)))
                    dom_i += 1
            except Exception:
                continue
        if not hits:
            return None
        ac = [h for h in hits if h[2]]
        pool = ac if ac else hits
        pool.sort(key=lambda t: t[0])
        return pool[0][1]

    v = await _scan_cards_v2()
    if v is not None:
        return v
    await asyncio.sleep(3.5)
    for _ in range(5):
        try:
            await page.evaluate("() => window.scrollBy(0, 500)")
        except Exception:
            break
        await asyncio.sleep(0.45)
    return await _scan_cards_v2()


async def _agoda_ui_price_from_main_inner_text(page, filter_raw: str) -> float | None:
    """
    Fallback khi không có / không đọc được [data-property-id]: cắt đoạn quanh tên KS trong main/body.
    """
    filt = _norm_hotel_filter_text(filter_raw)
    if not filt:
        return None
    tokens = [t for t in filt.split() if len(t) > 2]
    for frame in page.frames:
        try:
            blob = await frame.evaluate(
                """() => {
                const m = document.querySelector('main');
                return (m && m.innerText) ? m.innerText : (document.body.innerText || '');
            }"""
            )
        except Exception:
            continue
        if not blob or len(blob) < 80:
            continue
        low = blob.lower()
        pos = -1
        for t in [filt] + tokens:
            j = low.find(t.lower())
            if j >= 0:
                pos = j
                break
        if pos < 0:
            head_n = _norm_hotel_filter_text(blob[:50000])
            if not _card_text_matches_hotel_filter(head_n, filt):
                continue
            pos = 0
        chunk = blob[pos : pos + 4500]
        v = _parse_agoda_card_ui_final_vnd(chunk)
        if v is not None:
            return v
    return None


async def _agoda_ui_price_for_hotel_name_query(page, filter_raw: str) -> float | None:
    """Thẻ property → scroll retry → main/body text."""
    v = await _agoda_ui_price_for_name_filter(page, filter_raw)
    if v is not None:
        return v
    return await _agoda_ui_price_from_main_inner_text(page, filter_raw)


def _agoda_merge_cheaper_inclusive_row(old: dict, new: dict) -> dict:
    """Giữ bản ghi có Giá/đêm (đã gồm thuế) thấp hơn (cùng propertyId)."""
    ao = _parse_agoda_price_amount_vnd(old.get("Giá/đêm (đã gồm thuế)", "") or "")
    an = _parse_agoda_price_amount_vnd(new.get("Giá/đêm (đã gồm thuế)", "") or "")
    if an is not None and (ao is None or an < ao):
        return new
    return old


def _agoda_ui_lookup_keys_for_hotel(
    pid_str: str,
    property_page: str,
    extra_link_strings: tuple[str, ...] = (),
) -> list[str]:
    """Khớp map DOM: GraphQL có thể dùng propertyId khác hid trên link thẻ."""
    keys: list[str] = []
    for s in (pid_str or "",):
        s = str(s).strip()
        if s:
            keys.append(s)
    url_parts = [property_page or ""] + list(extra_link_strings or ())
    q_re = re.compile(
        r"[?&](?:hid|hotel|hotelId|hotel_id|propertyId|property_id)=(\d{4,14})\b",
        re.I,
    )
    for pp in url_parts:
        pp = (pp or "").strip()
        if not pp:
            continue
        for m in q_re.finditer(pp):
            keys.append(m.group(1))
        for rx in _AGODA_ID_URL_RES:
            m = rx.search(pp)
            if m:
                keys.append(m.group(1))
    seen: set[str] = set()
    out: list[str] = []
    for k in keys:
        if k and k not in seen:
            seen.add(k)
            out.append(k)
    return out


def _parse_agoda_card_ui_final_vnd(card_text: str) -> float | None:
    """
    Giá đỏ ngay trên dòng 'Mỗi đêm…' / 'Per night…'.
    Không dùng min() trong cửa sổ rộng — sẽ chọn nhầm mức trung gian (vd. ~401k thay vì ~364k sau coupon).
    Lấy số tiền **sát anchor nhất** (rightmost trong ~300 ký tự phía trước anchor **đầu tiên** trong phần đầu thẻ).
    """
    if not card_text:
        return None
    text = card_text.replace("\xa0", " ")
    head = text[:6500]
    primary_anchor = re.compile(
        r"Mỗi đêm|mỗi đêm|Per night|per night",
        re.I,
    )
    fallback_anchor = re.compile(
        r"đã gồm thuế|taxes and fees|Thuế và phí",
        re.I,
    )
    # Không dùng \s sau số: sẽ nuốt \n và làm mất dòng giá kế tiếp (promo dưới rack).
    money_pat = re.compile(
        r"(?:^|[^\d])((?:\d{1,3}(?:\.\d{3})+|\d{1,3}(?:,\d{3})+))[ \t\u00a0]*(?:đ|₫|VND)?",
        re.I,
    )

    def _tok_to_float(tok: str) -> float | None:
        t = tok.strip()
        if re.fullmatch(r"\d{1,3}(?:\.\d{3})+", t):
            return float(t.replace(".", ""))
        if re.fullmatch(r"\d{1,3}(?:,\d{3})+", t):
            return float(t.replace(",", ""))
        return None

    def _rightmost_money_before(idx: int, win_w: int = 320) -> float | None:
        start = max(0, idx - win_w)
        win = head[start:idx]
        last_v: float | None = None
        last_end = -1
        for mm in money_pat.finditer(win):
            v = _tok_to_float(mm.group(1))
            if v is None or not (120_000 <= v <= 99_000_000):
                continue
            if mm.end() > last_end:
                last_end = mm.end()
                last_v = v
        return last_v

    m = primary_anchor.search(head)
    if not m:
        m = fallback_anchor.search(head)
    if m:
        v = _rightmost_money_before(m.start())
        if v is not None:
            return v

    vals: list[float] = []
    for mm in money_pat.finditer(head):
        v = _tok_to_float(mm.group(1))
        if v is not None and 280_000 <= v <= 80_000_000:
            vals.append(v)
    return min(vals) if vals else None


async def _agoda_ui_map_via_playwright_locators(page) -> dict[str, float]:
    """Dự phòng khi page.evaluate không thấy thẻ (iframe / cây DOM khác)."""
    out: dict[str, float] = {}
    for frame in page.frames:
        try:
            loc = frame.locator("[data-property-id]")
            n = await loc.count()
            for i in range(n):
                el = loc.nth(i)
                try:
                    pid = await el.get_attribute("data-property-id")
                except Exception:
                    continue
                if not pid or not str(pid).strip().isdigit():
                    continue
                pid = str(pid).strip()
                try:
                    card_text = await el.evaluate(
                        """(el) => {
                        const r =
                            el.closest('[data-selenium="search-web-accommodation-card"]') ||
                            el.closest('[data-selenium="hotel-item"]') ||
                            el.closest('article') ||
                            el.closest('li') ||
                            el;
                        return (r && r.innerText) ? r.innerText : (el.innerText || '');
                    }"""
                    )
                except Exception:
                    continue
                v = _parse_agoda_card_ui_final_vnd(card_text or "")
                if v is None:
                    continue
                if pid not in out or v < out[pid]:
                    out[pid] = v
        except Exception:
            continue
    return out


async def _agoda_ui_shown_price_vnd_by_property_id(page) -> dict[str, float]:
    """
    Giá đỏ trên thẻ sau GIẢM % + coupon (vd. 364.609 đ / 647.800 ₫ kèm dòng Mỗi đêm…).
    Gắn giá với mọi id tìm được trên thẻ (data-property-id, hid=, hotel=…) vì API/DOM lệch id.
    Chạy trên mọi frame + locator Playwright (kết quả search đôi nằm trong iframe).
    """
    merged: dict[str, float] = {}
    _card_price_js = """() => {
                const out = {};
                function allDeepQuerySelectorAll(root, selector, acc) {
                    try {
                        root.querySelectorAll(selector).forEach((el) => acc.push(el));
                    } catch (e) {}
                    root.querySelectorAll('*').forEach((el) => {
                        if (el.shadowRoot) allDeepQuerySelectorAll(el.shadowRoot, selector, acc);
                    });
                }
                function parseMoneyToken(tok) {
                    const s = String(tok || '').trim();
                    if (/^\\d{1,3}(\\.\\d{3})+$/.test(s)) {
                        return parseInt(s.replace(/\\./g, ''), 10);
                    }
                    if (/^\\d{1,3}(,\\d{3})+$/.test(s)) {
                        return parseInt(s.replace(/,/g, ''), 10);
                    }
                    return NaN;
                }
                function parseFromCardText(t) {
                    const text = (t || '').replace(/\\u00a0/g, ' ');
                    const head = text.slice(0, 6500);
                    const primary = /Mỗi đêm|mỗi đêm|Per night|per night/i;
                    const fallback = /đã gồm thuế|taxes and fees|Thuế và phí/i;
                    const moneyRe =
                        /([\\d]{1,3}(?:\\.\\d{3})+|[\\d]{1,3}(?:,\\d{3})+)[ \\t\\u00a0]*(?:đ|₫|\\bVND\\b)?/gi;
                    let nightIdx = head.search(primary);
                    if (nightIdx < 0) {
                        nightIdx = head.search(fallback);
                    }
                    if (nightIdx >= 0) {
                        const win = head.slice(Math.max(0, nightIdx - 320), nightIdx);
                        let bestV = null;
                        let bestEnd = -1;
                        let m;
                        while ((m = moneyRe.exec(win)) !== null) {
                            const v = parseMoneyToken(m[1]);
                            if (v >= 120000 && v <= 99000000 && m.index + m[0].length > bestEnd) {
                                bestEnd = m.index + m[0].length;
                                bestV = v;
                            }
                        }
                        if (bestV != null) return bestV;
                    }
                    const vals = [];
                    let m2;
                    moneyRe.lastIndex = 0;
                    while ((m2 = moneyRe.exec(head)) !== null) {
                        const v = parseMoneyToken(m2[1]);
                        if (v >= 280000 && v <= 80000000) vals.push(v);
                    }
                    if (!vals.length) return null;
                    return Math.min.apply(null, vals);
                }
                function collectIdsForCard(root) {
                    const ids = new Set();
                    const add = (v) => {
                        if (v && /^[0-9]{4,14}$/.test(String(v).trim())) {
                            ids.add(String(v).trim());
                        }
                    };
                    try {
                        add(root.getAttribute('data-property-id'));
                        root.querySelectorAll('[data-property-id]').forEach((n) => {
                            add(n.getAttribute('data-property-id'));
                        });
                        root.querySelectorAll('[data-hotelid],[data-hotel-id]').forEach((n) => {
                            add(n.getAttribute('data-hotelid') || n.getAttribute('data-hotel-id'));
                        });
                    } catch (e) {}
                    root.querySelectorAll('a[href]').forEach((a) => {
                        let h = a.getAttribute('href') || '';
                        if (!h) return;
                        const tryH = (s) => {
                            let m = s.match(/[?&]hid=(\\d{4,14})/i);
                            if (m) add(m[1]);
                            m = s.match(/[?&]hotel[_]?id=(\\d{4,14})/i);
                            if (m) add(m[1]);
                            m = s.match(/[?&]hotel=(\\d{4,14})\\b/i);
                            if (m) add(m[1]);
                            m = s.match(/[?&]propertyId=(\\d{4,14})\\b/i);
                            if (m) add(m[1]);
                            m = s.match(/\\/(\\d{4,14})\\.html/i);
                            if (m) add(m[1]);
                        };
                        tryH(h);
                        if (!h.startsWith('http')) {
                            tryH('https://www.agoda.com' + (h.startsWith('/') ? h : '/' + h));
                        }
                    });
                    return [...ids];
                }
                function assignPriceToIds(ids, card) {
                    if (!card || !ids.length) return;
                    const v = parseFromCardText(card.innerText || '');
                    if (v == null) return;
                    ids.forEach((pid) => {
                        if (!out[pid] || v < out[pid]) out[pid] = v;
                    });
                }
                const roots = [];
                const seenRoot = new Set();
                function addRoot(el) {
                    if (el && !seenRoot.has(el)) {
                        seenRoot.add(el);
                        roots.push(el);
                    }
                }
                let acc = [];
                allDeepQuerySelectorAll(
                    document,
                    '[data-selenium="search-web-accommodation-card"]',
                    acc
                );
                acc.forEach(addRoot);
                if (!roots.length) {
                    acc = [];
                    allDeepQuerySelectorAll(document, '[data-selenium="hotel-item"]', acc);
                    acc.forEach(addRoot);
                }
                if (!roots.length) {
                    acc = [];
                    allDeepQuerySelectorAll(
                        document,
                        '[data-element-name="property-card-container"]',
                        acc
                    );
                    acc.forEach(addRoot);
                }
                if (!roots.length) {
                    document.querySelectorAll('[data-property-id]').forEach((el) => {
                        let r = el.closest('[data-selenium="search-web-accommodation-card"]');
                        if (!r) r = el.closest('[data-selenium="hotel-item"]');
                        if (!r) r = el.closest('li');
                        if (!r) r = el;
                        addRoot(r);
                    });
                }
                roots.forEach((root) => {
                    const ids = collectIdsForCard(root);
                    if (ids.length) assignPriceToIds(ids, root);
                });
                return out;
            }"""
    try:
        for frame in page.frames:
            try:
                raw = await frame.evaluate(_card_price_js)
                if isinstance(raw, dict):
                    for k, v in raw.items():
                        ks = str(k).strip()
                        try:
                            vf = float(v)
                        except (TypeError, ValueError):
                            continue
                        if ks and (ks not in merged or vf < merged[ks]):
                            merged[ks] = vf
            except Exception:
                continue
        loc_map = await _agoda_ui_map_via_playwright_locators(page)
        for k, v in loc_map.items():
            if k not in merged or v < merged[k]:
                merged[k] = v
    except Exception:
        pass
    return merged


def _extract_price(hotel: dict) -> str:
    """Exclusive / đêm — cùng một offer với giá inclusive đã chọn (_collect_prices)."""
    entries = _collect_prices(hotel)
    if entries:
        e = entries[0]
        return _format_price(e["excl"], e["currency"])
    return ""


def _extract_price_inclusive(hotel: dict) -> str:
    """Inclusive / đêm — offer có giá đã gồm thuế thấp nhất (ưu tiên VND)."""
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

    # Payload citySearch không thống nhất: một số property chỉ có lat/lng dưới tên khóa lạ
    # (geo_extract quét chuẩn hóa key + bỏ 0,0) — bổ sung sau khi các bước trên không ra.
    if (not lat_s or not lng_s) and isinstance(hotel, dict):
        la, lo = scan_json_for_latlng(hotel, max_visits=650)
        if la and lo:
            lat_s, lng_s = la, lo

    return pid_str, lat_s, lng_s


def parse_hotel_from_graphql(
    hotel: dict,
    destination: str,
    ui_shown_price_vnd_by_property_id: dict[str, float] | None = None,
    ui_name_filter_price_vnd: float | None = None,
    ui_name_filter_query: str = "",
) -> dict | None:
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

    property_page = _safe_get(info, "propertyLinks", "propertyPage", default="") or ""
    link_extra: list[str] = []
    plinks = info.get("propertyLinks")
    if isinstance(plinks, dict):
        for sub in ("propertyUrl", "mobileDeepLink", "desktopUrl", "seoUrl", "deepLink"):
            u = plinks.get(sub)
            if isinstance(u, str) and u.strip():
                link_extra.append(u.strip())

    pid_str, lat_s, lng_s = _extract_agoda_property_geo(hotel)

    incl_gql_s = _extract_price_inclusive(hotel)
    excl_gql_s = _extract_price(hotel)
    price_incl = incl_gql_s
    price_excl = excl_gql_s

    def _apply_dom_or_ui_price(ui_f: float) -> None:
        nonlocal price_incl, price_excl
        price_incl = _format_price(ui_f, "VND")
        ain = _parse_agoda_price_amount_vnd(incl_gql_s)
        aex = _parse_agoda_price_amount_vnd(excl_gql_s)
        if ain is not None and aex is not None and ain > 0:
            price_excl = _format_price(ui_f * (float(aex) / float(ain)), "VND")

    ui_applied = False
    if (
        ui_name_filter_price_vnd is not None
        and (ui_name_filter_query or "").strip()
        and _hotel_name_matches_search_filter(name, ui_name_filter_query)
    ):
        ufn = float(ui_name_filter_price_vnd)
        if ufn >= 150_000:
            _apply_dom_or_ui_price(ufn)
            ui_applied = True
    if not ui_applied and ui_shown_price_vnd_by_property_id:
        ui_amt = None
        for k in _agoda_ui_lookup_keys_for_hotel(
            pid_str, property_page, tuple(link_extra)
        ):
            v = ui_shown_price_vnd_by_property_id.get(k)
            if v is not None:
                ui_amt = v
                break
        if ui_amt is not None and float(ui_amt) >= 200_000:
            _apply_dom_or_ui_price(float(ui_amt))
    meal_plan = _extract_meal_plan(hotel)
    cancellation = _extract_cancellation(hotel)
    review = _extract_review_score(hotel)

    url = f"https://www.agoda.com{property_page}" if property_page else ""

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
    """
    Số trang từ thanh phân trang Agoda (đa ngôn ngữ).
    """
    try:
        el = page.locator("[data-selenium='pagination-text']")
        if await el.count() > 0:
            text = (await el.text_content() or "").strip()
            for pat in (
                r"trên\s+(\d+)",
                r"of\s+(\d+)",
                r"/\s*(\d+)\s*$",
                r"(\d+)\s*$",
            ):
                m = re.search(pat, text, re.I)
                if m:
                    n = int(m.group(1))
                    if 1 <= n <= 500:
                        return n
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

    Lưu ý: Agoda thường ~40 KS/trang; ngưỡng cũ props_on_page>=42 khiến dừng sớm (~200 KS / 5 trang).
    """
    await scroll_pagination_into_view(page)
    await asyncio.sleep(0.45)

    try:
        dom_pages_fresh = await get_total_pages(page)
        total_pages_hint = max(total_pages_hint, dom_pages_fresh)
    except Exception:
        pass

    state: bool | None = None
    for _ in range(22):
        state = await _next_button_clickable(page)
        if state is not None:
            break
        await asyncio.sleep(0.35)

    if state is True:
        return True
    if state is False:
        if current_page < total_pages_hint:
            return True
        if props_on_page >= 28:
            return True
        return False
    if current_page < total_pages_hint:
        return True
    if props_on_page >= 28:
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
    """
    Chặn font / media (nhẹ băng thông). Giữ ảnh: Agoda thường không hydrate thẻ kết quả / giá
    khi ảnh bị chặn — DOM giá UI = 0 selector, scraper chỉ còn giá API lệch promo.
    """
    if route.request.resource_type in ("media", "font"):
        await route.abort()
    else:
        await route.continue_()


async def scrape_agoda(
    url: str,
    destination: str,
    status_callback=None,
    visible_browser: bool | None = None,
) -> list:
    """
    Kết hợp GraphQL (intercept) + giá hiển thị trên thẻ khi cần (URL có hotelName= hoặc map id).
    visible_browser=True: mở cửa sổ Chrome (thường cần để giá khớp UI Agoda).
    """
    results = []

    async with async_playwright() as pw:
        if visible_browser is True:
            headless_on = False
        elif visible_browser is False:
            headless_on = True
        else:
            headless_on = os.environ.get("AGODA_HEADLESS", "1").strip().lower() not in (
                "0",
                "false",
                "no",
                "off",
            )
        launch_kwargs = dict(
            headless=headless_on,
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
                        wait_until="load",
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
            try:
                await page.wait_for_load_state("networkidle", timeout=22000)
            except Exception:
                pass
            await asyncio.sleep(6.0)

            hotel_name_q = _agoda_search_hotel_name_filter(url)

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
            pid_to_result_index: dict[str, int] = {}

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

                ui_shown_vnd: dict[str, float] = {}
                try:
                    try:
                        await page.wait_for_selector(
                            '[data-property-id],[data-selenium="search-web-accommodation-card"]',
                            timeout=28000,
                        )
                    except Exception:
                        pass
                    for _ in range(4):
                        await page.evaluate(
                            "() => window.scrollTo(0, document.body.scrollHeight)"
                        )
                        await asyncio.sleep(0.55)
                    await page.evaluate("() => window.scrollTo(0, 0)")
                    await asyncio.sleep(0.45)
                    ui_shown_vnd = await _agoda_ui_shown_price_vnd_by_property_id(page)
                except Exception:
                    ui_shown_vnd = {}

                ui_name_price: float | None = None
                if hotel_name_q:
                    try:
                        ui_name_price = await _agoda_ui_price_for_hotel_name_query(
                            page, hotel_name_q
                        )
                    except Exception:
                        ui_name_price = None
                    if ui_name_price is None and hotel_name_q:
                        if status_callback:
                            status_callback(
                                "Chưa đọc được giá trên giao diện — chờ thêm 20s và thử lại…"
                            )
                        await asyncio.sleep(20.0)
                        for _ in range(6):
                            try:
                                await page.evaluate(
                                    "() => window.scrollTo(0, document.body.scrollHeight)"
                                )
                            except Exception:
                                break
                            await asyncio.sleep(0.5)
                        try:
                            await page.evaluate("() => window.scrollTo(0, 0)")
                        except Exception:
                            pass
                        await asyncio.sleep(1.0)
                        try:
                            ui_name_price = await _agoda_ui_price_for_hotel_name_query(
                                page, hotel_name_q
                            )
                        except Exception:
                            pass

                for hotel in properties:
                    pid_key = _agoda_property_id_from_hotel(hotel)
                    record = parse_hotel_from_graphql(
                        hotel,
                        destination,
                        ui_shown_price_vnd_by_property_id=ui_shown_vnd or None,
                        ui_name_filter_price_vnd=ui_name_price,
                        ui_name_filter_query=hotel_name_q,
                    )
                    if not record:
                        continue
                    if pid_key:
                        if pid_key in pid_to_result_index:
                            idx = pid_to_result_index[pid_key]
                            results[idx] = _agoda_merge_cheaper_inclusive_row(
                                results[idx], record
                            )
                        else:
                            pid_to_result_index[pid_key] = len(results)
                            results.append(record)
                    else:
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


def run_scrape(
    url: str,
    destination: str,
    status_callback=None,
    *,
    visible_browser: bool | None = None,
) -> list:
    """Synchronous wrapper."""
    _ensure_windows_proactor_policy()
    ensure_playwright_chromium(status_callback)
    return asyncio.run(
        scrape_agoda(url, destination, status_callback, visible_browser=visible_browser)
    )
