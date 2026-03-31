"""
iVIVU hotel scraper via internal SearchHotelList API replay.

Flow:
1. Open region URL once with Playwright and capture the first SearchHotelList request.
2. Reuse the captured request payload/headers and replay pageIndex=1..N.
3. Parse response.data.list into the unified output schema.
"""

import asyncio
import json
import math
import re
import sys
from datetime import date
from urllib.parse import quote, urlparse, urlunparse

import requests as _requests
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

from playwright_bootstrap import ensure_playwright_chromium

from geo_extract import scan_json_for_latlng

IVIVU_SEARCH_API = "https://apiportal.ivivu.com/web_prot/gate/search/searchhotel?keyword={kw}"


def _ivivu_page_url_candidates(raw_url: str) -> list[str]:
    """Thử www / không www — một số môi trường chỉ phân giải được một dạng."""
    u = (raw_url or "").strip()
    if not u:
        return []
    if not re.match(r"^https?://", u, re.I):
        u = "https://" + u.lstrip("/")
    p = urlparse(u)
    scheme = p.scheme or "https"
    net = (p.netloc or "").strip()
    path = p.path if p.path else "/"
    if not net:
        return [u]
    low = net.lower()
    hosts = [net]
    if low.startswith("www."):
        hosts.append(net[4:])
    else:
        hosts.append("www." + net)
    seen: set[str] = set()
    out: list[str] = []
    for h in hosts:
        cand = urlunparse((scheme, h, path, p.params, p.query, p.fragment))
        if cand not in seen:
            seen.add(cand)
            out.append(cand)
    return out


async def _ivivu_goto_with_fallbacks(page, url: str, status_callback) -> None:
    """goto bền hơn: thử www / không www + domcontentloaded / load (tránh networkidle)."""
    last_err: BaseException | None = None
    candidates = _ivivu_page_url_candidates(url)
    if not candidates:
        raise RuntimeError("URL iVIVU trống hoặc không hợp lệ.")
    orig_norm = (url or "").strip().rstrip("/")

    for go_url in candidates:
        hard_fail = False
        for wait_until, timeout_ms in (("domcontentloaded", 65000), ("load", 45000)):
            try:
                await page.goto(go_url, wait_until=wait_until, timeout=timeout_ms)
                if go_url.rstrip("/") != orig_norm:
                    status_callback(f"ℹ️ Đã mở iVIVU qua: {go_url[:72]}…")
                return
            except PlaywrightTimeoutError as e:
                last_err = e
            except Exception as e:
                last_err = e
                es = str(e)
                if "ERR_NAME_NOT_RESOLVED" in es:
                    status_callback(
                        "⚠️ DNS không phân giải được tên miền — thử phiên bản URL khác…"
                    )
                    hard_fail = True
                    break
                if "ERR_CONNECTION" in es or "ERR_INTERNET_DISCONNECTED" in es:
                    status_callback(f"⚠️ Lỗi kết nối: {es[:120]}")
                    hard_fail = True
                    break
        if hard_fail:
            continue

    hint = ""
    if last_err and "ERR_NAME_NOT_RESOLVED" in str(last_err):
        hint = (
            " Kiểm tra DNS/mạng (ví dụ DNS 8.8.8.8), VPN/proxy, firewall; "
            "thử mở https://www.ivivu.com trên Chrome cùng máy."
        )
    raise RuntimeError(
        "Không mở được trang iVIVU sau khi thử nhiều URL và chế độ tải." + hint
    ) from last_err

def _ensure_windows_proactor_policy() -> None:
    if sys.platform.startswith("win") and hasattr(asyncio, "WindowsProactorEventLoopPolicy"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())


def _fmt_stars(star_score) -> str:
    """
    iVIVU `rating` commonly appears as 50 for 5-star, 45 for 4.5-star.
    """
    if star_score is None:
        return ""
    try:
        v = float(star_score)
        if v <= 0:
            return ""
        if v > 10:
            v = v / 10.0
        if abs(v - round(v)) < 1e-9:
            return f"{int(round(v))} sao"
        return f"{v:g} sao"
    except Exception:
        return ""


def _fmt_score(point) -> str:
    if point is None:
        return ""
    try:
        v = float(str(point).replace(",", "."))
        if v <= 0:
            return ""
        return f"{v:g}/10"
    except Exception:
        return ""


def _clean_price(text: str) -> str:
    if not text:
        return ""
    t = re.sub(r"\s+", " ", str(text)).strip()
    if "VND" not in t.upper():
        t = f"{t} VND"
    return t


def _full_url(path_or_url: str) -> str:
    if not path_or_url:
        return ""
    if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
        return path_or_url
    return f"https://www.ivivu.com{path_or_url}"


def resolve_ivivu_region_url(destination: str) -> str | None:
    """
    Resolve destination text to iVIVU region URL using iVIVU suggest API.
    Returns absolute URL or None when not found.
    """
    try:
        url = IVIVU_SEARCH_API.format(kw=quote(destination.strip()))
        resp = _requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list) or not data:
            return None
        # Prefer exact region-level result; fallback to first item.
        picked = None
        key = destination.strip().lower()
        for item in data:
            if not isinstance(item, dict):
                continue
            rname = str(item.get("regionName") or "").strip().lower()
            if item.get("type") == 2 and (rname == key or key in rname):
                picked = item
                break
        if picked is None:
            picked = data[0] if isinstance(data[0], dict) else None
        if not picked:
            return None
        return _full_url(str(picked.get("regionUrl") or ""))
    except Exception:
        return None


def _ivivu_coord_str(v) -> str:
    if v is None:
        return ""
    try:
        x = float(v)
        return f"{x:.8f}".rstrip("0").rstrip(".")
    except (TypeError, ValueError):
        return ""


def _ivivu_lat_lng(item: dict) -> tuple[str, str]:
    if not isinstance(item, dict):
        return "", ""
    la = item.get("latitude") or item.get("lat") or item.get("mapLat")
    lo = item.get("longitude") or item.get("lng") or item.get("mapLng")
    lat_s, lng_s = _ivivu_coord_str(la), _ivivu_coord_str(lo)
    if lat_s and lng_s:
        return lat_s, lng_s
    for key in ("location", "geo", "position"):
        b = item.get(key)
        if isinstance(b, dict):
            la = b.get("latitude") or b.get("lat")
            lo = b.get("longitude") or b.get("lng")
            lat_s, lng_s = _ivivu_coord_str(la), _ivivu_coord_str(lo)
            if lat_s and lng_s:
                return lat_s, lng_s
    lat_s, lng_s = scan_json_for_latlng(item)
    return lat_s, lng_s


def _ivivu_property_id(item: dict) -> str:
    if not isinstance(item, dict):
        return ""
    for k in ("hotelId", "hotelID", "id", "productId", "hotelCode", "code"):
        v = item.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


_IVIVU_EXCEL_ILLEGAL = re.compile(r"[\000-\010]|[\013-\014]|[\016-\037]")


def _ivivu_clean_taf_text(s: str, max_len: int = 500) -> str:
    t = _IVIVU_EXCEL_ILLEGAL.sub("", (s or "").strip())
    t = re.sub(r"\s+", " ", t)
    return t[:max_len] if len(t) > max_len else t


# Không dùng "nghỉ dưỡng" đơn lẻ — trùng tên KS ("Khu Nghỉ Dưỡng …") gây nhầm thành ribbon.
_IVIVU_MARKETING_RIBBON = re.compile(
    r"ưu\s*đãi|khuyến\s*mãi|đặc\s*biệt|trọn\s*gói|flash\s*sale|"
    r"dành\s*cho\s*khách|áp\s*dụng\s*cho|expats|miễn\s*phí\s*hủy|"
    r"combo\s*fantastic|nghỉ\s*dưỡng\s+trọn|ưu\s*đãi\s+.*nghỉ|"
    r"đặt\s*trước|quốc\s*tịch|tất\s*cả\s*quốc",
    re.I,
)


def _ivivu_text_looks_like_marketing_ribbon(s: str) -> bool:
    """Dòng marketing trên ribbon (vd. Ưu Đãi Đặc Biệt (Áp dụng cho…))."""
    if not s or len(s) > 320:
        return False
    return bool(_IVIVU_MARKETING_RIBBON.search(s))


def _ivivu_text_looks_like_promo_badge(s: str) -> bool:
    """Có mẫu gói đêm kiểu 2N1Đ — không phải tag địa điểm/tiện ích."""
    if not s or len(s) > 220:
        return False
    if re.search(r"\d+\s*[Nn]\s*\d+\s*[Đđ]", s):
        return True
    return bool(re.search(r"(?<![A-Za-zÀ-ỹ])\d+\s*[Nn]\s*\d+\s*D(?:\s|[+|]|$)", s))


def _ivivu_text_same_as_hotel_name(s: str, hotel_name: str) -> bool:
    """Trùng tên KS (sau chuẩn hóa khoảng trắng) — không dùng 'substring' để tránh loại nhầm ribbon dài."""
    if not s or not hotel_name:
        return False
    a = re.sub(r"\s+", " ", s.strip().lower())
    b = re.sub(r"\s+", " ", hotel_name.strip().lower())
    return a == b


def _ivivu_text_looks_like_amenity_location_chip(s: str) -> bool:
    """Chip xám địa điểm/tiện ích (vd. Bãi Sau | Xe đưa đón) — không phải ribbon promo."""
    if not s or len(s) > 200:
        return False
    if _ivivu_text_looks_like_marketing_ribbon(s) or _ivivu_text_looks_like_promo_badge(s):
        return False
    t = s.lower()
    if "|" not in s:
        return False
    markers = (
        "bãi sau",
        "bãi trước",
        "trung tâm",
        "gần biển",
        "xe đưa đón",
        "chợ đêm",
        "chợ hải sản",
        "ga tàu",
        "sân bay",
        "nhà hàng",
        "hồ bơi",
        "view biển",
    )
    return any(m in t for m in markers)


def _ivivu_string_to_badge(s: str) -> str:
    """Gộp nhận diện gói đêm + dòng marketing; lọc chip tiện ích."""
    t = _ivivu_clean_taf_text(s, max_len=900)
    if not t or _ivivu_text_looks_like_amenity_location_chip(t):
        return ""
    combo = _ivivu_slice_combo_badge(t)
    if combo and (
        _ivivu_text_looks_like_marketing_ribbon(t)
        or len(t) > len(combo) + 8
    ):
        return t[:900]
    if combo:
        return combo
    if _ivivu_text_looks_like_marketing_ribbon(t) or _ivivu_text_looks_like_promo_badge(t):
        return t[:900]
    return ""


def _ivivu_slice_combo_badge(s: str) -> str:
    """
    Cắt đúng đoạn badge promo: từ 2N1Đ… tới tối đa 2 nhánh | (thường là giá 1tr099 / 750k).
    Không lấy chuỗi chỉ có 'Bãi Sau | Xe đưa đón…' (không có NĐêm).
    """
    t = _ivivu_clean_taf_text(s)
    if not t:
        return ""
    # Tối đa một dấu | phía sau (tên gói | giá); tránh nuốt chữ nối sau giá.
    m = re.search(
        r"(\d+\s*[Nn]\s*\d+\s*[Đđ][^|]{0,130}(?:\|[^|]{0,28})?)",
        t,
    )
    if m:
        return m.group(1).strip()[:220]
    m = re.search(
        r"(\d+\s*[Nn]\s*\d+\s*D[^|]{0,130}(?:\|[^|]{0,28})?)",
        t,
        re.I,
    )
    if m:
        return m.group(1).strip()[:220]
    return ""


def _ivivu_deep_find_badget_json(obj, depth: int = 0, max_d: int = 6) -> str:
    """
    iVIVU dùng typo 'badget' trong class DOM (pdv__badget-text); JSON có thể có key tương tự.
    """
    if depth > max_d:
        return ""
    if isinstance(obj, dict):
        for k, v in obj.items():
            kn = str(k).lower().replace("_", "")
            if "badget" in kn:
                if isinstance(v, str):
                    t = _ivivu_clean_taf_text(v, max_len=900)
                    if t and not _ivivu_text_looks_like_amenity_location_chip(t):
                        got = _ivivu_string_to_badge(t)
                        if got:
                            return got[:900]
                        if _ivivu_text_looks_like_marketing_ribbon(t) or _ivivu_text_looks_like_promo_badge(
                            t
                        ):
                            return t[:900]
                elif isinstance(v, (dict, list)):
                    got = _ivivu_badge_from_value(v)
                    if got:
                        return got
        for v in obj.values():
            if isinstance(v, (dict, list)):
                got = _ivivu_deep_find_badget_json(v, depth + 1, max_d)
                if got:
                    return got
    elif isinstance(obj, list):
        for it in obj[:40]:
            if isinstance(it, (dict, list)):
                got = _ivivu_deep_find_badget_json(it, depth + 1, max_d)
                if got:
                    return got
    return ""


def _ivivu_badge_from_value(val) -> str:
    """Trích badge từ string / dict / list — gói đêm 2N1Đ hoặc dòng marketing ribbon."""
    if isinstance(val, str):
        return _ivivu_string_to_badge(val)
    if isinstance(val, dict):
        for kk in (
            "title",
            "name",
            "text",
            "label",
            "taf",
            "description",
            "content",
            "promotionLabel",
            "dealText",
        ):
            raw = str(val.get(kk) or "")
            got = _ivivu_string_to_badge(raw)
            if got:
                return got
    if isinstance(val, list):
        for el in val[:12]:
            if isinstance(el, str):
                got = _ivivu_string_to_badge(el)
                if got:
                    return got
            elif isinstance(el, dict):
                got = _ivivu_badge_from_value(el)
                if got:
                    return got
    return ""


def _ivivu_extract_badge_label(item: dict) -> str:
    """
    Mọi ribbon / nhãn promo trên card (DOM pdv__badget-text và tương tự):
    vd. 2N1Đ … | 1tr099, Ưu Đãi Đặc Biệt (…), Ưu Đãi Nghỉ Dưỡng Trọn Gói (…).
    Không lấy chip xám tiện ích/địa điểm (Bãi Sau | …) khi suy từ JSON.
    """
    if not isinstance(item, dict):
        return ""
    hotel_name = (item.get("hotelName") or "").strip()

    def _ok_badge(got: str) -> bool:
        return bool(got) and not _ivivu_text_same_as_hotel_name(got, hotel_name)

    dom_inj = item.get("_ivivuPdvBadgetDom")
    if isinstance(dom_inj, str) and dom_inj.strip():
        parts = [p.strip() for p in re.split(r"\s*•\s*", dom_inj) if p.strip()]
        kept = [
            p
            for p in parts
            if not _ivivu_text_looks_like_amenity_location_chip(p)
            and not _ivivu_text_same_as_hotel_name(p, hotel_name)
        ]
        merged = " • ".join(kept) if kept else ""
        if not merged:
            raw_one = _ivivu_clean_taf_text(dom_inj, max_len=900).strip()
            if raw_one and not _ivivu_text_same_as_hotel_name(raw_one, hotel_name):
                merged = raw_one
        if merged:
            return _ivivu_clean_taf_text(merged, max_len=900)[:900]

    cmap = {str(k).lower().replace("_", ""): v for k, v in item.items()}

    for dk in ("pdvbadgettext", "badgettext", "packagebadget", "hotelbadget"):
        v = cmap.get(dk)
        if isinstance(v, str) and v.strip():
            got = _ivivu_string_to_badge(v)
            if not got:
                t = _ivivu_clean_taf_text(v, max_len=900).strip()
                if t and (
                    _ivivu_text_looks_like_marketing_ribbon(t)
                    or _ivivu_text_looks_like_promo_badge(t)
                ):
                    got = t
            if _ok_badge(got):
                return got[:900]

    got_deep = _ivivu_deep_find_badget_json(item)
    if _ok_badge(got_deep):
        return got_deep[:900]

    direct_keys = (
        "taf",
        "taftext",
        "badge",
        "badgetext",
        "badgelabel",
        "promotionlabel",
        "promolabel",
        "dealtag",
        "combolabel",
        "packagelabel",
        "packagename",
        "flashsalelabel",
        "ribbontext",
        "shortpromo",
        "promotiontitle",
        "offername",
        "salelabel",
        "productlabel",
    )
    for dk in direct_keys:
        got = _ivivu_badge_from_value(cmap.get(dk))
        if _ok_badge(got):
            return got

    # Không đọc labels / tags / badges / hotelBadges — thường là tiện ích khu vực, không phải 2N1Đ.
    for nest in (
        "promotion",
        "promotions",
        "deal",
        "package",
        "combo",
        "offer",
        "flashsale",
    ):
        sub = item.get(nest)
        if sub is None:
            continue
        got = _ivivu_badge_from_value(sub)
        if _ok_badge(got):
            return got

    for _k, v in item.items():
        kn = str(_k).lower().replace("_", "")
        if kn in ("hotelname", "hotelnameslug", "slug", "address", "description"):
            continue
        if isinstance(v, str):
            if _ivivu_text_same_as_hotel_name(v, hotel_name):
                continue
            got = _ivivu_string_to_badge(v)
            if _ok_badge(got):
                return got
        elif isinstance(v, (list, dict)):
            got = _ivivu_badge_from_value(v)
            if _ok_badge(got):
                return got

    return ""


def _parse_hotel(item: dict, destination: str) -> dict:
    lat_s, lng_s = _ivivu_lat_lng(item)
    return {
        "Tên khách sạn": item.get("hotelName", "").strip(),
        "Nhãn badge": _ivivu_extract_badge_label(item),
        "Link khách sạn": _full_url(item.get("hotelLink") or item.get("url") or ""),
        "Địa chỉ": (item.get("address") or "").strip(),
        "Hạng sao": _fmt_stars(item.get("rating")),
        "Điểm đánh giá": _fmt_score(item.get("point")),
        "Số đánh giá": str(int(item.get("reviewCount") or 0)) if item.get("reviewCount") else "",
        "Giá/đêm (VND)": _clean_price(item.get("minPrice") or item.get("showPrice") or ""),
        "Chính sách hoàn hủy": "",
        "Nguồn": "ivivu.com",
        "Điểm đến": destination,
        "Mã Property Agoda": "",
        "ID khách sạn Travel": "",
        "Mã property (OTA)": _ivivu_property_id(item),
        "Vĩ độ": lat_s,
        "Kinh độ": lng_s,
    }


async def _capture_search_context(url: str, status_callback):
    captured: dict = {}
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
        )
        page = await browser.new_page()

        async def on_response(resp):
            if "SearchHotelList" not in resp.url or "payload" in captured:
                return
            try:
                data = await resp.json()
            except Exception:
                return
            if not isinstance(data, dict) or not data.get("success"):
                return
            payload = data.get("data") or {}
            items = payload.get("list") or []
            if not isinstance(items, list):
                return
            req = resp.request
            captured["url"] = resp.url
            captured["method"] = req.method
            captured["headers"] = dict(req.headers)
            captured["body"] = req.post_data or "{}"
            captured["first_json"] = data

        page.on("response", on_response)

        status_callback("🌐 Đang mở iVIVU để lấy ngữ cảnh truy vấn…")
        await _ivivu_goto_with_fallbacks(page, url, status_callback)

        for _ in range(20):
            if "body" in captured:
                break
            await asyncio.sleep(0.5)

        # Ribbon / nhãn promo trên từng card (2N1Đ, Ưu Đãi…), gom nhiều span / card bằng " • ".
        if "body" in captured:
            dom_texts: list[str] = []
            for _ in range(4):
                try:
                    dom_texts = await page.evaluate(
                        """() => {
                            const norm = (s) => (s || '').replace(/\\s+/g, ' ').trim();
                            /* Trong cùng card có nhiều ribbon (Ưu Đãi Đặc Biệt, Đặt Trước 5 Ngày…) — class có thể khác badget-text */
                            const strictSels = [
                                '.pdv__badget-text',
                                "[class*='pdv__badget-text']",
                                "[class*='badget-text']",
                                "[class*='badge-text']",
                            ];
                            /* Chỉ dùng trong <a> có ảnh KS — tránh link tiêu đề (nuốt tên KS) */
                            const overlaySels = [
                                "[class*='ribbon']",
                                "[class*='pdv__ribbon']",
                                "[class*='pdv__sale']",
                                "[class*='pdv__promo']",
                                "[class*='promo-tag']",
                                "[class*='tag-promo']",
                                "[class*='promo-label']",
                                "[class*='offer-tag']",
                                "[class*='discount-tag']",
                                "[class*='label--promo']",
                                "[class*='uu-dai']",
                            ];
                            function findCardRoot(node) {
                                let el = node;
                                for (let i = 0; i < 14 && el; i++) {
                                    if (!el || el === document.body) return null;
                                    const cls = typeof el.className === 'string' ? el.className : '';
                                    const tag = (el.tagName || '').toUpperCase();
                                    if (tag === 'LI' || tag === 'ARTICLE') return el;
                                    if (cls.includes('hotel') && (cls.includes('item') || cls.includes('card') || cls.includes('product'))) return el;
                                    if (cls.includes('product-item') || cls.includes('search-item') || cls.includes('result-item')) return el;
                                    if (cls.includes('pdv') && (cls.includes('item') || cls.includes('row') || cls.includes('block'))) return el;
                                    el = el.parentElement;
                                }
                                return null;
                            }
                            function imageLinkScopesInCard(root) {
                                const scopes = [];
                                const seen = new WeakSet();
                                root.querySelectorAll('a[href*="/khach-san/"]').forEach((L) => {
                                    if (!L.querySelector('img')) return;
                                    if (seen.has(L)) return;
                                    seen.add(L);
                                    scopes.push(L);
                                });
                                return scopes;
                            }
                            function finalizeRibbonNodes(nodes) {
                                nodes.sort((a, b) => {
                                    const pos = a.compareDocumentPosition(b);
                                    if (pos & Node.DOCUMENT_POSITION_FOLLOWING) return -1;
                                    if (pos & Node.DOCUMENT_POSITION_PRECEDING) return 1;
                                    return 0;
                                });
                                const seen = new Set();
                                const parts = [];
                                for (const el of nodes) {
                                    const t = norm(el.textContent);
                                    if (t.length < 3 || t.length > 320) continue;
                                    if (seen.has(t)) continue;
                                    seen.add(t);
                                    parts.push(t);
                                }
                                return parts.join(' • ');
                            }
                            function collectFromRoot(root) {
                                const linkScopes = imageLinkScopesInCard(root);
                                const nodes = [];
                                const addAll = (scope, selStr) => {
                                    try {
                                        scope.querySelectorAll(selStr).forEach((el) => nodes.push(el));
                                    } catch (e) {}
                                };
                                if (linkScopes.length) {
                                    for (const scope of linkScopes) {
                                        strictSels.forEach((s) => addAll(scope, s));
                                        overlaySels.forEach((s) => addAll(scope, s));
                                    }
                                } else {
                                    const scope = root;
                                    strictSels.forEach((s) => addAll(scope, s));
                                }
                                return finalizeRibbonNodes(nodes);
                            }
                            const rootsOrdered = [];
                            const seenRoot = new WeakSet();
                            function pushRoot(r) {
                                if (!r || seenRoot.has(r)) return;
                                seenRoot.add(r);
                                rootsOrdered.push(r);
                            }
                            document.querySelectorAll('a[href*="/khach-san/"]').forEach((a) => {
                                const href = a.getAttribute('href') || '';
                                if (href.length < 12) return;
                                const root = findCardRoot(a) || a.closest('li') || a.closest('[class*="item"]');
                                if (root) pushRoot(root);
                            });
                            if (rootsOrdered.length >= 3) {
                                rootsOrdered.sort((a, b) => {
                                    const ra = a.getBoundingClientRect();
                                    const rb = b.getBoundingClientRect();
                                    return ra.top - rb.top || ra.left - rb.left;
                                });
                                return rootsOrdered.map((root) => collectFromRoot(root));
                            }
                            const flat = [];
                            document.querySelectorAll('a[href*="/khach-san/"]').forEach((a) => {
                                if (!a.querySelector('img')) return;
                                const nodes = [];
                                const addAll = (selStr) => {
                                    try {
                                        a.querySelectorAll(selStr).forEach((el) => nodes.push(el));
                                    } catch (e) {}
                                };
                                strictSels.forEach((s) => addAll(s));
                                overlaySels.forEach((s) => addAll(s));
                                const joined = finalizeRibbonNodes(nodes);
                                if (joined) flat.push(joined);
                            });
                            return flat;
                        }"""
                    )
                except Exception:
                    dom_texts = []
                if isinstance(dom_texts, list) and dom_texts:
                    break
                await asyncio.sleep(0.9)
            captured["dom_badget_texts"] = dom_texts if isinstance(dom_texts, list) else []

        await browser.close()
    return captured if "body" in captured else None


async def _scrape_async(
    url: str,
    destination: str,
    status_callback,
    check_in: str = "",
    check_out: str = "",
    rooms: int = 1,
    adults: int = 2,
    children: int = 0,
) -> list[dict]:
    ctx = await _capture_search_context(url, status_callback)
    if not ctx:
        raise RuntimeError("Không lấy được dữ liệu API iVIVU từ URL này.")

    try:
        base_body = json.loads(ctx["body"])
    except Exception:
        base_body = {}
    page_size = int(base_body.get("pageSize") or 15)

    # Override search params from UI if provided.
    if check_in:
        base_body["checkInDate"] = check_in
    if check_out:
        base_body["checkOutDate"] = check_out
    rp = base_body.get("roomPicker")
    if not isinstance(rp, dict):
        rp = {}
        base_body["roomPicker"] = rp
    rp["adultNumber"] = int(max(1, adults))
    rp["childNumber"] = int(max(0, children))
    rp["roomNumber"] = int(max(1, rooms))
    if "childAges" not in rp or not isinstance(rp.get("childAges"), list):
        rp["childAges"] = []

    sess = _requests.Session()
    headers = dict(ctx.get("headers") or {})
    for hk in list(headers.keys()):
        if hk.lower() in {"content-length", "host", "connection", "accept-encoding"}:
            headers.pop(hk, None)
    sess.headers.update(headers)

    results: list[dict] = []
    seen: set[str] = set()

    def add_items(items: list[dict]) -> int:
        added = 0
        for it in items:
            hid = str(it.get("hotelId") or it.get("hotelCode") or "").strip()
            name = (it.get("hotelName") or "").strip().lower()
            key = hid or name
            if not key or key in seen:
                continue
            seen.add(key)
            results.append(_parse_hotel(it, destination))
            added += 1
        return added

    # Always fetch page 1 with the effective body to avoid stale captured dates.
    first_resp = sess.post(ctx["url"], json={**base_body, "pageIndex": 1}, timeout=20)
    first_resp.raise_for_status()
    first_data = (first_resp.json().get("data") or {})
    first_items = first_data.get("list") or []
    total_hotels = int(first_data.get("total") or 0)
    total_pages = max(1, math.ceil(total_hotels / max(page_size, 1))) if total_hotels else 1
    total_pages = min(total_pages, 200)

    status_callback(f"📊 iVIVU: tổng ~{total_hotels or '?'} khách sạn, {total_pages} trang")

    if not isinstance(first_items, list):
        first_items = []
    dom_bg = ctx.get("dom_badget_texts") or []
    if dom_bg and first_items:
        merged: list = []
        for i, it in enumerate(first_items):
            if isinstance(it, dict):
                d = dict(it)
                if i < len(dom_bg):
                    raw = str(dom_bg[i] or "").strip()
                    if raw:
                        cleaned = _ivivu_clean_taf_text(raw, max_len=900)
                        hn = (d.get("hotelName") or "").strip()
                        if not _ivivu_text_same_as_hotel_name(cleaned, hn):
                            d["_ivivuPdvBadgetDom"] = cleaned
                merged.append(d)
            else:
                merged.append(it)
        first_items = merged
        status_callback(
            f"ℹ️ iVIVU: nhãn ribbon (card DOM) — {len(dom_bg)} ô, "
            f"ghép thứ tự với {len(first_items)} KS trang 1."
        )
    elif dom_bg and not first_items:
        status_callback(f"ℹ️ iVIVU: có {len(dom_bg)} ribbon DOM nhưng API trang 1 rỗng.")

    add_items(first_items)
    status_callback(f"📄 Trang 1/{total_pages}: +{len(first_items)} raw (tổng: {len(results)})")

    consecutive_empty = 0
    for pg in range(2, total_pages + 1):
        if len(results) >= 3000:
            status_callback("⚠️ Đạt giới hạn 3000 khách sạn, dừng.")
            break

        body = json.loads(json.dumps(base_body, ensure_ascii=False))
        body["pageIndex"] = pg
        try:
            resp = sess.post(ctx["url"], json=body, timeout=20)
            resp.raise_for_status()
            payload = resp.json().get("data") or {}
            items = payload.get("list") or []
        except Exception as e:
            consecutive_empty += 1
            status_callback(f"  ⚠️ Trang {pg} lỗi: {type(e).__name__}")
            if consecutive_empty >= 3:
                break
            continue

        added = add_items(items if isinstance(items, list) else [])
        status_callback(f"📄 Trang {pg}/{total_pages}: +{added} mới (tổng: {len(results)})")

        if added == 0:
            consecutive_empty += 1
            if consecutive_empty >= 3:
                status_callback("⚠️ 3 trang liên tiếp không có mới, dừng.")
                break
        else:
            consecutive_empty = 0

    return results


def run_scrape_ivivu(
    url: str,
    destination: str,
    status_callback=None,
    check_in: str = "",
    check_out: str = "",
    rooms: int = 1,
    adults: int = 2,
    children: int = 0,
) -> list[dict]:
    if status_callback is None:
        status_callback = print
    _ensure_windows_proactor_policy()
    ensure_playwright_chromium(status_callback)
    return asyncio.run(
        _scrape_async(
            url, destination, status_callback,
            check_in=check_in, check_out=check_out,
            rooms=rooms, adults=adults, children=children,
        )
    )
