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


def _parse_hotel(item: dict, destination: str) -> dict:
    lat_s, lng_s = _ivivu_lat_lng(item)
    return {
        "Tên khách sạn": item.get("hotelName", "").strip(),
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

    add_items(first_items if isinstance(first_items, list) else [])
    status_callback(f"📄 Trang 1/{total_pages}: +{len(first_items) if isinstance(first_items, list) else 0} raw (tổng: {len(results)})")

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
