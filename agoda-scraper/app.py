import streamlit as st
import pandas as pd
import io
import hashlib
import re
import traceback
from urllib.parse import urlparse, parse_qs, parse_qsl, urlencode, urlunparse
from datetime import date, timedelta
from scraper import build_agoda_url, run_scrape
from scraper_tripcom import build_tripcom_url, resolve_trip_city, run_scrape_tripcom
from scraper_mytour import build_mytour_url, resolve_mytour_city, run_scrape_mytour
from scraper_travelcomvn import build_travel_url, resolve_travel_city, run_scrape_travel
from scraper_ivivu import run_scrape_ivivu, resolve_ivivu_region_url
from scraper_findtourgo import build_findtourgo_url, normalize_findtourgo_departure_display, run_scrape_findtourgo_tours
from scraper_travelcomvn_tour import build_travel_tour_url, resolve_travel_tour_slug, run_scrape_travel_tour
from market_db import (
    db_ready,
    init_db,
    replace_case_source,
    replace_tour_case_source,
    list_hotel_cases,
    list_tour_cases,
    build_cross_channel_compare,
    get_case_rows,
    get_tour_case_rows,
    delete_hotel_case_source,
    delete_tour_case_source,
)

# openpyxl từ chối một số ký tự điều khiển XML 1.0 (vd. \x08 trong tên KS từ iVIVU).
_EXCEL_ILLEGAL_STR = re.compile(r"[\000-\010]|[\013-\014]|[\016-\037]")


def _sanitize_str_for_openpyxl(val):
    if not isinstance(val, str):
        return val
    return _EXCEL_ILLEGAL_STR.sub("", val)


def _sanitize_df_for_openpyxl(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        if out[c].dtype == object or pd.api.types.is_string_dtype(out[c]):
            out[c] = out[c].apply(
                lambda x: _sanitize_str_for_openpyxl(x) if isinstance(x, str) else x
            )
    return out


st.set_page_config(
    page_title="OTA Hotel Scraper",
    page_icon="🏨",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.dialog("Xóa dữ liệu kênh này?")
def _dlg_confirm_delete_hotel_case():
    d = st.session_state.get("_hotel_case_delete_draft")
    if not d:
        return
    ck = d.get("case_key") or ""
    lb = d.get("label") or ""
    ota = d.get("ota") or "ota"
    st.write(
        f"Xóa **chỉ** dữ liệu khách sạn đã lưu của **{ota}** cho case này? "
        "Các OTA khác cùng case key không bị động."
    )
    st.caption("Case key dùng chung cho cùng điểm đến / ngày / số khách; mỗi kênh lưu snapshot riêng.")
    st.caption(lb)
    if ck:
        st.caption(f"`{ck}`")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Hủy", key=f"dlg_hotel_cancel_{ota}", use_container_width=True):
            st.session_state.pop("_hotel_case_delete_draft", None)
            st.rerun()
    with c2:
        if st.button("Xóa", key=f"dlg_hotel_ok_{ota}", type="primary", use_container_width=True):
            if not ck.strip():
                st.session_state.pop("_hotel_case_delete_draft", None)
                st.session_state["_hotel_db_feedback"] = ("err", "Thiếu case key.")
                st.rerun()
                return
            ok_del, msg_del = delete_hotel_case_source(ck, ota)
            st.session_state.pop("_hotel_case_delete_draft", None)
            if ok_del:
                _invalidate_hotel_case_list_cache()
                if st.session_state.get("active_case_key") == ck and st.session_state.get("active_source") == ota:
                    st.session_state.scrape_results = None
                st.session_state.pop("global_compare_df", None)
                st.session_state["_hotel_db_feedback"] = ("ok", msg_del)
            else:
                st.session_state["_hotel_db_feedback"] = ("err", msg_del)
            st.rerun()


@st.dialog("Xóa dữ liệu nguồn này?")
def _dlg_confirm_delete_tour_case():
    d = st.session_state.get("_tour_case_delete_draft")
    if not d:
        return
    ck = d.get("case_key") or ""
    lb = d.get("label") or ""
    ota = d.get("ota") or "ota"
    st.write(
        f"Xóa **chỉ** dữ liệu tour đã lưu của **{ota}** cho case này? "
        "Nguồn khác cùng case key không bị động."
    )
    st.caption(lb)
    if ck:
        st.caption(f"`{ck}`")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Hủy", key=f"dlg_tour_cancel_{ota}", use_container_width=True):
            st.session_state.pop("_tour_case_delete_draft", None)
            st.rerun()
    with c2:
        if st.button("Xóa", key=f"dlg_tour_ok_{ota}", type="primary", use_container_width=True):
            if not ck.strip():
                st.session_state.pop("_tour_case_delete_draft", None)
                st.session_state["_tour_db_feedback"] = ("err", "Thiếu case key.")
                st.rerun()
                return
            ok_del, msg_del = delete_tour_case_source(ck, ota)
            st.session_state.pop("_tour_case_delete_draft", None)
            if ok_del:
                _invalidate_tour_case_list_cache()
                if st.session_state.get("active_case_key") == ck and st.session_state.get("active_source") == ota:
                    st.session_state.scrape_results = None
                st.session_state["_tour_db_feedback"] = ("ok", msg_del)
            else:
                st.session_state["_tour_db_feedback"] = ("err", msg_del)
            st.rerun()


def normalize_agoda_direct_url(raw_url: str) -> str:
    """
    Normalize pasted Agoda URL to broad search mode.
    We force `productType=-1` to avoid narrow inventory subsets (e.g. productType=2).
    """
    try:
        parsed = urlparse(raw_url.strip())
        params = dict(parse_qsl(parsed.query, keep_blank_values=True))
        params["productType"] = "-1"
        params["currency"] = "VND"
        params["currencyCode"] = "VND"
        params["priceCur"] = "VND"
        new_query = urlencode(params, doseq=True)
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))
    except Exception:
        return raw_url.strip()


_HOTEL_ID_GEO_KEYS = frozenset(
    {"Mã Property Agoda", "ID khách sạn Travel", "Mã property (OTA)", "Vĩ độ", "Kinh độ"}
)

# Cột chỉ hiện trên một số OTA (vd. badge promo 2N1Đ | giá trên card iVIVU).
_HOTEL_COLS_ONLY_FOR_SOURCES: dict[str, frozenset[str]] = {
    "Nhãn badge": frozenset({"iVIVU"}),
}

HOTEL_RESULT_COLUMNS = [
    "Phân khúc",
    "Nguồn",
    "Tỉnh thành / Điểm đến",
    "Tên khách sạn",
    "Nhãn badge",
    "Địa chỉ",
    "Mã Property Agoda",
    "ID khách sạn Travel",
    "Mã property (OTA)",
    "Vĩ độ",
    "Kinh độ",
    "Hạng sao",
    "Điểm đánh giá",
    "Số đánh giá",
    "Giá/đêm (VND)",
    "Giá/đêm (chưa gồm thuế)",
    "Giá/đêm (đã gồm thuế)",
    "Thuế phí ước tính",
    "Gói bữa ăn",
    "Chính sách hoàn hủy",
    "Link khách sạn",
]


def hotel_table_column_order(source: str) -> list[str]:
    """Chỉ Travel.com.vn có 2 cột ID (Agoda + Travel); OTA khác: một mã property + geo."""
    keep = {
        "Agoda": {"Mã Property Agoda", "Vĩ độ", "Kinh độ"},
        "Trip.com": {"Mã property (OTA)", "Vĩ độ", "Kinh độ"},
        "Mytour.vn": {"Mã property (OTA)", "Vĩ độ", "Kinh độ"},
        "iVIVU": {"Mã property (OTA)", "Vĩ độ", "Kinh độ"},
        "Travel.com.vn": {"Mã Property Agoda", "ID khách sạn Travel", "Vĩ độ", "Kinh độ"},
    }
    ks = keep.get(source, {"Vĩ độ", "Kinh độ"})
    out: list[str] = []
    for c in HOTEL_RESULT_COLUMNS:
        allowed = _HOTEL_COLS_ONLY_FOR_SOURCES.get(c)
        if allowed is not None and source not in allowed:
            continue
        if c not in _HOTEL_ID_GEO_KEYS or c in ks:
            out.append(c)
    return out


TOUR_RESULT_COLUMNS = [
    "Phân khúc",
    "Nguồn",
    "Quốc gia",
    "Tên tour",
    "Mã tour",
    "Công ty lữ hành",
    "Thời lượng (ngày)",
    "Giá từ",
    "Tiền tệ",
    "Điểm đánh giá",
    "Xếp hạng nội bộ",
    "Điểm khởi hành",
    "Phương tiện",
    "Ngày khởi hành",
    "Link tour",
]


def _pick_text(row: dict, *keys: str) -> str:
    for k in keys:
        if k in row:
            v = row.get(k)
            txt = "" if v is None else str(v).strip()
            if txt:
                return txt
    return ""


def normalize_hotel_rows(rows: list, source: str, destination: str) -> list:
    out = []
    for item in rows or []:
        row = item if isinstance(item, dict) else {}
        price_pre_tax = _pick_text(row, "Giá/đêm (chưa gồm thuế)", "Giá/đêm (chưa gồm thuế phí)")
        price_tax = _pick_text(row, "Giá/đêm (đã gồm thuế)", "Giá/đêm (VND)")
        price_vnd = _pick_text(row, "Giá/đêm (VND)", "Giá/đêm (đã gồm thuế)", "Giá/đêm (chưa gồm thuế)")
        normalized = {
            "Phân khúc": "Hotel",
            "Nguồn": source,
            "Tỉnh thành / Điểm đến": _pick_text(row, "Tỉnh thành / Điểm đến", "Tỉnh/Thành") or destination,
            "Tên khách sạn": _pick_text(row, "Tên khách sạn"),
            "Nhãn badge": _pick_text(row, "Nhãn badge", "Taf"),
            "Địa chỉ": _pick_text(row, "Địa chỉ", "Địa điểm nổi bật"),
            "Mã Property Agoda": _pick_text(row, "Mã Property Agoda"),
            "ID khách sạn Travel": _pick_text(row, "ID khách sạn Travel"),
            "Mã property (OTA)": _pick_text(row, "Mã property (OTA)"),
            "Vĩ độ": _pick_text(row, "Vĩ độ"),
            "Kinh độ": _pick_text(row, "Kinh độ"),
            "Hạng sao": _pick_text(row, "Hạng sao"),
            "Điểm đánh giá": _pick_text(row, "Điểm đánh giá"),
            "Số đánh giá": _pick_text(row, "Số đánh giá"),
            "Giá/đêm (VND)": price_vnd,
            "Giá/đêm (chưa gồm thuế)": price_pre_tax,
            "Giá/đêm (đã gồm thuế)": price_tax,
            "Thuế phí ước tính": _pick_text(row, "Thuế phí ước tính"),
            "Gói bữa ăn": _pick_text(row, "Gói bữa ăn"),
            "Chính sách hoàn hủy": _pick_text(row, "Chính sách hoàn hủy"),
            "Link khách sạn": _pick_text(row, "Link khách sạn"),
        }
        out.append(normalized)
    return out


def _norm_destination_key(text: str) -> str:
    txt = (text or "").strip().lower()
    replacements = {
        "đ": "d", "á": "a", "à": "a", "ả": "a", "ã": "a", "ạ": "a",
        "ă": "a", "ắ": "a", "ằ": "a", "ẳ": "a", "ẵ": "a", "ặ": "a",
        "â": "a", "ấ": "a", "ầ": "a", "ẩ": "a", "ẫ": "a", "ậ": "a",
        "é": "e", "è": "e", "ẻ": "e", "ẽ": "e", "ẹ": "e",
        "ê": "e", "ế": "e", "ề": "e", "ể": "e", "ễ": "e", "ệ": "e",
        "í": "i", "ì": "i", "ỉ": "i", "ĩ": "i", "ị": "i",
        "ó": "o", "ò": "o", "ỏ": "o", "õ": "o", "ọ": "o",
        "ô": "o", "ố": "o", "ồ": "o", "ổ": "o", "ỗ": "o", "ộ": "o",
        "ơ": "o", "ớ": "o", "ờ": "o", "ở": "o", "ỡ": "o", "ợ": "o",
        "ú": "u", "ù": "u", "ủ": "u", "ũ": "u", "ụ": "u",
        "ư": "u", "ứ": "u", "ừ": "u", "ử": "u", "ữ": "u", "ự": "u",
        "ý": "y", "ỳ": "y", "ỷ": "y", "ỹ": "y", "ỵ": "y",
    }
    for src, dst in replacements.items():
        txt = txt.replace(src, dst)
    txt = re.sub(r"[^a-z0-9]+", "-", txt).strip("-")
    return txt or "unknown"


def _normalize_date_text(raw: str) -> str:
    txt = (raw or "").strip()
    if not txt:
        return ""
    if re.match(r"^\d{4}-\d{2}-\d{2}$", txt):
        return txt
    m = re.match(r"^(\d{2})-(\d{2})-(\d{4})$", txt)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", txt)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return txt


def _extract_hotel_case_info(active_source: str, active_url: str, active_destination: str) -> dict:
    parsed = urlparse(active_url or "")
    qs = parse_qs(parsed.query or "")
    checkin = ""
    checkout = ""
    rooms = 1
    adults = 2
    children = 0

    def to_int(v: str, default: int) -> int:
        try:
            return int(str(v).strip())
        except Exception:
            return default

    def q(*keys: str, default: str = "") -> str:
        for k in keys:
            v = qs.get(k)
            if v and v[0]:
                return v[0]
        return default

    if active_source == "Agoda":
        checkin = q("checkIn")
        checkout = q("checkOut")
        rooms = to_int(q("rooms", default="1") or "1", 1)
        adults = to_int(q("adults", default="2") or "2", 2)
        children = to_int(q("children", default="0") or "0", 0)
    elif active_source == "Trip.com":
        checkin = q("checkIn", "checkin")
        checkout = q("checkOut", "checkout")
        rooms = to_int(q("crn", "room", default="1") or "1", 1)
        adults = to_int(q("adult", "adults", default="2") or "2", 2)
        children = to_int(q("children", default="0") or "0", 0)
    elif active_source == "Mytour.vn":
        checkin = q("checkIn")
        checkout = q("checkOut")
        rooms = to_int(q("rooms", default="1") or "1", 1)
        adults = to_int(q("adults", default="2") or "2", 2)
        children = to_int(q("children", default="0") or "0", 0)
    elif active_source == "Travel.com.vn":
        checkin = q("in")
        checkout = q("out")
        rooms = to_int(q("room", default="1") or "1", 1)
        adults = to_int(q("adults", default="2") or "2", 2)
        children = to_int(q("children", default="0") or "0", 0)
    elif active_source == "iVIVU":
        checkin = st.session_state.get("_iv_checkin", "")
        checkout = st.session_state.get("_iv_checkout", "")
        rooms = int(st.session_state.get("_iv_rooms", 1) or 1)
        adults = int(st.session_state.get("_iv_adults", 2) or 2)
        children = int(st.session_state.get("_iv_children", 0) or 0)

    checkin = _normalize_date_text(checkin)
    checkout = _normalize_date_text(checkout)
    destination = (active_destination or "").strip()
    case_key = (
        f"hotel|{_norm_destination_key(destination)}|{checkin}|{checkout}|"
        f"{rooms}|{adults}|{children}|vnd"
    )
    return {
        "case_key": case_key,
        "destination": destination,
        "checkin": checkin,
        "checkout": checkout,
        "rooms": rooms,
        "adults": adults,
        "children": children,
    }


def _extract_tour_case_info(active_source: str, active_url: str, active_destination: str) -> dict:
    parsed = urlparse(active_url or "")
    qs = parse_qs(parsed.query or "")
    dest = (active_destination or "").strip()

    def q(*keys: str, default: str = "") -> str:
        for k in keys:
            v = qs.get(k)
            if v and v[0]:
                return v[0]
        return default

    def url_fp() -> str:
        return hashlib.sha1((active_url or "").encode("utf-8", errors="ignore")).hexdigest()[:14]

    if active_source == "FindTourGo":
        country = (st.session_state.get("_tour_country") or "").strip() or q("where", "country") or dest
        ps = (st.session_state.get("_tour_period_start") or "").strip() or q(
            "tourPeriodStart", "periodStart", "tour_period_start"
        )
        pe = (st.session_state.get("_tour_period_end") or "").strip() or q(
            "tourPeriodEnd", "periodEnd", "tour_period_end"
        )
        cur = (st.session_state.get("_tour_currency") or "USD").strip() or q("currency", default="USD") or "USD"
        ps = _normalize_date_text(ps)
        pe = _normalize_date_text(pe)
        if not country and not ps and not pe:
            case_key = f"tour|FindTourGo|url|{url_fp()}"
        else:
            case_key = (
                f"tour|FindTourGo|{_norm_destination_key(country)}|{ps}|{pe}|{_norm_destination_key(cur)}"
            )
        return {
            "case_key": case_key,
            "destination": country or dest,
            "period_start": ps,
            "period_end": pe,
            "currency": cur,
        }

    if active_source == "Travel.com.vn":
        fd = _normalize_date_text(q("fromDate"))
        if not dest and not fd:
            case_key = f"tour|Travel.com.vn|url|{url_fp()}"
        else:
            case_key = f"tour|Travel.com.vn|{_norm_destination_key(dest)}|{fd}"
        return {
            "case_key": case_key,
            "destination": dest,
            "period_start": fd,
            "period_end": "",
            "currency": "",
        }

    return {
        "case_key": f"tour|{active_source}|url|{url_fp()}",
        "destination": dest,
        "period_start": "",
        "period_end": "",
        "currency": "",
    }


st.markdown("""
<style>
/* ── Global ── */
[data-testid="stAppViewContainer"] { background: #F0F4F8; }
[data-testid="stHeader"] { background: transparent; }
/* Keep toolbar visible so sidebar toggle always remains accessible. */
[data-testid="stToolbar"] { display: block; }
[data-testid="stSidebarCollapsedControl"] { display: flex !important; visibility: visible !important; }
.block-container { padding-top: 0 !important; max-width: 1200px; }

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #F7FAFC 0%, #EEF3F9 100%);
    border-right: 1px solid #E2E8F0;
}
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] h3 {
    color: #1F2937;
    font-size: 1rem;
    font-weight: 800;
    margin-bottom: .2rem;
}
[data-testid="stSidebar"] [data-testid="stRadio"] {
    background: transparent;
    border: 0;
    box-shadow: none;
    padding: 0;
}
[data-testid="stSidebar"] [data-testid="stRadio"] > div {
    flex-direction: column !important;
    gap: 8px;
}
[data-testid="stSidebar"] [data-testid="stRadio"] label {
    background: #FFFFFF;
    border: 1.5px solid #DCE5F1 !important;
    border-radius: 12px !important;
    padding: .55rem .75rem !important;
    font-weight: 700 !important;
    font-size: .9rem !important;
    box-shadow: 0 1px 6px rgba(15,23,42,.05);
}
[data-testid="stSidebar"] [data-baseweb="radio"] label:has(input:checked) {
    border-color: #4F46E5 !important;
    background: #EEF2FF;
    box-shadow: 0 2px 10px rgba(79,70,229,.22);
}
[data-testid="stSidebar"] [data-testid="stRadio"] label:hover {
    border-color: #A5B4FC !important;
}

/* ── Hero ── */
.hero {
    border-radius: 0 0 28px 28px;
    padding: 2.2rem 2rem 1.6rem;
    margin: -1rem -1rem 1.6rem -1rem;
    text-align: center;
    box-shadow: 0 4px 24px rgba(0,0,0,0.18);
}
.hero-agoda   { background: linear-gradient(135deg, #E53E1A 0%, #FF6B35 50%, #FF9A3C 100%); }
.hero-tripcom { background: linear-gradient(135deg, #007DFF 0%, #00B4D8 60%, #0096C7 100%); }
.hero-mytour  { background: linear-gradient(135deg, #059669 0%, #10B981 50%, #34D399 100%); }
.hero-travel  { background: linear-gradient(135deg, #7C3AED 0%, #A855F7 50%, #C084FC 100%); }
.hero-ivivu   { background: linear-gradient(135deg, #F59E0B 0%, #F97316 50%, #EF4444 100%); }
.hero h1 { color:#fff!important; font-size:2.2rem!important; font-weight:800!important; margin:0 0 .25rem!important; text-shadow:0 2px 8px rgba(0,0,0,.2); }
.hero p  { color:rgba(255,255,255,.9)!important; font-size:.95rem!important; margin:0!important; }

/* ── Radio OTA selector ── */
[data-testid="stRadio"] { background:#fff; border-radius:16px; padding:8px 16px; box-shadow:0 2px 10px rgba(0,0,0,.07); border:1px solid #E8ECF0; }
[data-testid="stRadio"] > div { flex-direction: row !important; gap: 12px; }
[data-testid="stRadio"] label { border-radius:12px !important; padding:.55rem 1.1rem !important; font-weight:700 !important; font-size:.92rem !important; border:1.5px solid #E2E8F0 !important; cursor:pointer !important; transition:all .2s !important; }

/* ── Tabs ── */
[data-testid="stTabs"] [role="tablist"] {
    background:#fff; border-radius:12px; padding:4px; gap:4px;
    box-shadow:0 2px 8px rgba(0,0,0,.06); border:1px solid #E8ECF0;
}
[data-testid="stTabs"] button[role="tab"] {
    border-radius:9px!important; font-weight:600!important;
    font-size:.9rem!important; padding:.5rem 1.2rem!important; color:#6B7280!important;
}
[data-testid="stTabs"] button[role="tab"][aria-selected="true"] { color:#fff!important; }

/* agoda tab active */
.ota-agoda [data-testid="stTabs"] button[role="tab"][aria-selected="true"] {
    background:linear-gradient(135deg,#E53E1A,#FF6B35)!important;
    box-shadow:0 2px 8px rgba(229,62,26,.35)!important;
}
/* tripcom tab active */
.ota-tripcom [data-testid="stTabs"] button[role="tab"][aria-selected="true"] {
    background:linear-gradient(135deg,#007DFF,#00B4D8)!important;
    box-shadow:0 2px 8px rgba(0,125,255,.35)!important;
}
/* mytour tab active */
.ota-mytour [data-testid="stTabs"] button[role="tab"][aria-selected="true"] {
    background:linear-gradient(135deg,#059669,#10B981)!important;
    box-shadow:0 2px 8px rgba(5,150,105,.35)!important;
}
/* travel tab active */
.ota-travel [data-testid="stTabs"] button[role="tab"][aria-selected="true"] {
    background:linear-gradient(135deg,#7C3AED,#A855F7)!important;
    box-shadow:0 2px 8px rgba(124,58,237,.35)!important;
}
/* ivivu tab active */
.ota-ivivu [data-testid="stTabs"] button[role="tab"][aria-selected="true"] {
    background:linear-gradient(135deg,#F59E0B,#EF4444)!important;
    box-shadow:0 2px 8px rgba(245,158,11,.35)!important;
}

[data-testid="stTabs"] [role="tabpanel"] { padding-top:0!important; }

/* ── Inputs ── */
[data-testid="stTextInput"] input, [data-testid="stDateInput"] input,
[data-testid="stNumberInput"] input, [data-testid="stTextArea"] textarea {
    border-radius:10px!important; border:1.5px solid #E2E8F0!important; background:#FAFBFC!important;
}

/* ── Primary button — agoda ── */
.ota-agoda [data-testid="stButton"] button[kind="primary"] {
    background:linear-gradient(135deg,#E53E1A,#FF6B35)!important;
    box-shadow:0 4px 14px rgba(229,62,26,.35)!important;
    border:none!important; border-radius:12px!important; font-weight:700!important;
    font-size:1rem!important; padding:.7rem 1.5rem!important;
}
/* ── Primary button — tripcom ── */
.ota-tripcom [data-testid="stButton"] button[kind="primary"] {
    background:linear-gradient(135deg,#007DFF,#00B4D8)!important;
    box-shadow:0 4px 14px rgba(0,125,255,.35)!important;
    border:none!important; border-radius:12px!important; font-weight:700!important;
    font-size:1rem!important; padding:.7rem 1.5rem!important;
}
/* ── Primary button — mytour ── */
.ota-mytour [data-testid="stButton"] button[kind="primary"] {
    background:linear-gradient(135deg,#059669,#10B981)!important;
    box-shadow:0 4px 14px rgba(5,150,105,.35)!important;
    border:none!important; border-radius:12px!important; font-weight:700!important;
    font-size:1rem!important; padding:.7rem 1.5rem!important;
}
/* ── Primary button — travel ── */
.ota-travel [data-testid="stButton"] button[kind="primary"] {
    background:linear-gradient(135deg,#7C3AED,#A855F7)!important;
    box-shadow:0 4px 14px rgba(124,58,237,.35)!important;
    border:none!important; border-radius:12px!important; font-weight:700!important;
    font-size:1rem!important; padding:.7rem 1.5rem!important;
}
/* ── Primary button — ivivu ── */
.ota-ivivu [data-testid="stButton"] button[kind="primary"] {
    background:linear-gradient(135deg,#F59E0B,#EF4444)!important;
    box-shadow:0 4px 14px rgba(245,158,11,.35)!important;
    border:none!important; border-radius:12px!important; font-weight:700!important;
    font-size:1rem!important; padding:.7rem 1.5rem!important;
}
[data-testid="stButton"] button[kind="primary"]:hover { transform:translateY(-1px)!important; }
[data-testid="stButton"] button[kind="secondary"] {
    border-radius:10px!important; border:1.5px solid #E2E8F0!important; font-weight:600!important;
}

/* ── Metrics ── */
[data-testid="stMetric"] {
    background:#fff; border-radius:14px; padding:1rem 1.2rem!important;
    box-shadow:0 2px 10px rgba(0,0,0,.06); border:1px solid #E8ECF0;
}
[data-testid="stMetricLabel"] { font-size:.78rem!important; font-weight:600!important; color:#6B7280!important; }
[data-testid="stMetricValue"] { font-size:1.9rem!important; font-weight:800!important; color:#1A1A2E!important; }

/* ── Dataframe ── */
[data-testid="stDataFrame"] {
    border-radius:14px!important; overflow:hidden;
    border:1px solid #E8ECF0!important; box-shadow:0 2px 10px rgba(0,0,0,.05);
}

/* ── Info badge ── */
.info-badge {
    display:inline-block; border-radius:8px; padding:.55rem 1rem;
    font-size:.88rem; font-weight:500; margin-bottom:1rem; width:100%;
}
.info-badge-agoda   { background:#FFF4EE; color:#E53E1A; border:1px solid #FFD5C2; }
.info-badge-tripcom { background:#EFF6FF; color:#007DFF; border:1px solid #BFDBFE; }
.info-badge-mytour  { background:#ECFDF5; color:#059669; border:1px solid #A7F3D0; }
.info-badge-travel  { background:#F5F3FF; color:#7C3AED; border:1px solid #DDD6FE; }
.info-badge-ivivu   { background:#FFF7ED; color:#C2410C; border:1px solid #FED7AA; }

/* ── Section label ── */
.section-label { font-size:.82rem; font-weight:600; color:#6B7280; text-transform:uppercase; letter-spacing:.06em; margin-bottom:.3rem; }

/* ── Result header ── */
.result-header {
    display:flex; align-items:center; gap:.7rem;
    color:#fff; border-radius:14px; padding:1rem 1.5rem; margin-bottom:1.2rem;
    box-shadow:0 2px 12px rgba(0,0,0,.2);
}
.result-header-agoda   { background:linear-gradient(135deg,#E53E1A,#b52b0f); }
.result-header-tripcom { background:linear-gradient(135deg,#007DFF,#0056b3); }
.result-header-mytour  { background:linear-gradient(135deg,#059669,#047857); }
.result-header-travel  { background:linear-gradient(135deg,#7C3AED,#5B21B6); }
.result-header-ivivu   { background:linear-gradient(135deg,#F59E0B,#C2410C); }
.result-header h3 { margin:0; font-size:1.2rem; font-weight:700; color:#fff; }
.result-badge { background:rgba(255,255,255,.25); color:#fff; border-radius:20px; padding:.2rem .8rem; font-size:.9rem; font-weight:700; margin-left:auto; }

/* ── Footer ── */
.footer { text-align:center; color:#9CA3AF; font-size:.8rem; padding:1.5rem 0 .5rem; border-top:1px solid #E8ECF0; margin-top:1rem; }

/* ── Download button ── */
.stDownloadButton button { border-radius:12px!important; font-weight:700!important; padding:.65rem 1.5rem!important; }
</style>
""", unsafe_allow_html=True)


def _invalidate_hotel_case_list_cache() -> None:
    for k in list(st.session_state.keys()):
        if isinstance(k, str) and k.startswith("_hotel_case_list_"):
            st.session_state.pop(k, None)


def _invalidate_tour_case_list_cache() -> None:
    for k in list(st.session_state.keys()):
        if isinstance(k, str) and k.startswith("_tour_case_list_"):
            st.session_state.pop(k, None)


def _cached_list_hotel_cases(limit: int, source: str | None) -> list:
    """Tránh query DB mỗi lần rerun Streamlit (cùng phiên, cùng nguồn)."""
    key = f"_hotel_case_list_{source or '__all__'}_{limit}"
    if key not in st.session_state:
        st.session_state[key] = list_hotel_cases(limit=limit, source=source)
    return st.session_state[key]


def _cached_list_tour_cases(limit: int, source: str) -> list:
    key = f"_tour_case_list_{source}_{limit}"
    if key not in st.session_state:
        st.session_state[key] = list_tour_cases(limit=limit, source=source)
    return st.session_state[key]


# ── Session state defaults ──────────────────────────────────────────────────
for key, default in [
    ("scrape_results", None), ("is_scraping", False),
    ("active_destination", ""), ("active_url", ""),
    ("active_segment", "Hotel"), ("active_source", "Agoda"),
    ("active_case_key", ""), ("compare_df", None),
    ("global_compare_df", None),
    ("trigger_scrape", False), ("trip_city_info", None),
    ("agoda_visible_browser", False),
    ("mytour_city_info", None),     ("check_in_str", ""), ("check_out_str", ""),
    ("hotel_compare_on", False),
]:
    if key not in st.session_state:
        st.session_state[key] = default

DB_CFG_OK, DB_INFO = db_ready()
# init_db() mở kết nối + DDL — chỉ chạy một lần sau khi cấu hình DB đúng (mỗi phiên Streamlit).
if not DB_CFG_OK:
    _db_init_ok, _db_init_msg = False, DB_INFO
elif st.session_state.get("_db_schema_initialized"):
    _db_init_ok = True
    _db_init_msg = st.session_state.get("_db_schema_info_msg", DB_INFO)
else:
    _db_init_ok, _db_init_msg = init_db()
    if _db_init_ok:
        st.session_state["_db_schema_initialized"] = True
        st.session_state["_db_schema_info_msg"] = _db_init_msg
DB_OK = DB_CFG_OK and _db_init_ok
if DB_CFG_OK and not _db_init_ok:
    DB_INFO = _db_init_msg

today = date.today()

if DB_CFG_OK and not _db_init_ok:
    st.warning(_db_init_msg)

# ── Segment + source selector ───────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ⚙️ Bộ chọn dữ liệu")
    st.caption("Chọn phân khúc và nguồn scrape")
    segment = st.radio(
        "Chọn phân khúc",
        ["🏨 Hotel", "🧭 Tour"],
        horizontal=False,
        key="segment_radio",
    )
    segment_name = segment.split(" ", 1)[1]
    st.markdown("---")
    hotel_selection_name = "Agoda"
    if segment_name == "Hotel":

        def _clear_hotel_compare():
            st.session_state.hotel_compare_on = False

        hotel_ota_pick = st.radio(
            "Chọn OTA / Công cụ",
            ["🟠 Agoda", "🔵 Trip.com", "🟢 Mytour.vn", "🟣 Travel.com.vn", "🟡 iVIVU"],
            horizontal=False,
            key="hotel_ota_radio",
            on_change=_clear_hotel_compare,
        )
        st.divider()
        if st.button(
            "🧮 So sánh đa kênh",
            use_container_width=True,
            type="primary" if st.session_state.hotel_compare_on else "secondary",
        ):
            st.session_state.hotel_compare_on = True

        if st.session_state.hotel_compare_on:
            hotel_selection_name = "So sánh đa kênh"
        else:
            hotel_selection_name = hotel_ota_pick.split(" ", 1)[1]
        ota_name = hotel_selection_name
    else:
        tour_source = st.radio(
            "Chọn nguồn Tour",
            ["🧭 FindTourGo", "🟣 Travel.com.vn"],
            horizontal=False,
            key="tour_source_radio",
        )
        ota = tour_source
        ota_name = tour_source.split(" ", 1)[1]

compare_tool_mode = segment_name == "Hotel" and hotel_selection_name == "So sánh đa kênh"

# Clear results if selector changed
selector_key = f"{segment_name}:{ota_name}"
if "prev_selector" not in st.session_state:
    st.session_state.prev_selector = selector_key
if st.session_state.prev_selector != selector_key:
    st.session_state.scrape_results = None
    st.session_state.compare_df = None
    st.session_state.global_compare_df = None
    st.session_state.prev_selector = selector_key

hero_class = {
    "Agoda": "hero-agoda",
    "Trip.com": "hero-tripcom",
    "Mytour.vn": "hero-mytour",
    "Travel.com.vn": "hero-travel",
    "iVIVU": "hero-ivivu",
    "FindTourGo": "hero-tripcom",
    "So sánh đa kênh": "hero-tripcom",
}[ota_name]
ota_class = {
    "Agoda": "ota-agoda",
    "Trip.com": "ota-tripcom",
    "Mytour.vn": "ota-mytour",
    "Travel.com.vn": "ota-travel",
    "iVIVU": "ota-ivivu",
    "FindTourGo": "ota-tripcom",
    "So sánh đa kênh": "ota-tripcom",
}[ota_name]

logo_map = {
    "Agoda": "🟠",
    "Trip.com": "🔵",
    "Mytour.vn": "🟢",
    "Travel.com.vn": "🟣",
    "iVIVU": "🟡",
    "FindTourGo": "🧭",
    "So sánh đa kênh": "🧮",
}
title_suffix = "Hotel Scraper" if segment_name == "Hotel" else "Tour Scraper"
subtitle = (
    "Thu thập dữ liệu khách sạn · Phân tích giá · Xuất Excel / CSV"
    if segment_name == "Hotel"
    else "Thu thập dữ liệu tour · Lấy ngày khởi hành · Xuất Excel / CSV"
)
if compare_tool_mode:
    title_suffix = ""
    subtitle = "Chọn case trong DB để so sánh giá giữa các OTA · Tải Excel / CSV"
hero_title = f"{logo_map[ota_name]} {ota_name} {title_suffix}".strip()
st.markdown(f"""
<div class="hero {hero_class}">
  <h1>{hero_title}</h1>
  <p>{subtitle}</p>
</div>
""", unsafe_allow_html=True)

st.markdown(f"<div class='{ota_class}'>", unsafe_allow_html=True)

# ── TOUR SOURCES ────────────────────────────────────────────────────────────
if compare_tool_mode:
    pass
elif segment_name == "Tour":
    if ota_name == "FindTourGo":
        tab_f1, tab_f2 = st.tabs(["📋  Nhập cấu hình", "🔗  Dán URL trực tiếp"])

        with tab_f1:
            st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
            st.markdown("""<div class="info-badge info-badge-tripcom">
              💡 Ví dụ country code: <b>CN</b>, <b>VN</b>, <b>JP</b>. Kết quả có kèm cột <b>Ngày khởi hành</b>.
            </div>""", unsafe_allow_html=True)
            fc1, fc2 = st.columns(2, gap="medium")
            with fc1:
                st.markdown("<div class='section-label'>🌍 Country code (where)</div>", unsafe_allow_html=True)
                tour_country = st.text_input("Country", value="CN", key="tour_country", label_visibility="collapsed")
                st.caption("Nhập mã 2 ký tự (CN, VN, JP...) hoặc tên nước (Trung Quốc, China...)")
            with fc2:
                st.markdown("<div class='section-label'>💱 Tiền tệ</div>", unsafe_allow_html=True)
                tour_currency = st.selectbox("Currency", ["USD", "VND", "EUR", "JPY", "SGD"], index=0, key="tour_currency", label_visibility="collapsed")

            fc3, fc4 = st.columns(2, gap="medium")
            with fc3:
                st.markdown("<div class='section-label'>📅 Từ ngày</div>", unsafe_allow_html=True)
                tour_start = st.date_input("Tour start", value=today + timedelta(days=7), min_value=today, key="tour_start", label_visibility="collapsed")
            with fc4:
                st.markdown("<div class='section-label'>📅 Đến ngày</div>", unsafe_allow_html=True)
                tour_end = st.date_input("Tour end", value=today + timedelta(days=37), min_value=today + timedelta(days=1), key="tour_end", label_visibility="collapsed")

            tour_btn_disabled = tour_start > tour_end
            if tour_btn_disabled:
                st.error("⚠️ Ngày kết thúc phải sau hoặc bằng ngày bắt đầu.")

            tour_url = build_findtourgo_url(
                country_code=tour_country.strip(),
                tour_period_start=tour_start.strftime("%Y-%m-%d"),
                tour_period_end=tour_end.strftime("%Y-%m-%d"),
                currency=tour_currency,
                locale="vi",
            )
            with st.expander("👁️ Xem URL sẽ được scrape"):
                st.code(tour_url, language="text")

            st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
            if st.button(
                "🚀  Bắt đầu thu thập",
                disabled=tour_btn_disabled or not tour_country.strip() or st.session_state.is_scraping,
                key="tour_form_btn", use_container_width=True, type="primary",
            ):
                st.session_state.update({
                    "active_url": tour_url,
                    "active_destination": tour_country.strip().upper(),
                    "active_segment": "Tour",
                    "active_source": "FindTourGo",
                    "_tour_country": tour_country.strip().upper(),
                    "_tour_period_start": tour_start.strftime("%Y-%m-%d"),
                    "_tour_period_end": tour_end.strftime("%Y-%m-%d"),
                    "_tour_currency": tour_currency,
                    "trigger_scrape": True,
                })

        with tab_f2:
            st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
            st.markdown("<div class='section-label'>🔗 URL FindTourGo</div>", unsafe_allow_html=True)
            tour_direct_url = st.text_area(
                "URL",
                placeholder="https://findtourgo.com/vi/country/china?tourPeriodEnd=2026-04-30&tourPeriodStart=2026-04-01&where=CN&currency=USD",
                height=90,
                key="tour_url_direct",
                label_visibility="collapsed",
            )
            st.markdown("<div class='section-label'>🌍 Country code (tuỳ chọn)</div>", unsafe_allow_html=True)
            tour_direct_country = st.text_input(
                "Country code override",
                value="",
                placeholder="VD: CN",
                key="tour_country_direct",
                label_visibility="collapsed",
            )
            st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
            if st.button(
                "🚀  Bắt đầu thu thập",
                disabled=not tour_direct_url.strip() or st.session_state.is_scraping,
                key="tour_url_btn", use_container_width=True, type="primary",
            ):
                st.session_state.update({
                    "active_url": tour_direct_url.strip(),
                    "active_destination": (tour_direct_country.strip() or "TOUR").upper(),
                    "active_segment": "Tour",
                    "active_source": "FindTourGo",
                    "_tour_country": tour_direct_country.strip().upper(),
                    "_tour_period_start": "",
                    "_tour_period_end": "",
                    "_tour_currency": "",
                    "trigger_scrape": True,
                })
    else:  # Travel.com.vn tour
        tab_t1, tab_t2 = st.tabs(["📋  Nhập cấu hình", "🔗  Dán URL trực tiếp"])

        with tab_t1:
            st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
            st.markdown("""<div class="info-badge info-badge-travel">
              💡 Tour Travel.com.vn sẽ lấy: tên tour, mã tour, khởi hành, thời gian, giá và <b>ngày khởi hành</b>.
            </div>""", unsafe_allow_html=True)
            tc1, tc2 = st.columns(2, gap="medium")
            with tc1:
                st.markdown("<div class='section-label'>🌍 Điểm đến tour</div>", unsafe_allow_html=True)
                tv_tour_dest = st.text_input("Điểm đến tour", value="Trung Quốc", key="tv_tour_dest", label_visibility="collapsed")
            with tc2:
                st.markdown("<div class='section-label'>📅 Từ ngày</div>", unsafe_allow_html=True)
                tv_tour_from = st.date_input("fromDate", value=today + timedelta(days=1), min_value=today, key="tv_tour_from", label_visibility="collapsed")

            tv_slug = resolve_travel_tour_slug(tv_tour_dest.strip())
            tv_tour_url = build_travel_tour_url(tv_slug, tv_tour_from.strftime("%Y-%m-%d"))
            with st.expander("👁️ Xem URL sẽ được scrape"):
                st.code(tv_tour_url, language="text")

            st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
            if st.button(
                "🚀  Bắt đầu thu thập",
                disabled=not tv_tour_dest.strip() or st.session_state.is_scraping,
                key="travel_tour_form_btn", use_container_width=True, type="primary",
            ):
                st.session_state.update({
                    "active_url": tv_tour_url,
                    "active_destination": tv_tour_dest.strip(),
                    "active_segment": "Tour",
                    "active_source": "Travel.com.vn",
                    "trigger_scrape": True,
                })

        with tab_t2:
            st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
            st.markdown("<div class='section-label'>🔗 URL Travel.com.vn tour</div>", unsafe_allow_html=True)
            travel_tour_direct_url = st.text_area(
                "URL",
                placeholder="https://travel.com.vn/du-lich-trung-quoc.aspx?fromDate=2026-03-30",
                height=90,
                key="travel_tour_direct_url",
                label_visibility="collapsed",
            )
            st.markdown("<div class='section-label'>📍 Nhãn điểm đến</div>", unsafe_allow_html=True)
            travel_tour_dest = st.text_input(
                "Điểm đến", placeholder="VD: Trung Quốc",
                key="travel_tour_dest", label_visibility="collapsed",
            )
            st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
            if st.button(
                "🚀  Bắt đầu thu thập",
                disabled=not travel_tour_direct_url.strip() or st.session_state.is_scraping,
                key="travel_tour_url_btn", use_container_width=True, type="primary",
            ):
                st.session_state.update({
                    "active_url": travel_tour_direct_url.strip(),
                    "active_destination": travel_tour_dest.strip() or "Travel Tour",
                    "active_segment": "Tour",
                    "active_source": "Travel.com.vn",
                    "trigger_scrape": True,
                })

# ── AGODA ────────────────────────────────────────────────────────────────────
elif ota_name == "Agoda":
    st.checkbox(
        "Hiện cửa sổ trình duyệt khi scrape (khuyến nghị nếu giá không khớp giá đỏ trên Agoda)",
        key="agoda_visible_browser",
        help="Chrome không headless. Nhiều phiên bản Agoda chỉ hiển thị đúng giá sau coupon khi có cửa sổ thật.",
    )
    tab1, tab2 = st.tabs(["📋  Nhập cấu hình", "🔗  Dán URL trực tiếp"])

    with tab1:
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
        st.markdown("<div class='section-label'>📍 Điểm đến</div>", unsafe_allow_html=True)
        destination_form = st.text_input("Điểm đến", placeholder="VD: Hà Nội, Đà Nẵng, Hội An, Phú Quốc...",
                                         key="agoda_dest", label_visibility="collapsed")
        col1, col2 = st.columns(2, gap="medium")
        with col1:
            st.markdown("<div class='section-label'>📅 Check-in</div>", unsafe_allow_html=True)
            checkin_date = st.date_input("Check-in", value=today + timedelta(days=7),
                                         min_value=today, key="agoda_checkin", label_visibility="collapsed")
        with col2:
            st.markdown("<div class='section-label'>📅 Check-out</div>", unsafe_allow_html=True)
            checkout_date = st.date_input("Check-out", value=today + timedelta(days=8),
                                          min_value=today + timedelta(days=1), key="agoda_checkout", label_visibility="collapsed")
        col3, col4, col5 = st.columns(3, gap="medium")
        with col3:
            st.markdown("<div class='section-label'>🛏️ Số phòng</div>", unsafe_allow_html=True)
            num_rooms = st.number_input("Phòng", min_value=1, max_value=10, value=1, key="agoda_rooms", label_visibility="collapsed")
        with col4:
            st.markdown("<div class='section-label'>👤 Người lớn</div>", unsafe_allow_html=True)
            num_adults = st.number_input("Người lớn", min_value=1, max_value=20, value=2, key="agoda_adults", label_visibility="collapsed")
        with col5:
            st.markdown("<div class='section-label'>👶 Trẻ em</div>", unsafe_allow_html=True)
            num_children = st.number_input("Trẻ em", min_value=0, max_value=10, value=0, key="agoda_children", label_visibility="collapsed")

        child_ages = []
        if num_children > 0:
            st.markdown("<div class='section-label'>🎂 Độ tuổi trẻ em</div>", unsafe_allow_html=True)
            age_cols = st.columns(min(num_children, 5))
            for i in range(num_children):
                with age_cols[i % 5]:
                    child_ages.append(st.number_input(f"Trẻ {i+1}", 0, 17, 5, key=f"agoda_age_{i}"))

        btn_disabled = checkin_date >= checkout_date
        if btn_disabled:
            st.error("⚠️ Check-out phải sau Check-in!")

        url_preview = ""
        if destination_form and not btn_disabled:
            url_preview = build_agoda_url(destination=destination_form,
                                          check_in=checkin_date.strftime("%Y-%m-%d"),
                                          check_out=checkout_date.strftime("%Y-%m-%d"),
                                          rooms=num_rooms, adults=num_adults,
                                          children=num_children, child_ages=child_ages)
            with st.expander("👁️ Xem URL sẽ được scrape"):
                st.code(url_preview, language="text")

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        if st.button("🚀  Bắt đầu thu thập", disabled=btn_disabled or not destination_form or st.session_state.is_scraping,
                     key="agoda_form_btn", use_container_width=True, type="primary"):
            if url_preview:
                st.session_state.update({
                    "active_url": url_preview,
                    "active_destination": destination_form,
                    "active_segment": "Hotel",
                    "active_source": ota_name,
                    "trigger_scrape": True,
                })

    with tab2:
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
        st.markdown("""<div class="info-badge info-badge-agoda">
          💡 Truy cập Agoda, tìm kiếm khách sạn theo ý muốn, copy URL và dán vào đây.
        </div>""", unsafe_allow_html=True)
        st.markdown("<div class='section-label'>🔗 URL tìm kiếm Agoda</div>", unsafe_allow_html=True)
        direct_url = st.text_area("URL", placeholder="https://www.agoda.com/search?city=...",
                                   height=90, key="agoda_url", label_visibility="collapsed")
        st.markdown("<div class='section-label'>🗺️ Tên điểm đến</div>", unsafe_allow_html=True)
        dest_url_tab = st.text_input("Điểm đến", placeholder="VD: Đà Nẵng", key="agoda_dest_url", label_visibility="collapsed")
        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        if st.button("🚀  Bắt đầu thu thập",
                     disabled=not direct_url.strip() or not dest_url_tab.strip() or st.session_state.is_scraping,
                     key="agoda_url_btn", use_container_width=True, type="primary"):
            pasted = normalize_agoda_direct_url(direct_url)
            st.session_state.update({
                "active_url": pasted,
                "active_destination": dest_url_tab.strip(),
                "active_segment": "Hotel",
                "active_source": ota_name,
                "trigger_scrape": True,
            })

# ── TRIP.COM ─────────────────────────────────────────────────────────────────
elif ota_name == "Trip.com":
    tab_t1, tab_t2 = st.tabs(["📋  Nhập cấu hình", "🔗  Dán URL trực tiếp"])

    with tab_t1:
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
        st.markdown("""<div class="info-badge info-badge-tripcom">
          🌐 Hỗ trợ: Hà Nội · TP. HCM · Đà Nẵng · Phú Quốc · Nha Trang · Đà Lạt · Hội An · Hạ Long · Huế · Vũng Tàu và nhiều nơi khác.
        </div>""", unsafe_allow_html=True)

        st.markdown("<div class='section-label'>📍 Điểm đến</div>", unsafe_allow_html=True)
        trip_dest = st.text_input("Điểm đến", placeholder="VD: Hà Nội, Đà Nẵng, Phú Quốc...",
                                  key="trip_dest", label_visibility="collapsed")

        if trip_dest.strip():
            city_info = resolve_trip_city(trip_dest.strip())
            if city_info:
                st.caption(f"✅ Tìm thấy City ID = {city_info[0]}")
            else:
                st.warning(f"⚠️ Chưa hỗ trợ '{trip_dest}'. Thêm thành phố trong tương lai.")

        col_t1, col_t2 = st.columns(2, gap="medium")
        with col_t1:
            st.markdown("<div class='section-label'>📅 Check-in</div>", unsafe_allow_html=True)
            trip_checkin = st.date_input("Check-in", value=today + timedelta(days=7),
                                         min_value=today, key="trip_checkin", label_visibility="collapsed")
        with col_t2:
            st.markdown("<div class='section-label'>📅 Check-out</div>", unsafe_allow_html=True)
            trip_checkout = st.date_input("Check-out", value=today + timedelta(days=8),
                                          min_value=today + timedelta(days=1), key="trip_checkout", label_visibility="collapsed")

        col_t3, col_t4, col_t5 = st.columns(3, gap="medium")
        with col_t3:
            st.markdown("<div class='section-label'>🛏️ Số phòng</div>", unsafe_allow_html=True)
            trip_rooms = st.number_input("Phòng", 1, 10, 1, key="trip_rooms", label_visibility="collapsed")
        with col_t4:
            st.markdown("<div class='section-label'>👤 Người lớn</div>", unsafe_allow_html=True)
            trip_adults = st.number_input("Người lớn", 1, 20, 2, key="trip_adults", label_visibility="collapsed")
        with col_t5:
            st.markdown("<div class='section-label'>👶 Trẻ em</div>", unsafe_allow_html=True)
            trip_children = st.number_input("Trẻ em", 0, 10, 0, key="trip_children", label_visibility="collapsed")

        if trip_checkin >= trip_checkout:
            st.error("⚠️ Check-out phải sau Check-in!")
            trip_btn_disabled = True
        else:
            trip_btn_disabled = False

        trip_city_info = resolve_trip_city(trip_dest.strip()) if trip_dest.strip() else None

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        if st.button("🚀  Bắt đầu thu thập",
                     disabled=trip_btn_disabled or not trip_dest.strip() or not trip_city_info or st.session_state.is_scraping,
                     key="trip_btn", use_container_width=True, type="primary"):
            city_id, country_id = trip_city_info
            url = build_tripcom_url(city_id=city_id,
                                    check_in=trip_checkin.strftime("%Y-%m-%d"),
                                    check_out=trip_checkout.strftime("%Y-%m-%d"),
                                    rooms=trip_rooms, adults=trip_adults, children=trip_children)
            st.session_state.update({"active_url": url, "active_destination": trip_dest.strip(), "trigger_scrape": True,
                                      "trip_city_info": trip_city_info, "active_segment": "Hotel", "active_source": ota_name})

    with tab_t2:
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
        st.markdown("""<div class="info-badge info-badge-tripcom">
          💡 Truy cập <b>vn.trip.com/hotels</b>, tìm kiếm khách sạn theo khu vực/tỉnh bất kỳ, copy URL từ thanh địa chỉ và dán vào đây.
        </div>""", unsafe_allow_html=True)
        st.markdown("<div class='section-label'>🔗 URL tìm kiếm Trip.com</div>", unsafe_allow_html=True)
        trip_direct_url = st.text_area("URL", placeholder="https://vn.trip.com/hotels/list?city=286&checkIn=2026-04-22...",
                                        height=90, key="trip_url", label_visibility="collapsed")
        st.markdown("<div class='section-label'>🗺️ Tên điểm đến</div>", unsafe_allow_html=True)
        trip_dest_url = st.text_input("Điểm đến", placeholder="VD: An Giang, Ninh Bình...",
                                       key="trip_dest_url", label_visibility="collapsed")
        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        if st.button("🚀  Bắt đầu thu thập",
                     disabled=not trip_direct_url.strip() or not trip_dest_url.strip() or st.session_state.is_scraping,
                     key="trip_url_btn", use_container_width=True, type="primary"):
            st.session_state.update({
                "active_url": trip_direct_url.strip(),
                "active_destination": trip_dest_url.strip(),
                "active_segment": "Hotel",
                "active_source": ota_name,
                "trigger_scrape": True,
            })

# ── MYTOUR.VN ────────────────────────────────────────────────────────────────
elif ota_name == "Mytour.vn":
    tab_m1, tab_m2 = st.tabs(["📋  Nhập cấu hình", "🔗  Dán URL trực tiếp"])

    with tab_m1:
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
        st.markdown("""<div class="info-badge info-badge-mytour">
          🌐 Hỗ trợ: Hà Nội · TP. HCM · Đà Nẵng · Phú Quốc · Nha Trang · Đà Lạt · Hội An · Hạ Long · Huế · Vũng Tàu · Sa Pa và nhiều nơi khác.
        </div>""", unsafe_allow_html=True)

        st.markdown("<div class='section-label'>📍 Điểm đến</div>", unsafe_allow_html=True)
        mytour_dest = st.text_input("Điểm đến", placeholder="VD: Hà Nội, Đà Nẵng, Phú Quốc...",
                                    key="mytour_dest", label_visibility="collapsed")

        if mytour_dest.strip():
            city_info_mt = resolve_mytour_city(mytour_dest.strip())
            if city_info_mt:
                st.caption(f"✅ Tìm thấy điểm đến: {city_info_mt[1]}")
            else:
                st.warning(f"⚠️ Chưa hỗ trợ '{mytour_dest}'. Thêm điểm đến trong tương lai.")

        col_m1, col_m2 = st.columns(2, gap="medium")
        with col_m1:
            st.markdown("<div class='section-label'>📅 Check-in</div>", unsafe_allow_html=True)
            mytour_checkin = st.date_input("Check-in", value=today + timedelta(days=7),
                                           min_value=today, key="mytour_checkin", label_visibility="collapsed")
        with col_m2:
            st.markdown("<div class='section-label'>📅 Check-out</div>", unsafe_allow_html=True)
            mytour_checkout = st.date_input("Check-out", value=today + timedelta(days=8),
                                            min_value=today + timedelta(days=1), key="mytour_checkout", label_visibility="collapsed")

        col_m3, col_m4, col_m5 = st.columns(3, gap="medium")
        with col_m3:
            st.markdown("<div class='section-label'>🛏️ Số phòng</div>", unsafe_allow_html=True)
            mytour_rooms = st.number_input("Phòng", 1, 10, 1, key="mytour_rooms", label_visibility="collapsed")
        with col_m4:
            st.markdown("<div class='section-label'>👤 Người lớn</div>", unsafe_allow_html=True)
            mytour_adults = st.number_input("Người lớn", 1, 20, 2, key="mytour_adults", label_visibility="collapsed")
        with col_m5:
            st.markdown("<div class='section-label'>👶 Trẻ em</div>", unsafe_allow_html=True)
            mytour_children = st.number_input("Trẻ em", 0, 10, 0, key="mytour_children", label_visibility="collapsed")

        if mytour_checkin >= mytour_checkout:
            st.error("⚠️ Check-out phải sau Check-in!")
            mytour_btn_disabled = True
        else:
            mytour_btn_disabled = False

        mytour_city_info = resolve_mytour_city(mytour_dest.strip()) if mytour_dest.strip() else None

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        if st.button("🚀  Bắt đầu thu thập",
                     disabled=mytour_btn_disabled or not mytour_dest.strip() or not mytour_city_info or st.session_state.is_scraping,
                     key="mytour_btn", use_container_width=True, type="primary"):
            province_id_val, display_name = mytour_city_info
            ci_str = mytour_checkin.strftime("%d-%m-%Y")
            co_str = mytour_checkout.strftime("%d-%m-%Y")
            slug = mytour_dest.strip().lower()
            slug = slug.replace("ồ", "o").replace("ội", "oi").replace("à", "a").replace("ẵng", "ang")
            slug = re.sub(r"[^a-z0-9]+", "-", slug).strip("-")
            url = build_mytour_url(city_slug=slug, check_in=ci_str, check_out=co_str,
                                   rooms=mytour_rooms, adults=mytour_adults, children=mytour_children)
            st.session_state.update({
                "active_url": url,
                "active_destination": mytour_dest.strip(),
                "active_segment": "Hotel",
                "active_source": ota_name,
                "trigger_scrape": True,
                "mytour_province_id": province_id_val,
                "mytour_city_slug": slug,
                "mytour_paste_mode": False,
                "check_in_str": ci_str,
                "check_out_str": co_str,
                "_mt_rooms": mytour_rooms,
                "_mt_adults": mytour_adults,
                "_mt_children": mytour_children,
            })

    with tab_m2:
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
        st.markdown("""<div class="info-badge info-badge-mytour">
          💡 Truy cập <b>mytour.vn/khach-san</b>, tìm kiếm theo khu vực/tỉnh/thành bất kỳ, copy URL từ thanh địa chỉ và dán vào đây.
        </div>""", unsafe_allow_html=True)
        st.markdown("<div class='section-label'>🔗 URL tìm kiếm Mytour.vn</div>", unsafe_allow_html=True)
        mt_direct_url = st.text_area("URL", placeholder="https://mytour.vn/khach-san/search?aliasCode=tp3&checkIn=22-04-2026...",
                                      height=90, key="mt_url", label_visibility="collapsed")
        st.markdown("<div class='section-label'>🗺️ Tên điểm đến</div>", unsafe_allow_html=True)
        mt_dest_url = st.text_input("Điểm đến", placeholder="VD: An Giang, Ninh Bình...",
                                     key="mt_dest_url", label_visibility="collapsed")

        # Parse dates from pasted URL for display
        mt_ci_parsed, mt_co_parsed = "", ""
        if mt_direct_url.strip():
            from urllib.parse import urlparse, parse_qs
            try:
                parsed = urlparse(mt_direct_url.strip())
                params = parse_qs(parsed.query)
                mt_ci_parsed = params.get("checkIn", [""])[0]
                mt_co_parsed = params.get("checkOut", [""])[0]
                if mt_ci_parsed and mt_co_parsed:
                    st.caption(f"📅 Ngày phát hiện từ URL: check-in **{mt_ci_parsed}** → check-out **{mt_co_parsed}**")
            except Exception:
                pass

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        if st.button("🚀  Bắt đầu thu thập",
                     disabled=not mt_direct_url.strip() or not mt_dest_url.strip() or st.session_state.is_scraping,
                     key="mt_url_btn", use_container_width=True, type="primary"):
            st.session_state.update({
                "active_url": mt_direct_url.strip(),
                "active_destination": mt_dest_url.strip(),
                "active_segment": "Hotel",
                "active_source": ota_name,
                "trigger_scrape": True,
                "mytour_province_id": None,
                "mytour_city_slug": "",
                "mytour_paste_mode": True,
                "check_in_str": mt_ci_parsed,
                "check_out_str": mt_co_parsed,
                "_mt_rooms": 1,
                "_mt_adults": 2,
                "_mt_children": 0,
            })

# ── TRAVEL.COM.VN ────────────────────────────────────────────────────────────
elif ota_name == "Travel.com.vn":
    tab_tv1, tab_tv2 = st.tabs(["📋  Nhập cấu hình", "🔗  Dán URL trực tiếp"])

    with tab_tv1:
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
        st.markdown("""<div class="info-badge info-badge-travel">
          🌐 Hỗ trợ: Hà Nội · TP. HCM · Đà Nẵng · Nha Trang · Phú Quốc · Đà Lạt · Hội An · Vũng Tàu · Hạ Long · Huế · Châu Đốc và nhiều nơi khác.
        </div>""", unsafe_allow_html=True)

        st.markdown("<div class='section-label'>📍 Điểm đến</div>", unsafe_allow_html=True)
        tv_dest = st.text_input("Điểm đến", placeholder="VD: Hà Nội, Đà Nẵng, Châu Đốc...",
                                key="tv_dest", label_visibility="collapsed")

        tv_city_info = None
        if tv_dest.strip():
            tv_city_info = resolve_travel_city(tv_dest.strip())
            if tv_city_info:
                st.caption(f"✅ Tìm thấy: {tv_city_info[2]} (ID={tv_city_info[1]})")
            else:
                st.warning(f"⚠️ Chưa hỗ trợ '{tv_dest}'. Hãy dùng tab 'Dán URL trực tiếp'.")

        col_tv1, col_tv2 = st.columns(2, gap="medium")
        with col_tv1:
            st.markdown("<div class='section-label'>📅 Check-in</div>", unsafe_allow_html=True)
            tv_checkin = st.date_input("Check-in", value=today + timedelta(days=7),
                                       min_value=today, key="tv_checkin", label_visibility="collapsed")
        with col_tv2:
            st.markdown("<div class='section-label'>📅 Check-out</div>", unsafe_allow_html=True)
            tv_checkout = st.date_input("Check-out", value=today + timedelta(days=8),
                                        min_value=today + timedelta(days=1), key="tv_checkout", label_visibility="collapsed")

        col_tv3, col_tv4, col_tv5 = st.columns(3, gap="medium")
        with col_tv3:
            st.markdown("<div class='section-label'>🛏️ Số phòng</div>", unsafe_allow_html=True)
            tv_rooms = st.number_input("Phòng", 1, 10, 1, key="tv_rooms", label_visibility="collapsed")
        with col_tv4:
            st.markdown("<div class='section-label'>👤 Người lớn</div>", unsafe_allow_html=True)
            tv_adults = st.number_input("Người lớn", 1, 20, 2, key="tv_adults", label_visibility="collapsed")
        with col_tv5:
            st.markdown("<div class='section-label'>👶 Trẻ em</div>", unsafe_allow_html=True)
            tv_children = st.number_input("Trẻ em", 0, 10, 0, key="tv_children", label_visibility="collapsed")

        tv_btn_disabled = (tv_checkin >= tv_checkout) or not tv_dest.strip() or not tv_city_info
        if tv_checkin >= tv_checkout:
            st.error("⚠️ Check-out phải sau Check-in!")

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        if st.button("🚀  Bắt đầu thu thập",
                     disabled=tv_btn_disabled or st.session_state.is_scraping,
                     key="tv_btn", use_container_width=True, type="primary"):
            slug, cid, cname = tv_city_info
            ci_str = tv_checkin.strftime("%d-%m-%Y")
            co_str = tv_checkout.strftime("%d-%m-%Y")
            tv_url = build_travel_url(city_slug=slug, city_id=cid, city_name=cname,
                                      check_in=ci_str, check_out=co_str,
                                      rooms=tv_rooms, adults=tv_adults, children=tv_children)
            st.session_state.update({
                "active_url": tv_url,
                "active_destination": tv_dest.strip(),
                "active_segment": "Hotel",
                "active_source": ota_name,
                "trigger_scrape": True,
            })

    with tab_tv2:
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
        st.markdown("""<div class="info-badge info-badge-travel">
          💡 Truy cập <b>travel.com.vn/hotels</b>, tìm kiếm khách sạn theo khu vực bất kỳ, copy URL từ thanh địa chỉ và dán vào đây.
        </div>""", unsafe_allow_html=True)
        st.markdown("<div class='section-label'>🔗 URL tìm kiếm Travel.com.vn</div>", unsafe_allow_html=True)
        tv_direct_url = st.text_area("URL", placeholder="https://travel.com.vn/hotels/khach-san-tai-...aspx?room=1&in=22-04-2026...",
                                      height=90, key="tv_url", label_visibility="collapsed")
        st.markdown("<div class='section-label'>🗺️ Tên điểm đến</div>", unsafe_allow_html=True)
        tv_dest_url = st.text_input("Điểm đến", placeholder="VD: Châu Đốc, An Giang...",
                                     key="tv_dest_url", label_visibility="collapsed")
        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        if st.button("🚀  Bắt đầu thu thập",
                     disabled=not tv_direct_url.strip() or not tv_dest_url.strip() or st.session_state.is_scraping,
                     key="tv_url_btn", use_container_width=True, type="primary"):
            st.session_state.update({
                "active_url": tv_direct_url.strip(),
                "active_destination": tv_dest_url.strip(),
                "active_segment": "Hotel",
                "active_source": ota_name,
                "trigger_scrape": True,
            })

# ── IVIVU ────────────────────────────────────────────────────────────────────
elif ota_name == "iVIVU":
    tab_i1, tab_i2 = st.tabs(["📋  Nhập cấu hình", "🔗  Dán URL trực tiếp"])

    with tab_i1:
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
        st.markdown("""<div class="info-badge info-badge-ivivu">
          🌐 Hỗ trợ tìm kiếm theo điểm đến + ngày + số khách, tương tự các tab OTA khác.
        </div>""", unsafe_allow_html=True)
        st.markdown("<div class='section-label'>📍 Điểm đến</div>", unsafe_allow_html=True)
        iv_dest_form = st.text_input(
            "Điểm đến", placeholder="VD: Nha Trang, Đà Lạt...",
            key="ivivu_dest_form", label_visibility="collapsed"
        )

        iv_suggest_url = ""
        if iv_dest_form.strip():
            iv_suggest_url = resolve_ivivu_region_url(iv_dest_form.strip()) or ""
            if iv_suggest_url:
                st.caption(f"✅ URL iVIVU: {iv_suggest_url}")
            else:
                st.warning("⚠️ Không tìm thấy điểm đến trên iVIVU. Hãy thử tên khác hoặc dùng tab dán URL.")

        col_i1, col_i2 = st.columns(2, gap="medium")
        with col_i1:
            st.markdown("<div class='section-label'>📅 Check-in</div>", unsafe_allow_html=True)
            iv_checkin = st.date_input(
                "Check-in", value=today + timedelta(days=7),
                min_value=today, key="ivivu_checkin", label_visibility="collapsed"
            )
        with col_i2:
            st.markdown("<div class='section-label'>📅 Check-out</div>", unsafe_allow_html=True)
            iv_checkout = st.date_input(
                "Check-out", value=today + timedelta(days=8),
                min_value=today + timedelta(days=1), key="ivivu_checkout", label_visibility="collapsed"
            )

        col_i3, col_i4, col_i5 = st.columns(3, gap="medium")
        with col_i3:
            st.markdown("<div class='section-label'>🛏️ Số phòng</div>", unsafe_allow_html=True)
            iv_rooms = st.number_input("Phòng", 1, 10, 1, key="ivivu_rooms", label_visibility="collapsed")
        with col_i4:
            st.markdown("<div class='section-label'>👤 Người lớn</div>", unsafe_allow_html=True)
            iv_adults = st.number_input("Người lớn", 1, 20, 2, key="ivivu_adults", label_visibility="collapsed")
        with col_i5:
            st.markdown("<div class='section-label'>👶 Trẻ em</div>", unsafe_allow_html=True)
            iv_children = st.number_input("Trẻ em", 0, 10, 0, key="ivivu_children", label_visibility="collapsed")

        if iv_checkin >= iv_checkout:
            st.error("⚠️ Check-out phải sau Check-in!")
            iv_form_disabled = True
        else:
            iv_form_disabled = False

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        if st.button(
            "🚀  Bắt đầu thu thập",
            disabled=iv_form_disabled or not iv_dest_form.strip() or not iv_suggest_url or st.session_state.is_scraping,
            key="ivivu_form_btn", use_container_width=True, type="primary"
        ):
            st.session_state.update({
                "active_url": iv_suggest_url,
                "active_destination": iv_dest_form.strip(),
                "active_segment": "Hotel",
                "active_source": ota_name,
                "_iv_checkin": iv_checkin.strftime("%Y-%m-%d"),
                "_iv_checkout": iv_checkout.strftime("%Y-%m-%d"),
                "_iv_rooms": int(iv_rooms),
                "_iv_adults": int(iv_adults),
                "_iv_children": int(iv_children),
                "trigger_scrape": True,
            })

    with tab_i2:
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
        st.markdown("""<div class="info-badge info-badge-ivivu">
          💡 Dùng khi bạn muốn dán trực tiếp URL đã lọc sẵn trên iVIVU.
        </div>""", unsafe_allow_html=True)
        st.markdown("<div class='section-label'>🔗 URL iVIVU</div>", unsafe_allow_html=True)
        iv_url = st.text_area(
            "URL", placeholder="https://www.ivivu.com/khach-san-nha-trang",
            height=90, key="ivivu_url", label_visibility="collapsed"
        )
        st.markdown("<div class='section-label'>🗺️ Tên điểm đến</div>", unsafe_allow_html=True)
        iv_dest = st.text_input(
            "Điểm đến", placeholder="VD: Nha Trang, Đà Lạt...",
            key="ivivu_dest", label_visibility="collapsed"
        )
        col_d1, col_d2 = st.columns(2, gap="medium")
        with col_d1:
            iv_checkin_d = st.date_input(
                "Check-in", value=today + timedelta(days=7),
                min_value=today, key="ivivu_checkin_d", label_visibility="collapsed"
            )
        with col_d2:
            iv_checkout_d = st.date_input(
                "Check-out", value=today + timedelta(days=8),
                min_value=today + timedelta(days=1), key="ivivu_checkout_d", label_visibility="collapsed"
            )
        col_d3, col_d4, col_d5 = st.columns(3, gap="medium")
        with col_d3:
            iv_rooms_d = st.number_input("Phòng", 1, 10, 1, key="ivivu_rooms_d", label_visibility="collapsed")
        with col_d4:
            iv_adults_d = st.number_input("Người lớn", 1, 20, 2, key="ivivu_adults_d", label_visibility="collapsed")
        with col_d5:
            iv_children_d = st.number_input("Trẻ em", 0, 10, 0, key="ivivu_children_d", label_visibility="collapsed")
        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        if st.button(
            "🚀  Bắt đầu thu thập",
            disabled=not iv_url.strip() or not iv_dest.strip() or st.session_state.is_scraping,
            key="ivivu_btn", use_container_width=True, type="primary"
        ):
            st.session_state.update({
                "active_url": iv_url.strip(),
                "active_destination": iv_dest.strip(),
                "active_segment": "Hotel",
                "active_source": ota_name,
                "_iv_checkin": iv_checkin_d.strftime("%Y-%m-%d"),
                "_iv_checkout": iv_checkout_d.strftime("%Y-%m-%d"),
                "_iv_rooms": int(iv_rooms_d),
                "_iv_adults": int(iv_adults_d),
                "_iv_children": int(iv_children_d),
                "trigger_scrape": True,
            })

st.markdown("</div>", unsafe_allow_html=True)

# ── Hotel DB case viewer for current source ─────────────────────────────────
if segment_name == "Hotel" and compare_tool_mode:
    st.markdown("#### 🧮 Công cụ so sánh đa kênh")
    if not DB_OK:
        st.warning(
            "Chưa cấu hình Supabase DB. Vào Streamlit Secrets và thêm DATABASE_URL "
            "hoặc SUPABASE_DB_HOST/PORT/NAME/USER/PASSWORD."
        )
    else:
        st.caption(f"DB: {DB_INFO}")
        all_case_rows = _cached_list_hotel_cases(300, None)
        if not all_case_rows:
            st.caption("Chưa có case nào trong DB.")
        else:
            cmp_options = []
            cmp_map = {}
            for c in all_case_rows:
                label = (
                    f"{c.get('destination','')} | {c.get('checkin','')}→{c.get('checkout','')} | "
                    f"{c.get('source_count',0)} nguồn"
                )
                cmp_options.append(label)
                cmp_map[label] = c.get("case_key", "")

            selected_cmp_label = st.selectbox(
                "Case để so sánh",
                cmp_options,
                key="sidebar_tool_compare_case_picker",
            )
            selected_cmp_case = cmp_map.get(selected_cmp_label, "")

            cmp_b1, cmp_b2 = st.columns(2, gap="small")
            with cmp_b1:
                if st.button("🔎 So sánh", key="sidebar_tool_compare_btn", use_container_width=True, type="primary"):
                    cmp_rows = build_cross_channel_compare(selected_cmp_case)
                    st.session_state.global_compare_df = pd.DataFrame(cmp_rows) if cmp_rows else pd.DataFrame()
            with cmp_b2:
                if st.button("🔄 Làm mới danh sách", key="refresh_compare_cases", use_container_width=True):
                    _invalidate_hotel_case_list_cache()
                    st.rerun()

            gdf = st.session_state.get("global_compare_df")
            if isinstance(gdf, pd.DataFrame):
                if not gdf.empty:
                    st.markdown("#### 📊 Bảng so sánh giá đa kênh")
                    st.dataframe(gdf, use_container_width=True, height=420)

                    c1, c2 = st.columns(2, gap="medium")
                    with c1:
                        out_cmp = io.BytesIO()
                        with pd.ExcelWriter(out_cmp, engine="openpyxl") as writer:
                            _sanitize_df_for_openpyxl(gdf).to_excel(
                                writer, index=False, sheet_name="SoSanhDaKenh"
                            )
                        out_cmp.seek(0)
                        st.download_button(
                            label="📊 Tải Excel so sánh",
                            data=out_cmp.getvalue(),
                            file_name="so_sanh_da_kenh.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True,
                        )
                    with c2:
                        cmp_csv = gdf.to_csv(index=False, encoding="utf-8-sig")
                        st.download_button(
                            label="📄 Tải CSV so sánh",
                            data=cmp_csv.encode("utf-8-sig"),
                            file_name="so_sanh_da_kenh.csv",
                            mime="text/csv",
                            use_container_width=True,
                        )
                else:
                    st.info("Case này chưa đủ dữ liệu từ >=2 nguồn để so sánh.")
elif segment_name == "Hotel":
    with st.expander(f"🗂️ Xem lại case DB của {ota_name}", expanded=False):
        if not DB_OK:
            st.warning(
                "Chưa cấu hình Supabase DB. Vào Streamlit Secrets và thêm DATABASE_URL "
                "hoặc SUPABASE_DB_HOST/PORT/NAME/USER/PASSWORD."
            )
        else:
            if st.button("🔄 Làm mới danh sách case từ DB", key=f"refresh_cases_{ota_name}"):
                _invalidate_hotel_case_list_cache()
                st.rerun()
            source_cases = _cached_list_hotel_cases(300, ota_name)
            if not source_cases:
                st.caption(f"Chưa có case nào của kênh {ota_name} trong DB.")
            else:
                fb = st.session_state.pop("_hotel_db_feedback", None)
                if fb:
                    if fb[0] == "ok":
                        st.success(fb[1])
                    else:
                        st.error(fb[1])

                options = []
                case_meta_map = {}
                for c in source_cases:
                    label = (
                        f"{c.get('destination','')} | {c.get('checkin','')}→{c.get('checkout','')} | "
                        f"{c.get('rooms',1)}R-{c.get('adults',2)}A-{c.get('children',0)}C | "
                        f"cập nhật: {c.get('updated_at','')}"
                    )
                    options.append(label)
                    case_meta_map[label] = c

                row_pick, row_trash = st.columns([1, 0.14], vertical_alignment="bottom")
                with row_pick:
                    selected_label = st.selectbox(
                        "Case đã lưu của kênh này",
                        options,
                        key=f"case_picker_{ota_name}",
                    )
                selected_case = case_meta_map.get(selected_label, {})
                selected_case_key = selected_case.get("case_key", "")
                with row_trash:
                    if st.button(
                        "🗑️",
                        key=f"case_trash_{ota_name}",
                        help=f"Xóa dữ liệu đã lưu của {ota_name} cho case này",
                        use_container_width=True,
                    ):
                        if (selected_case_key or "").strip():
                            st.session_state["_hotel_case_delete_draft"] = {
                                "case_key": selected_case_key,
                                "label": selected_label,
                                "ota": ota_name,
                            }

                st.caption(f"Case key: `{selected_case_key}`")

                if st.session_state.get("_hotel_case_delete_draft"):
                    _dlg_confirm_delete_hotel_case()

                if st.button("📥 Nạp lại case vào bảng kết quả", key=f"load_case_{ota_name}", use_container_width=True):
                    loaded_rows = get_case_rows(selected_case_key, ota_name)
                    if loaded_rows:
                        st.session_state.scrape_results = loaded_rows
                        st.session_state.active_destination = selected_case.get("destination", "")
                        st.session_state.active_segment = "Hotel"
                        st.session_state.active_source = ota_name
                        st.session_state.active_case_key = selected_case_key
                        st.success(f"Đã nạp {len(loaded_rows)} dòng từ DB.")
                        st.rerun()
                    else:
                        st.warning("Case này không có dữ liệu chi tiết để nạp.")

elif segment_name == "Tour":
    with st.expander(f"🗂️ Xem lại case DB tour — {ota_name}", expanded=False):
        if not DB_OK:
            st.warning(
                "Chưa cấu hình Supabase DB. Vào Streamlit Secrets và thêm DATABASE_URL "
                "hoặc SUPABASE_DB_HOST/PORT/NAME/USER/PASSWORD."
            )
        else:
            if st.button("🔄 Làm mới danh sách case tour từ DB", key=f"refresh_tour_cases_{ota_name}"):
                _invalidate_tour_case_list_cache()
                st.rerun()
            tour_cases = _cached_list_tour_cases(300, ota_name)
            if not tour_cases:
                st.caption(f"Chưa có case tour nào của nguồn {ota_name} trong DB.")
            else:
                tfb = st.session_state.pop("_tour_db_feedback", None)
                if tfb:
                    if tfb[0] == "ok":
                        st.success(tfb[1])
                    else:
                        st.error(tfb[1])

                t_options = []
                t_meta = {}
                for c in tour_cases:
                    t_label = (
                        f"{c.get('destination','')} | {c.get('period_start','')}→{c.get('period_end','') or '—'} | "
                        f"{c.get('currency','') or '—'} | {c.get('row_count',0)} dòng | cập nhật: {c.get('updated_at','')}"
                    )
                    t_options.append(t_label)
                    t_meta[t_label] = c

                t_row_pick, t_row_trash = st.columns([1, 0.14], vertical_alignment="bottom")
                with t_row_pick:
                    t_sel = st.selectbox("Case tour đã lưu", t_options, key=f"tour_case_picker_{ota_name}")
                t_case = t_meta.get(t_sel, {})
                t_key = t_case.get("case_key", "")
                with t_row_trash:
                    if st.button(
                        "🗑️",
                        key=f"tour_case_trash_{ota_name}",
                        help=f"Xóa dữ liệu tour đã lưu của {ota_name} cho case này",
                        use_container_width=True,
                    ):
                        if (t_key or "").strip():
                            st.session_state["_tour_case_delete_draft"] = {
                                "case_key": t_key,
                                "label": t_sel,
                                "ota": ota_name,
                            }

                st.caption(f"Case key: `{t_key}`")

                if st.session_state.get("_tour_case_delete_draft"):
                    _dlg_confirm_delete_tour_case()

                if st.button("📥 Nạp lại case tour vào bảng", key=f"load_tour_case_{ota_name}", use_container_width=True):
                    loaded = get_tour_case_rows(t_key, ota_name)
                    if loaded:
                        st.session_state.scrape_results = loaded
                        st.session_state.active_destination = t_case.get("destination", "")
                        st.session_state.active_segment = "Tour"
                        st.session_state.active_source = ota_name
                        st.session_state.active_case_key = t_key
                        st.success(f"Đã nạp {len(loaded)} tour từ DB.")
                        st.rerun()
                    else:
                        st.warning("Case này không có dữ liệu chi tiết để nạp.")

# ── Scraping ──────────────────────────────────────────────────────────────────
if (not compare_tool_mode) and st.session_state.get("trigger_scrape"):
    st.session_state["trigger_scrape"] = False
    st.session_state.is_scraping = True
    st.session_state.scrape_results = None

    active_url = st.session_state.get("active_url", "")
    active_destination = st.session_state.get("active_destination", "")
    active_segment = st.session_state.get("active_segment", segment_name)
    active_source = st.session_state.get("active_source", ota_name)

    status_messages = []
    def update_status(msg: str):
        status_messages.append(msg)

    with st.spinner(f"⏳  Đang kết nối và tải dữ liệu từ {active_source}..."):
        try:
            if active_segment == "Tour":
                if active_source == "FindTourGo":
                    results = run_scrape_findtourgo_tours(
                        url=active_url,
                        country_code=st.session_state.get("_tour_country", ""),
                        period_start=st.session_state.get("_tour_period_start", ""),
                        period_end=st.session_state.get("_tour_period_end", ""),
                        currency=st.session_state.get("_tour_currency", "USD"),
                        locale="vi",
                        status_callback=update_status,
                    )
                elif active_source == "Travel.com.vn":
                    results = run_scrape_travel_tour(
                        url=active_url,
                        status_callback=update_status,
                    )
                else:
                    raise ValueError(f"Nguồn tour chưa hỗ trợ: {active_source}")
                st.session_state.scrape_results = results
            elif active_source == "Agoda":
                raw_results = run_scrape(
                    url=active_url,
                    destination=active_destination,
                    status_callback=update_status,
                    visible_browser=st.session_state.get("agoda_visible_browser", False),
                )
                st.session_state.scrape_results = normalize_hotel_rows(raw_results, source=active_source, destination=active_destination)
            elif active_source == "Trip.com":
                raw_results = run_scrape_tripcom(url=active_url, destination=active_destination, status_callback=update_status)
                st.session_state.scrape_results = normalize_hotel_rows(raw_results, source=active_source, destination=active_destination)
            elif active_source == "Travel.com.vn":
                raw_results = run_scrape_travel(url=active_url, destination=active_destination, status_callback=update_status)
                st.session_state.scrape_results = normalize_hotel_rows(raw_results, source=active_source, destination=active_destination)
            elif active_source == "iVIVU":
                raw_results = run_scrape_ivivu(
                    url=active_url,
                    destination=active_destination,
                    check_in=st.session_state.get("_iv_checkin", ""),
                    check_out=st.session_state.get("_iv_checkout", ""),
                    rooms=st.session_state.get("_iv_rooms", 1),
                    adults=st.session_state.get("_iv_adults", 2),
                    children=st.session_state.get("_iv_children", 0),
                    status_callback=update_status,
                )
                st.session_state.scrape_results = normalize_hotel_rows(raw_results, source=active_source, destination=active_destination)
            else:  # Mytour.vn
                ci_str = st.session_state.get("check_in_str", "")
                co_str = st.session_state.get("check_out_str", "")
                pid = st.session_state.get("mytour_province_id")
                slug = st.session_state.get("mytour_city_slug", "")
                paste_mode = st.session_state.get("mytour_paste_mode", False)
                mt_rooms = st.session_state.get("_mt_rooms", 1)
                mt_adults = st.session_state.get("_mt_adults", 2)
                mt_children = st.session_state.get("_mt_children", 0)
                if paste_mode or pid is None:
                    raw_results = run_scrape_mytour(
                        url=active_url, destination=active_destination,
                        check_in=ci_str, check_out=co_str,
                        province_id=None, city_slug="",
                        rooms=mt_rooms, adults=mt_adults, children=mt_children,
                        status_callback=update_status,
                    )
                else:
                    raw_results = run_scrape_mytour(
                        url=active_url, destination=active_destination,
                        check_in=ci_str, check_out=co_str,
                        province_id=pid, city_slug=slug,
                        rooms=mt_rooms, adults=mt_adults, children=mt_children,
                        status_callback=update_status,
                    )
                st.session_state.scrape_results = normalize_hotel_rows(raw_results, source=active_source, destination=active_destination)
            results = st.session_state.scrape_results
        except Exception as e:
            # Some exceptions stringify to an empty string; always show a useful message.
            err_text = str(e).strip() or f"{type(e).__name__}: {repr(e)}"
            st.error(f"❌  Lỗi: {err_text}")
            tb_text = traceback.format_exc()
            with st.expander("🧩 Chi tiết lỗi kỹ thuật"):
                st.code(tb_text, language="text")
            results = []

    st.session_state.is_scraping = False

    # Persist hotel snapshots with "replace same case + same source" rule.
    if active_segment == "Hotel" and results:
        try:
            case_info = _extract_hotel_case_info(active_source, active_url, active_destination)
            saved_count = replace_case_source(case_info, active_source, results)
            _invalidate_hotel_case_list_cache()
            st.session_state.active_case_key = case_info["case_key"]
            status_messages.append(
                f"💾 DB: cập nhật case={case_info['case_key']} | source={active_source} | rows={saved_count}"
            )
        except Exception as db_err:
            status_messages.append(f"⚠️ DB save lỗi: {type(db_err).__name__}: {db_err}")
    elif active_segment == "Hotel":
        st.session_state.active_case_key = _extract_hotel_case_info(active_source, active_url, active_destination)["case_key"]
    elif active_segment == "Tour" and results:
        try:
            t_info = _extract_tour_case_info(active_source, active_url, active_destination)
            saved_t = replace_tour_case_source(t_info, active_source, results)
            _invalidate_tour_case_list_cache()
            st.session_state.active_case_key = t_info["case_key"]
            status_messages.append(
                f"💾 DB tour: case={t_info['case_key']} | source={active_source} | rows={saved_t}"
            )
        except Exception as db_err:
            status_messages.append(f"⚠️ DB tour lỗi: {type(db_err).__name__}: {db_err}")
    elif active_segment == "Tour":
        st.session_state.active_case_key = _extract_tour_case_info(active_source, active_url, active_destination)["case_key"]

    if status_messages:
        with st.expander("📋  Nhật ký chi tiết"):
            for msg in status_messages:
                st.markdown(f"<div style='font-family:monospace;font-size:.82rem;padding:3px 12px;border-left:3px solid #ccc;margin:2px 0;'>• {msg}</div>", unsafe_allow_html=True)

    unit = "tour" if active_segment == "Tour" else "khách sạn"
    if results:
        st.success(f"✅  Hoàn tất! Đã thu thập **{len(results)}** {unit} từ {active_source}.")
    else:
        st.warning(f"⚠️  Không tìm thấy dữ liệu. Hãy thử lại hoặc chọn điểm đến khác.")

# ── Results ──────────────────────────────────────────────────────────────────
if (not compare_tool_mode) and st.session_state.scrape_results:
    results = st.session_state.scrape_results
    df = pd.DataFrame(results)
    active_destination = st.session_state.get("active_destination", "data")
    active_segment = st.session_state.get("active_segment", segment_name)
    active_source = st.session_state.get("active_source", ota_name)
    if active_segment == "Hotel":
        for col in HOTEL_RESULT_COLUMNS:
            if col not in df.columns:
                df[col] = ""
        df = df[HOTEL_RESULT_COLUMNS]
        hotel_display_cols = hotel_table_column_order(active_source)
    else:
        hotel_display_cols = []
        for col in TOUR_RESULT_COLUMNS:
            if col not in df.columns:
                df[col] = ""
        df = df[TOUR_RESULT_COLUMNS]
        if active_source == "FindTourGo" and "Ngày khởi hành" in df.columns:

            def _fmt_ftg_departure(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return ""
                s = str(v).strip()
                if not s or s.lower() == "nan":
                    return ""
                return normalize_findtourgo_departure_display(s)

            df["Ngày khởi hành"] = df["Ngày khởi hành"].apply(_fmt_ftg_departure)

    header_class = {
        "Agoda": "result-header-agoda",
        "Trip.com": "result-header-tripcom",
        "Mytour.vn": "result-header-mytour",
        "Travel.com.vn": "result-header-travel",
        "iVIVU": "result-header-ivivu",
        "FindTourGo": "result-header-tripcom",
    }.get(active_source, "result-header-agoda")

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    st.markdown(f"""
    <div class="result-header {header_class}">
      <span style="font-size:1.5rem">{logo_map.get(active_source, '📦')}</span>
      <h3>{active_source} · {active_destination}</h3>
      <span class="result-badge">{len(df)} {'tour' if active_segment == 'Tour' else 'khách sạn'}</span>
    </div>
    """, unsafe_allow_html=True)

    # ── Metrics ──
    m_cols = st.columns(5)
    if active_segment == "Tour":
        m_cols[0].metric("🧭 Tổng tour", len(df))
        m_cols[1].metric("📅 Có ngày khởi hành", int(df["Ngày khởi hành"].astype(bool).sum()))
        m_cols[2].metric("💰 Có giá", int(df["Giá từ"].astype(bool).sum()))
        m_cols[3].metric("🏢 Có hãng lữ hành", int(df["Công ty lữ hành"].astype(bool).sum()))
        m_cols[4].metric("⭐ Có điểm đánh giá", int(df["Điểm đánh giá"].astype(bool).sum()))
    else:
        m_cols[0].metric("🏨 Tổng", len(df))
        m_cols[1].metric("💰 Có giá", int(df["Giá/đêm (VND)"].astype(bool).sum()))
        m_cols[2].metric("⭐ Có sao", int(df["Hạng sao"].astype(bool).sum()))
        m_cols[3].metric("🍳 Có bữa ăn", int(df["Gói bữa ăn"].astype(bool).sum()))
        m_cols[4].metric("🔓 Hủy miễn phí", int(df["Chính sách hoàn hủy"].str.contains("Hủy miễn phí", na=False).sum()))

    # ── Filters ──
    st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
    st.markdown("#### 🔍  Tìm kiếm & Lọc")
    if active_segment == "Tour":
        fc1, fc2, fc3 = st.columns([2, 1, 1], gap="medium")
        with fc1:
            search_text = st.text_input("Tên tour", placeholder="🔎  Nhập tên tour...", key="search_filter_tour")
        with fc2:
            has_departure = st.selectbox("📅 Khởi hành", ["Tất cả", "Có ngày khởi hành"], key="departure_filter")
        with fc3:
            min_days = st.number_input("⏱️ Số ngày tối thiểu", min_value=0, max_value=90, value=0, key="duration_filter")

        fdf = df.copy()
        if search_text:
            fdf = fdf[fdf["Tên tour"].str.contains(search_text, case=False, na=False)]
        if has_departure == "Có ngày khởi hành":
            fdf = fdf[fdf["Ngày khởi hành"].astype(bool)]
        if min_days > 0:
            duration_num = (
                fdf["Thời lượng (ngày)"]
                .astype(str)
                .str.extract(r"(\d+)", expand=False)
            )
            fdf = fdf[pd.to_numeric(duration_num, errors="coerce").fillna(0) >= min_days]
        if len(fdf) < len(df):
            st.caption(f"Hiển thị {len(fdf)} / {len(df)} tour")

        col_cfg = {
            "Nguồn": st.column_config.TextColumn("🌐 Nguồn", width="small"),
            "Quốc gia": st.column_config.TextColumn("🌍 Quốc gia", width="small"),
            "Tên tour": st.column_config.TextColumn("🧭 Tên tour", width="large"),
            "Mã tour": st.column_config.TextColumn("🔖 Mã", width="small"),
            "Công ty lữ hành": st.column_config.TextColumn("🏢 Lữ hành", width="medium"),
            "Thời lượng (ngày)": st.column_config.NumberColumn("⏱️ Ngày", width="small"),
            "Giá từ": st.column_config.TextColumn("💰 Giá từ", width="small"),
            "Tiền tệ": st.column_config.TextColumn("💱", width="small"),
            "Điểm đánh giá": st.column_config.TextColumn("⭐ Đánh giá", width="small"),
            "Điểm khởi hành": st.column_config.TextColumn("🧳 Khởi hành", width="small"),
            "Phương tiện": st.column_config.TextColumn("✈️ Phương tiện", width="small"),
            "Ngày khởi hành": st.column_config.TextColumn("📅 Ngày khởi hành", width="large"),
            "Link tour": st.column_config.LinkColumn("🔗 Link tour", width="small"),
        }
    else:
        fc1, fc2, fc3 = st.columns([2, 1, 1], gap="medium")
        with fc1:
            search_text = st.text_input("Tên khách sạn", placeholder="🔎  Nhập tên...", key="search_filter")
        with fc2:
            star_opts = ["Tất cả"] + sorted([s for s in df["Hạng sao"].dropna().unique() if s])
            selected_star = st.selectbox("⭐  Hạng sao", star_opts, key="star_filter")
        with fc3:
            cancel_opts = ["Tất cả", "Hủy miễn phí", "Không hủy miễn phí"]
            selected_cancel = st.selectbox("🔓  Chính sách hủy", cancel_opts, key="cancel_filter")

        fdf = df.copy()
        if search_text:
            fdf = fdf[fdf["Tên khách sạn"].str.contains(search_text, case=False, na=False)]
        if selected_star != "Tất cả":
            fdf = fdf[fdf["Hạng sao"] == selected_star]
        if selected_cancel == "Hủy miễn phí":
            fdf = fdf[fdf["Chính sách hoàn hủy"].str.contains("Hủy miễn phí", na=False)]
        elif selected_cancel == "Không hủy miễn phí":
            fdf = fdf[~fdf["Chính sách hoàn hủy"].str.contains("Hủy miễn phí", na=False)]
        if len(fdf) < len(df):
            st.caption(f"Hiển thị {len(fdf)} / {len(df)} khách sạn")

        col_cfg = {
            "Nguồn": st.column_config.TextColumn("🌐 Nguồn", width="small"),
            "Tỉnh thành / Điểm đến": st.column_config.TextColumn("📍 Điểm đến", width="small"),
            "Tên khách sạn": st.column_config.TextColumn("🏨 Tên khách sạn", width="large"),
            "Nhãn badge": st.column_config.TextColumn(
                "🏷️ Nhãn badge", width="large", help="VD: 2N1Đ Xe + Ăn sáng | 1tr099"
            ),
            "Địa chỉ": st.column_config.TextColumn("📌 Địa chỉ", width="large"),
            "Mã Property Agoda": st.column_config.TextColumn("🔑 Agoda ID", width="small"),
            "ID khách sạn Travel": st.column_config.TextColumn("🆔 Travel ID", width="small"),
            "Mã property (OTA)": st.column_config.TextColumn("🆔 Property ID", width="small"),
            "Vĩ độ": st.column_config.TextColumn("φ Vĩ độ", width="small"),
            "Kinh độ": st.column_config.TextColumn("λ Kinh độ", width="small"),
            "Hạng sao": st.column_config.TextColumn("⭐ Sao", width="small"),
            "Điểm đánh giá": st.column_config.TextColumn("📊 Đánh giá", width="small"),
            "Số đánh giá": st.column_config.TextColumn("📝 Số đánh giá", width="small"),
            "Giá/đêm (VND)": st.column_config.TextColumn("💰 Giá/đêm (VND)", width="medium"),
            "Giá/đêm (chưa gồm thuế)": st.column_config.TextColumn("💸 Chưa thuế", width="small"),
            "Giá/đêm (đã gồm thuế)": st.column_config.TextColumn("💳 Đã gồm thuế", width="small"),
            "Thuế phí ước tính": st.column_config.TextColumn("🧾 Thuế phí", width="small"),
            "Gói bữa ăn": st.column_config.TextColumn("🍳 Bữa ăn", width="small"),
            "Chính sách hoàn hủy": st.column_config.TextColumn("📋 Hủy", width="medium"),
            "Link khách sạn": st.column_config.LinkColumn("🔗 Link", width="small"),
        }

    if active_segment == "Hotel":
        fdf_show = fdf[hotel_display_cols]
        col_cfg_show = {k: v for k, v in col_cfg.items() if k in hotel_display_cols}
    else:
        fdf_show = fdf
        col_cfg_show = col_cfg

    st.dataframe(fdf_show, use_container_width=True, height=480, column_config=col_cfg_show)

    # ── Export ──
    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    st.markdown("#### 📥  Xuất dữ liệu")
    dl1, dl2, dl3 = st.columns([2, 2, 1], gap="medium")

    with dl1:
        out = io.BytesIO()
        sheet = f"{active_source} - {active_destination}"[:31]
        df_xlsx = df[hotel_display_cols] if active_segment == "Hotel" else df
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            _sanitize_df_for_openpyxl(df_xlsx).to_excel(writer, index=False, sheet_name=sheet)
            ws = writer.sheets[sheet]
            widths = [18, 45, 35, 45, 12, 18, 18, 14, 18, 20, 35, 55]
            from openpyxl.utils import get_column_letter
            for i, w in enumerate(widths[:len(df_xlsx.columns)]):
                ws.column_dimensions[get_column_letter(i + 1)].width = w
        out.seek(0)
        st.download_button(
            label="📊  Tải Excel (.xlsx)", data=out.getvalue(),
            file_name=f"{active_source}_{active_destination}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True, type="primary"
        )
    with dl2:
        df_csv = df[hotel_display_cols] if active_segment == "Hotel" else df
        csv_data = df_csv.to_csv(index=False, encoding="utf-8-sig")
        st.download_button(
            label="📄  Tải CSV (.csv)", data=csv_data.encode("utf-8-sig"),
            file_name=f"{active_source}_{active_destination}.csv",
            mime="text/csv", use_container_width=True
        )
    with dl3:
        if st.button("🗑️  Xóa & tìm lại", use_container_width=True):
            st.session_state.scrape_results = None
            st.session_state.compare_df = None
            st.rerun()

st.markdown("""
<div class="footer">
  ⚠️ <strong>Lưu ý:</strong> Tool chỉ dùng cho mục đích nghiên cứu thị trường.
  Hãy sử dụng có trách nhiệm và tuân thủ điều khoản của các OTA.
</div>
""", unsafe_allow_html=True)
