import streamlit as st
import pandas as pd
import io
from datetime import date, timedelta
from scraper import build_agoda_url, run_scrape
from scraper_tripcom import (
    build_tripcom_url, resolve_trip_city, run_scrape_tripcom, KNOWN_CITY_IDS
)

st.set_page_config(
    page_title="OTA Hotel Scraper",
    page_icon="🏨",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
/* ── Global ── */
[data-testid="stAppViewContainer"] { background: #F0F4F8; }
[data-testid="stHeader"] { background: transparent; }
[data-testid="stToolbar"] { display: none; }
.block-container { padding-top: 0 !important; max-width: 1200px; }

/* ── Hero ── */
.hero {
    border-radius: 0 0 28px 28px;
    padding: 2.4rem 2rem 1.8rem;
    margin: -1rem -1rem 1.6rem -1rem;
    text-align: center;
    box-shadow: 0 4px 24px rgba(0,0,0,0.18);
}
.hero-agoda { background: linear-gradient(135deg, #E53E1A 0%, #FF6B35 50%, #FF9A3C 100%); }
.hero-tripcom { background: linear-gradient(135deg, #007DFF 0%, #00B4D8 60%, #0096C7 100%); }
.hero h1 { color:#fff!important; font-size:2.4rem!important; font-weight:800!important; margin:0 0 0.25rem!important; text-shadow:0 2px 8px rgba(0,0,0,.2); }
.hero p  { color:rgba(255,255,255,.9)!important; font-size:1rem!important; margin:0!important; }

/* ── OTA selector ── */
.ota-bar {
    display: flex; gap: 12px; margin-bottom: 1.4rem;
    background:#fff; border-radius:16px; padding:8px;
    box-shadow:0 2px 10px rgba(0,0,0,.07); border:1px solid #E8ECF0;
}
.ota-btn {
    flex:1; border:none; border-radius:12px; padding:.7rem 1rem;
    font-size:.95rem; font-weight:700; cursor:pointer; transition:all .2s;
    background:transparent; color:#6B7280;
}
.ota-btn.active-agoda { background:linear-gradient(135deg,#E53E1A,#FF6B35); color:#fff; box-shadow:0 3px 12px rgba(229,62,26,.35); }
.ota-btn.active-tripcom { background:linear-gradient(135deg,#007DFF,#00B4D8); color:#fff; box-shadow:0 3px 12px rgba(0,125,255,.35); }

/* ── Tabs ── */
[data-testid="stTabs"] [role="tablist"] {
    background:#fff; border-radius:12px; padding:4px; gap:4px;
    box-shadow:0 2px 8px rgba(0,0,0,.06); border:1px solid #E8ECF0;
}
[data-testid="stTabs"] button[role="tab"] {
    border-radius:9px!important; font-weight:600!important;
    font-size:.9rem!important; padding:.5rem 1.2rem!important; color:#6B7280!important;
}
[data-testid="stTabs"] button[role="tab"][aria-selected="true"] {
    color:#fff!important;
}
.ota-agoda [data-testid="stTabs"] button[role="tab"][aria-selected="true"] {
    background:linear-gradient(135deg,#E53E1A,#FF6B35)!important;
    box-shadow:0 2px 8px rgba(229,62,26,.35)!important;
}
.ota-tripcom [data-testid="stTabs"] button[role="tab"][aria-selected="true"] {
    background:linear-gradient(135deg,#007DFF,#00B4D8)!important;
    box-shadow:0 2px 8px rgba(0,125,255,.35)!important;
}
[data-testid="stTabs"] [role="tabpanel"] { padding-top:0!important; }

/* ── Inputs ── */
[data-testid="stTextInput"] input,
[data-testid="stDateInput"] input,
[data-testid="stNumberInput"] input,
[data-testid="stTextArea"] textarea {
    border-radius:10px!important; border:1.5px solid #E2E8F0!important; background:#FAFBFC!important;
}

/* ── Buttons ── */
[data-testid="stButton"] button[kind="primary"] {
    border:none!important; border-radius:12px!important; font-weight:700!important;
    font-size:1rem!important; padding:.7rem 1.5rem!important; letter-spacing:.01em;
    transition:transform .15s, box-shadow .15s!important;
}
[data-testid="stButton"] button[kind="primary"]:hover { transform:translateY(-1px)!important; }
[data-testid="stButton"] button[kind="secondary"] {
    border-radius:10px!important; border:1.5px solid #E2E8F0!important; font-weight:600!important; color:#374151!important;
}

/* agoda primary btn */
.ota-agoda [data-testid="stButton"] button[kind="primary"] {
    background:linear-gradient(135deg,#E53E1A,#FF6B35)!important;
    box-shadow:0 4px 14px rgba(229,62,26,.35)!important;
}
.ota-agoda [data-testid="stButton"] button[kind="primary"]:hover {
    box-shadow:0 6px 20px rgba(229,62,26,.45)!important;
}
/* tripcom primary btn */
.ota-tripcom [data-testid="stButton"] button[kind="primary"] {
    background:linear-gradient(135deg,#007DFF,#00B4D8)!important;
    box-shadow:0 4px 14px rgba(0,125,255,.35)!important;
}
.ota-tripcom [data-testid="stButton"] button[kind="primary"]:hover {
    box-shadow:0 6px 20px rgba(0,125,255,.45)!important;
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

/* ── Status box ── */
.status-box {
    background:#F8F9FA; border-left:3px solid #007DFF;
    border-radius:0 8px 8px 0; padding:7px 14px;
    font-size:.85rem; color:#374151; margin:4px 0; font-family:monospace;
}
.status-box-agoda { border-left-color:#E53E1A!important; }

/* ── Result header ── */
.result-header {
    display:flex; align-items:center; gap:.7rem;
    color:#fff; border-radius:14px; padding:1rem 1.5rem; margin-bottom:1.2rem;
    box-shadow:0 2px 12px rgba(0,0,0,.2);
}
.result-header-agoda { background:linear-gradient(135deg,#E53E1A,#b52b0f); }
.result-header-tripcom { background:linear-gradient(135deg,#007DFF,#0056b3); }
.result-header h3 { margin:0; font-size:1.2rem; font-weight:700; color:#fff; }
.result-badge { background:rgba(255,255,255,.25); color:#fff; border-radius:20px; padding:.2rem .8rem; font-size:.9rem; font-weight:700; margin-left:auto; }

/* ── Info badge ── */
.info-badge {
    display:inline-block; border-radius:8px; padding:.55rem 1rem;
    font-size:.88rem; font-weight:500; margin-bottom:1rem; width:100%;
}
.info-badge-agoda { background:#FFF4EE; color:#E53E1A; border:1px solid #FFD5C2; }
.info-badge-tripcom { background:#EFF6FF; color:#007DFF; border:1px solid #BFDBFE; }

/* ── Section label ── */
.section-label { font-size:.82rem; font-weight:600; color:#6B7280; text-transform:uppercase; letter-spacing:.06em; margin-bottom:.3rem; }

/* ── Footer ── */
.footer { text-align:center; color:#9CA3AF; font-size:.8rem; padding:1.5rem 0 .5rem; border-top:1px solid #E8ECF0; margin-top:1rem; }

/* ── Download buttons ── */
.stDownloadButton button { border-radius:12px!important; font-weight:700!important; padding:.65rem 1.5rem!important; }
</style>
""", unsafe_allow_html=True)

# ── Session state ─────────────────────────────────────────────────────────────
for key, default in [
    ("scrape_results", None), ("is_scraping", False),
    ("active_ota", "Agoda"), ("active_destination", ""),
    ("active_url", ""), ("trigger_scrape", False),
]:
    if key not in st.session_state:
        st.session_state[key] = default

# ── OTA selector ──────────────────────────────────────────────────────────────
ota_col1, ota_col2, ota_col3 = st.columns([1, 1, 2])
with ota_col1:
    if st.button("🟠  Agoda", use_container_width=True,
                 type="primary" if st.session_state.active_ota == "Agoda" else "secondary",
                 key="ota_agoda_btn"):
        st.session_state.active_ota = "Agoda"
        st.session_state.scrape_results = None
        st.rerun()
with ota_col2:
    if st.button("🔵  Trip.com", use_container_width=True,
                 type="primary" if st.session_state.active_ota == "Trip.com" else "secondary",
                 key="ota_trip_btn"):
        st.session_state.active_ota = "Trip.com"
        st.session_state.scrape_results = None
        st.rerun()
with ota_col3:
    ota_badge_color = "#E53E1A" if st.session_state.active_ota == "Agoda" else "#007DFF"
    st.markdown(f"""
    <div style='padding:.5rem 0;'>
      <span style='background:{ota_badge_color};color:#fff;border-radius:8px;padding:.35rem .9rem;font-weight:700;font-size:.85rem;'>
        Đang dùng: {st.session_state.active_ota}
      </span>
    </div>""", unsafe_allow_html=True)

ota = st.session_state.active_ota
hero_class = "hero-agoda" if ota == "Agoda" else "hero-tripcom"
ota_class = "ota-agoda" if ota == "Agoda" else "ota-tripcom"
status_box_extra = "status-box-agoda" if ota == "Agoda" else ""

# ── Hero ──────────────────────────────────────────────────────────────────────
logo = "🟠" if ota == "Agoda" else "🔵"
subtitle = "Thu thập dữ liệu khách sạn · Phân tích giá · Xuất Excel / CSV"
st.markdown(f"""
<div class="hero {hero_class}">
  <h1>{logo} {ota} Hotel Scraper</h1>
  <p>{subtitle}</p>
</div>
""", unsafe_allow_html=True)

st.markdown(f"<div class='{ota_class}'>", unsafe_allow_html=True)

# ── Input form ────────────────────────────────────────────────────────────────
today = date.today()

if ota == "Agoda":
    tab1, tab2 = st.tabs(["📋  Nhập cấu hình", "🔗  Dán URL trực tiếp"])

    with tab1:
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
        st.markdown("<div class='section-label'>📍 Điểm đến</div>", unsafe_allow_html=True)
        destination_form = st.text_input("Điểm đến", placeholder="VD: Hà Nội, Đà Nẵng, Hội An, Phú Quốc...",
                                         key="destination_form", label_visibility="collapsed")

        col3, col4 = st.columns(2, gap="medium")
        with col3:
            st.markdown("<div class='section-label'>📅 Ngày Check-in</div>", unsafe_allow_html=True)
            checkin_date = st.date_input("Check-in", value=today + timedelta(days=7),
                                         min_value=today, key="checkin_date", label_visibility="collapsed")
        with col4:
            st.markdown("<div class='section-label'>📅 Ngày Check-out</div>", unsafe_allow_html=True)
            checkout_date = st.date_input("Check-out", value=today + timedelta(days=8),
                                          min_value=today + timedelta(days=1), key="checkout_date", label_visibility="collapsed")

        col5, col6, col7 = st.columns(3, gap="medium")
        with col5:
            st.markdown("<div class='section-label'>🛏️ Số phòng</div>", unsafe_allow_html=True)
            num_rooms = st.number_input("Phòng", min_value=1, max_value=10, value=1, key="num_rooms", label_visibility="collapsed")
        with col6:
            st.markdown("<div class='section-label'>👤 Người lớn</div>", unsafe_allow_html=True)
            num_adults = st.number_input("Người lớn", min_value=1, max_value=20, value=2, key="num_adults", label_visibility="collapsed")
        with col7:
            st.markdown("<div class='section-label'>👶 Trẻ em</div>", unsafe_allow_html=True)
            num_children = st.number_input("Trẻ em", min_value=0, max_value=10, value=0, key="num_children", label_visibility="collapsed")

        child_ages_form = []
        if num_children > 0:
            st.markdown("<div class='section-label'>🎂 Độ tuổi trẻ em</div>", unsafe_allow_html=True)
            age_cols = st.columns(min(num_children, 5))
            for i in range(num_children):
                with age_cols[i % 5]:
                    child_ages_form.append(st.number_input(f"Trẻ {i+1}", min_value=0, max_value=17, value=5, key=f"child_age_{i}"))

        if checkin_date >= checkout_date:
            st.error("⚠️ Check-out phải sau Check-in!")
            btn_disabled = True
        else:
            btn_disabled = False

        url_preview = ""
        if destination_form and not btn_disabled:
            url_preview = build_agoda_url(
                destination=destination_form,
                check_in=checkin_date.strftime("%Y-%m-%d"),
                check_out=checkout_date.strftime("%Y-%m-%d"),
                rooms=num_rooms, adults=num_adults, children=num_children,
                child_ages=child_ages_form
            )
            with st.expander("👁️ Xem URL sẽ được scrape"):
                st.code(url_preview, language="text")

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        if st.button("🚀  Bắt đầu thu thập dữ liệu", disabled=btn_disabled or not destination_form or st.session_state.is_scraping,
                     key="scrape_form_btn", use_container_width=True, type="primary"):
            if url_preview:
                st.session_state.update({"active_url": url_preview, "active_destination": destination_form, "trigger_scrape": True})

    with tab2:
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
        st.markdown("""<div class="info-badge info-badge-agoda">
          💡 Truy cập Agoda, tìm kiếm khách sạn theo ý muốn, copy toàn bộ URL và dán vào đây.
        </div>""", unsafe_allow_html=True)
        st.markdown("<div class='section-label'>🔗 URL tìm kiếm Agoda</div>", unsafe_allow_html=True)
        direct_url = st.text_area("URL", placeholder="https://www.agoda.com/search?city=...",
                                   height=90, key="direct_url", label_visibility="collapsed")
        st.markdown("<div class='section-label'>🗺️ Tên điểm đến</div>", unsafe_allow_html=True)
        dest_url_tab = st.text_input("Điểm đến", placeholder="VD: Đà Nẵng", key="dest_url_tab", label_visibility="collapsed")
        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        if st.button("🚀  Bắt đầu thu thập dữ liệu",
                     disabled=not direct_url.strip() or not dest_url_tab.strip() or st.session_state.is_scraping,
                     key="scrape_url_btn", use_container_width=True, type="primary"):
            pasted = direct_url.strip()
            if "currency=VND" not in pasted:
                sep = "&" if "?" in pasted else "?"
                pasted += f"{sep}currency=VND&currencyCode=VND&priceCur=VND"
            st.session_state.update({"active_url": pasted, "active_destination": dest_url_tab.strip() or "Không xác định", "trigger_scrape": True})

else:  # Trip.com
    st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

    # Show supported cities
    vn_cities = sorted(set(v for k, v in {
        "Hà Nội": "Hà Nội", "TP. HCM": "Hồ Chí Minh", "Đà Nẵng": "Đà Nẵng",
        "Phú Quốc": "Phú Quốc", "Nha Trang": "Nha Trang", "Đà Lạt": "Đà Lạt",
        "Hội An": "Hội An", "Hạ Long": "Hạ Long", "Huế": "Huế",
        "Vũng Tàu": "Vũng Tàu", "Cần Thơ": "Cần Thơ", "Sa Pa": "Sa Pa",
    }.items()))
    st.markdown(f"""<div class="info-badge info-badge-tripcom">
      🌐 Trip.com hỗ trợ các thành phố: {" · ".join(vn_cities[:12])} và nhiều thành phố khác.
    </div>""", unsafe_allow_html=True)

    st.markdown("<div class='section-label'>📍 Điểm đến</div>", unsafe_allow_html=True)
    trip_dest = st.text_input("Điểm đến", placeholder="VD: Hà Nội, Đà Nẵng, Phú Quốc...",
                              key="trip_dest", label_visibility="collapsed")

    # City ID lookup preview
    if trip_dest.strip():
        city_info = resolve_trip_city(trip_dest.strip())
        if city_info:
            st.caption(f"✅ Tìm thấy: City ID = {city_info[0]}")
        else:
            st.warning(f"⚠️ Chưa có dữ liệu cho '{trip_dest}'. Hỗ trợ thêm điểm đến trong tương lai.")

    col_t1, col_t2 = st.columns(2, gap="medium")
    with col_t1:
        st.markdown("<div class='section-label'>📅 Ngày Check-in</div>", unsafe_allow_html=True)
        trip_checkin = st.date_input("Check-in", value=today + timedelta(days=7),
                                     min_value=today, key="trip_checkin", label_visibility="collapsed")
    with col_t2:
        st.markdown("<div class='section-label'>📅 Ngày Check-out</div>", unsafe_allow_html=True)
        trip_checkout = st.date_input("Check-out", value=today + timedelta(days=8),
                                      min_value=today + timedelta(days=1), key="trip_checkout", label_visibility="collapsed")

    col_t3, col_t4, col_t5 = st.columns(3, gap="medium")
    with col_t3:
        st.markdown("<div class='section-label'>🛏️ Số phòng</div>", unsafe_allow_html=True)
        trip_rooms = st.number_input("Phòng", min_value=1, max_value=10, value=1, key="trip_rooms", label_visibility="collapsed")
    with col_t4:
        st.markdown("<div class='section-label'>👤 Người lớn</div>", unsafe_allow_html=True)
        trip_adults = st.number_input("Người lớn", min_value=1, max_value=20, value=2, key="trip_adults", label_visibility="collapsed")
    with col_t5:
        st.markdown("<div class='section-label'>👶 Trẻ em</div>", unsafe_allow_html=True)
        trip_children = st.number_input("Trẻ em", min_value=0, max_value=10, value=0, key="trip_children", label_visibility="collapsed")

    if trip_checkin >= trip_checkout:
        st.error("⚠️ Check-out phải sau Check-in!")
        trip_btn_disabled = True
    else:
        trip_btn_disabled = False

    trip_city_info = resolve_trip_city(trip_dest.strip()) if trip_dest.strip() else None

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
    if st.button("🚀  Bắt đầu thu thập dữ liệu",
                 disabled=trip_btn_disabled or not trip_dest.strip() or not trip_city_info or st.session_state.is_scraping,
                 key="scrape_trip_btn", use_container_width=True, type="primary"):
        city_id, country_id = trip_city_info
        url = build_tripcom_url(
            city_id=city_id,
            check_in=trip_checkin.strftime("%Y-%m-%d"),
            check_out=trip_checkout.strftime("%Y-%m-%d"),
            rooms=trip_rooms, adults=trip_adults, children=trip_children
        )
        st.session_state.update({"active_url": url, "active_destination": trip_dest.strip(), "trigger_scrape": True})

st.markdown("</div>", unsafe_allow_html=True)

# ── Scraping ──────────────────────────────────────────────────────────────────
if st.session_state.get("trigger_scrape"):
    st.session_state["trigger_scrape"] = False
    st.session_state.is_scraping = True
    st.session_state.scrape_results = None

    active_url = st.session_state.get("active_url", "")
    active_destination = st.session_state.get("active_destination", "")
    active_ota = st.session_state.get("active_ota", "Agoda")

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    status_messages = []

    def update_status(msg: str):
        status_messages.append(msg)

    with st.spinner(f"⏳  Đang kết nối và tải dữ liệu từ {active_ota}..."):
        try:
            if active_ota == "Agoda":
                results = run_scrape(url=active_url, destination=active_destination, status_callback=update_status)
            else:
                results = run_scrape_tripcom(url=active_url, destination=active_destination, status_callback=update_status)
            st.session_state.scrape_results = results
        except Exception as e:
            st.error(f"❌  Lỗi khi chạy scraper: {str(e)}")
            results = []

    st.session_state.is_scraping = False

    if status_messages:
        status_class = "status-box status-box-agoda" if active_ota == "Agoda" else "status-box"
        with st.expander("📋  Nhật ký chi tiết"):
            for msg in status_messages:
                st.markdown(f"<div class='{status_class}'>• {msg}</div>", unsafe_allow_html=True)

    if results:
        st.success(f"✅  Hoàn tất! Đã thu thập **{len(results)}** khách sạn từ {active_ota}.")
    else:
        st.warning(f"⚠️  Không tìm thấy dữ liệu từ {active_ota}. Hãy thử lại sau.")

# ── Results ───────────────────────────────────────────────────────────────────
if st.session_state.scrape_results:
    results = st.session_state.scrape_results
    df = pd.DataFrame(results)
    active_destination = st.session_state.get("active_destination", "data")
    active_ota = st.session_state.get("active_ota", "Agoda")

    header_class = "result-header-agoda" if active_ota == "Agoda" else "result-header-tripcom"
    logo = "🟠" if active_ota == "Agoda" else "🔵"

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    st.markdown(f"""
    <div class="result-header {header_class}">
      <span style="font-size:1.5rem">{logo}</span>
      <h3>{active_ota} · {active_destination}</h3>
      <span class="result-badge">{len(df)} khách sạn</span>
    </div>
    """, unsafe_allow_html=True)

    # ── Metrics ──
    price_col = "Giá/đêm (chưa gồm thuế)" if active_ota == "Agoda" else "Giá/đêm (VND)"
    m_cols = st.columns(5)
    m_cols[0].metric("🏨 Tổng", len(df))
    m_cols[1].metric("💰 Có giá", df[price_col].astype(bool).sum() if price_col in df.columns else 0)
    m_cols[2].metric("⭐ Có sao", df["Hạng sao"].astype(bool).sum() if "Hạng sao" in df.columns else 0)
    if active_ota == "Agoda":
        m_cols[3].metric("🍳 Có bữa ăn", df["Gói bữa ăn"].astype(bool).sum() if "Gói bữa ăn" in df.columns else 0)
    else:
        m_cols[3].metric("🍳 Có bữa ăn", "N/A")
    m_cols[4].metric("🔓 Hủy miễn phí", df["Chính sách hoàn hủy"].str.contains("Hủy miễn phí", na=False).sum() if "Chính sách hoàn hủy" in df.columns else 0)

    # ── Filters ──
    st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
    st.markdown("#### 🔍  Tìm kiếm & Lọc")
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

    # ── Column config ──
    base_cfg = {
        "Tỉnh thành / Điểm đến": st.column_config.TextColumn("📍 Điểm đến", width="small"),
        "Tên khách sạn": st.column_config.TextColumn("🏨 Tên khách sạn", width="large"),
        "Địa chỉ": st.column_config.TextColumn("📌 Địa chỉ / Khu vực", width="medium"),
        "Hạng sao": st.column_config.TextColumn("⭐ Sao", width="small"),
        "Điểm đánh giá": st.column_config.TextColumn("📊 Đánh giá", width="small"),
        "Chính sách hoàn hủy": st.column_config.TextColumn("📋 Hủy", width="medium"),
        "Link khách sạn": st.column_config.LinkColumn("🔗 Link", width="small"),
    }
    if active_ota == "Agoda":
        base_cfg.update({
            "Địa điểm nổi bật": st.column_config.TextColumn("🗺️ Landmark", width="large"),
            "Gói bữa ăn": st.column_config.TextColumn("🍳 Bữa ăn", width="small"),
            "Giá/đêm (chưa gồm thuế)": st.column_config.TextColumn("💰 Giá (chưa thuế)", width="medium"),
            "Giá/đêm (đã gồm thuế)": st.column_config.TextColumn("💰 Giá (đã thuế)", width="medium"),
        })
    else:
        base_cfg["Giá/đêm (VND)"] = st.column_config.TextColumn("💰 Giá/đêm (VND)", width="medium")

    st.dataframe(fdf, use_container_width=True, height=480, column_config=base_cfg)

    # ── Export ──
    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    st.markdown("#### 📥  Xuất dữ liệu")
    dl1, dl2, dl3 = st.columns([2, 2, 1], gap="medium")

    with dl1:
        out = io.BytesIO()
        sheet = f"{active_ota} - {active_destination}"[:31]
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name=sheet)
            ws = writer.sheets[sheet]
            col_widths = [18, 45, 28, 45, 10, 18, 14, 18, 20, 30, 50]
            for i, w in enumerate(col_widths[:len(df.columns)]):
                from openpyxl.utils import get_column_letter
                ws.column_dimensions[get_column_letter(i + 1)].width = w
        out.seek(0)
        st.download_button(
            label="📊  Tải về Excel (.xlsx)", data=out.getvalue(),
            file_name=f"{active_ota}_{active_destination}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True, type="primary"
        )

    with dl2:
        csv_data = df.to_csv(index=False, encoding="utf-8-sig")
        st.download_button(
            label="📄  Tải về CSV (.csv)", data=csv_data.encode("utf-8-sig"),
            file_name=f"{active_ota}_{active_destination}.csv",
            mime="text/csv", use_container_width=True
        )

    with dl3:
        if st.button("🗑️  Xóa & tìm lại", use_container_width=True):
            st.session_state.scrape_results = None
            st.rerun()

st.markdown("""
<div class="footer">
  ⚠️ <strong>Lưu ý:</strong> Tool này chỉ dùng cho mục đích nghiên cứu thị trường.
  Hãy sử dụng có trách nhiệm và tuân thủ điều khoản của các OTA.
</div>
""", unsafe_allow_html=True)
