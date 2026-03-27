import streamlit as st
import pandas as pd
import io
from datetime import date, timedelta
from scraper import build_agoda_url, run_scrape

st.set_page_config(
    page_title="Agoda Hotel Scraper",
    page_icon="🏨",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
/* ── Global ── */
[data-testid="stAppViewContainer"] {
    background: #F0F4F8;
}
[data-testid="stHeader"] { background: transparent; }
[data-testid="stToolbar"] { display: none; }
.block-container { padding-top: 0 !important; max-width: 1200px; }

/* ── Hero header ── */
.hero {
    background: linear-gradient(135deg, #E53E1A 0%, #FF6B35 50%, #FF9A3C 100%);
    border-radius: 0 0 28px 28px;
    padding: 2.6rem 2rem 2rem;
    margin: -1rem -1rem 1.6rem -1rem;
    text-align: center;
    box-shadow: 0 4px 24px rgba(229,62,26,0.25);
}
.hero h1 {
    color: #fff !important;
    font-size: 2.6rem !important;
    font-weight: 800 !important;
    margin: 0 0 0.3rem !important;
    letter-spacing: -0.5px;
    text-shadow: 0 2px 8px rgba(0,0,0,0.15);
}
.hero p {
    color: rgba(255,255,255,0.9) !important;
    font-size: 1.05rem !important;
    margin: 0 !important;
    font-weight: 400;
}

/* ── Cards ── */
.card {
    background: #fff;
    border-radius: 16px;
    padding: 1.5rem 1.8rem;
    box-shadow: 0 2px 12px rgba(0,0,0,0.07);
    margin-bottom: 1.2rem;
    border: 1px solid #E8ECF0;
}
.card-title {
    font-size: 1.05rem;
    font-weight: 700;
    color: #1A1A2E;
    margin: 0 0 1rem;
    display: flex;
    align-items: center;
    gap: 0.5rem;
}
.section-label {
    font-size: 0.82rem;
    font-weight: 600;
    color: #6B7280;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin-bottom: 0.3rem;
}

/* ── Tabs ── */
[data-testid="stTabs"] [role="tablist"] {
    background: #fff;
    border-radius: 12px;
    padding: 4px;
    gap: 4px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.06);
    border: 1px solid #E8ECF0;
}
[data-testid="stTabs"] button[role="tab"] {
    border-radius: 9px !important;
    font-weight: 600 !important;
    font-size: 0.9rem !important;
    padding: 0.5rem 1.2rem !important;
    color: #6B7280 !important;
    transition: all 0.2s;
}
[data-testid="stTabs"] button[role="tab"][aria-selected="true"] {
    background: linear-gradient(135deg, #E53E1A, #FF6B35) !important;
    color: #fff !important;
    box-shadow: 0 2px 8px rgba(229,62,26,0.35) !important;
}
[data-testid="stTabs"] [role="tabpanel"] {
    padding-top: 0 !important;
}

/* ── Inputs ── */
[data-testid="stTextInput"] input,
[data-testid="stDateInput"] input,
[data-testid="stNumberInput"] input,
[data-testid="stTextArea"] textarea {
    border-radius: 10px !important;
    border: 1.5px solid #E2E8F0 !important;
    background: #FAFBFC !important;
    transition: border-color 0.2s;
}
[data-testid="stTextInput"] input:focus,
[data-testid="stTextArea"] textarea:focus {
    border-color: #E53E1A !important;
    box-shadow: 0 0 0 3px rgba(229,62,26,0.12) !important;
}

/* ── Primary button ── */
[data-testid="stButton"] button[kind="primary"],
.stDownloadButton button[kind="primary"] {
    background: linear-gradient(135deg, #E53E1A, #FF6B35) !important;
    border: none !important;
    border-radius: 12px !important;
    font-weight: 700 !important;
    font-size: 1rem !important;
    padding: 0.7rem 1.5rem !important;
    box-shadow: 0 4px 14px rgba(229,62,26,0.35) !important;
    transition: transform 0.15s, box-shadow 0.15s !important;
    letter-spacing: 0.01em;
}
[data-testid="stButton"] button[kind="primary"]:hover {
    transform: translateY(-1px) !important;
    box-shadow: 0 6px 20px rgba(229,62,26,0.45) !important;
}
[data-testid="stButton"] button[kind="secondary"] {
    border-radius: 10px !important;
    border: 1.5px solid #E2E8F0 !important;
    font-weight: 600 !important;
    color: #374151 !important;
}

/* ── Metric cards ── */
[data-testid="stMetric"] {
    background: #fff;
    border-radius: 14px;
    padding: 1rem 1.2rem !important;
    box-shadow: 0 2px 10px rgba(0,0,0,0.06);
    border: 1px solid #E8ECF0;
}
[data-testid="stMetricLabel"] { font-size: 0.78rem !important; font-weight: 600 !important; color: #6B7280 !important; }
[data-testid="stMetricValue"] { font-size: 1.9rem !important; font-weight: 800 !important; color: #1A1A2E !important; }

/* ── Dataframe ── */
[data-testid="stDataFrame"] {
    border-radius: 14px !important;
    overflow: hidden;
    border: 1px solid #E8ECF0 !important;
    box-shadow: 0 2px 10px rgba(0,0,0,0.05);
}

/* ── Alerts ── */
[data-testid="stAlert"] {
    border-radius: 12px !important;
    border: none !important;
}

/* ── Status box ── */
.status-box {
    background: #F8F9FA;
    border-left: 3px solid #E53E1A;
    border-radius: 0 8px 8px 0;
    padding: 7px 14px;
    font-size: 0.85rem;
    color: #374151;
    margin: 4px 0;
    font-family: monospace;
}

/* ── Footer ── */
.footer {
    text-align: center;
    color: #9CA3AF;
    font-size: 0.8rem;
    padding: 1.5rem 0 0.5rem;
    border-top: 1px solid #E8ECF0;
    margin-top: 1rem;
}

/* ── Info badge ── */
.info-badge {
    display: inline-block;
    background: #FFF4EE;
    color: #E53E1A;
    border: 1px solid #FFD5C2;
    border-radius: 8px;
    padding: 0.55rem 1rem;
    font-size: 0.88rem;
    font-weight: 500;
    margin-bottom: 1rem;
    width: 100%;
}

/* ── Expander ── */
[data-testid="stExpander"] {
    border-radius: 12px !important;
    border: 1px solid #E8ECF0 !important;
    background: #fff;
}

/* ── Result header ── */
.result-header {
    display: flex;
    align-items: center;
    gap: 0.7rem;
    background: linear-gradient(135deg, #1A1A2E, #2D3561);
    color: #fff;
    border-radius: 14px;
    padding: 1rem 1.5rem;
    margin-bottom: 1.2rem;
    box-shadow: 0 2px 12px rgba(26,26,46,0.2);
}
.result-header h3 { margin: 0; font-size: 1.2rem; font-weight: 700; color: #fff; }
.result-badge {
    background: #E53E1A;
    color: #fff;
    border-radius: 20px;
    padding: 0.2rem 0.8rem;
    font-size: 0.9rem;
    font-weight: 700;
    margin-left: auto;
}

/* ── Download buttons ── */
.stDownloadButton button {
    border-radius: 12px !important;
    font-weight: 700 !important;
    padding: 0.65rem 1.5rem !important;
    transition: transform 0.15s !important;
}
.stDownloadButton button:hover { transform: translateY(-1px) !important; }

/* ── Spinner ── */
[data-testid="stSpinner"] > div {
    border-top-color: #E53E1A !important;
}
</style>
""", unsafe_allow_html=True)

# ── Hero header ──────────────────────────────────────────────────────────────
st.markdown("""
<div class="hero">
  <h1>🏨 Agoda Hotel Scraper</h1>
  <p>Công cụ thu thập dữ liệu khách sạn từ Agoda · Phân tích thị trường du lịch · Xuất Excel / CSV</p>
</div>
""", unsafe_allow_html=True)

# ── Session state ─────────────────────────────────────────────────────────────
if "scrape_results" not in st.session_state:
    st.session_state.scrape_results = None
if "is_scraping" not in st.session_state:
    st.session_state.is_scraping = False

# ── Input tabs ────────────────────────────────────────────────────────────────
tab1, tab2 = st.tabs(["📋  Nhập cấu hình tìm kiếm", "🔗  Dán URL trực tiếp"])

with tab1:
    st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

    with st.container():
        st.markdown("<div class='section-label'>📍 Điểm đến</div>", unsafe_allow_html=True)
        destination_form = st.text_input(
            "Điểm đến",
            placeholder="VD: Hà Nội, Đà Nẵng, Hội An, Phú Quốc, Nha Trang...",
            key="destination_form",
            label_visibility="collapsed"
        )

    col3, col4 = st.columns(2, gap="medium")
    with col3:
        st.markdown("<div class='section-label'>📅 Ngày Check-in</div>", unsafe_allow_html=True)
        today = date.today()
        checkin_date = st.date_input(
            "Check-in", value=today + timedelta(days=7),
            min_value=today, key="checkin_date", label_visibility="collapsed"
        )
    with col4:
        st.markdown("<div class='section-label'>📅 Ngày Check-out</div>", unsafe_allow_html=True)
        checkout_date = st.date_input(
            "Check-out", value=today + timedelta(days=8),
            min_value=today + timedelta(days=1), key="checkout_date", label_visibility="collapsed"
        )

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
                age = st.number_input(f"Trẻ {i+1}", min_value=0, max_value=17, value=5, key=f"child_age_{i}")
                child_ages_form.append(age)

    if checkin_date >= checkout_date:
        st.error("⚠️ Ngày Check-out phải sau ngày Check-in!")
        btn_disabled_form = True
    else:
        btn_disabled_form = False

    url_preview = ""
    if destination_form and not btn_disabled_form:
        url_preview = build_agoda_url(
            destination=destination_form,
            check_in=checkin_date.strftime("%Y-%m-%d"),
            check_out=checkout_date.strftime("%Y-%m-%d"),
            rooms=num_rooms,
            adults=num_adults,
            children=num_children,
            child_ages=child_ages_form
        )
        with st.expander("👁️ Xem URL sẽ được scrape"):
            st.code(url_preview, language="text")

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
    scrape_form = st.button(
        "🚀  Bắt đầu thu thập dữ liệu",
        disabled=btn_disabled_form or not destination_form or st.session_state.is_scraping,
        key="scrape_form_btn",
        use_container_width=True,
        type="primary"
    )

    if scrape_form and url_preview:
        st.session_state["active_url"] = url_preview
        st.session_state["active_destination"] = destination_form
        st.session_state["trigger_scrape"] = True

with tab2:
    st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
    st.markdown("""
    <div class="info-badge">
      💡 Truy cập Agoda, tìm kiếm khách sạn theo ý muốn, rồi copy toàn bộ URL thanh địa chỉ và dán vào đây.
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<div class='section-label'>🔗 URL tìm kiếm Agoda</div>", unsafe_allow_html=True)
    direct_url = st.text_area(
        "URL", placeholder="https://www.agoda.com/search?city=...",
        height=90, key="direct_url", label_visibility="collapsed"
    )

    st.markdown("<div class='section-label'>🗺️ Tên điểm đến (ghi vào dữ liệu xuất)</div>", unsafe_allow_html=True)
    dest_url_tab = st.text_input(
        "Điểm đến", placeholder="VD: Đà Nẵng",
        key="dest_url_tab", label_visibility="collapsed"
    )

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
    scrape_url = st.button(
        "🚀  Bắt đầu thu thập dữ liệu",
        disabled=not direct_url.strip() or not dest_url_tab.strip() or st.session_state.is_scraping,
        key="scrape_url_btn",
        use_container_width=True,
        type="primary"
    )

    if scrape_url and direct_url.strip():
        pasted = direct_url.strip()
        if "currency=VND" not in pasted:
            sep = "&" if "?" in pasted else "?"
            pasted += f"{sep}currency=VND&currencyCode=VND&priceCur=VND"
        st.session_state["active_url"] = pasted
        st.session_state["active_destination"] = dest_url_tab.strip() or "Không xác định"
        st.session_state["trigger_scrape"] = True

# ── Scraping ──────────────────────────────────────────────────────────────────
if st.session_state.get("trigger_scrape"):
    st.session_state["trigger_scrape"] = False
    st.session_state.is_scraping = True
    st.session_state.scrape_results = None

    active_url = st.session_state.get("active_url", "")
    active_destination = st.session_state.get("active_destination", "")

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    status_messages = []

    def update_status(msg: str):
        status_messages.append(msg)

    with st.spinner("⏳  Đang kết nối và tải toàn bộ dữ liệu từ Agoda..."):
        try:
            results = run_scrape(
                url=active_url,
                destination=active_destination,
                status_callback=update_status,
            )
            st.session_state.scrape_results = results
        except Exception as e:
            st.error(f"❌  Lỗi khi chạy scraper: {str(e)}")
            results = []

    st.session_state.is_scraping = False

    if status_messages:
        with st.expander("📋  Nhật ký chi tiết quá trình scraping"):
            for msg in status_messages:
                st.markdown(f"<div class='status-box'>• {msg}</div>", unsafe_allow_html=True)

    if results:
        st.success(f"✅  Hoàn tất! Đã thu thập **{len(results)}** khách sạn.")
    else:
        st.warning("⚠️  Không tìm thấy dữ liệu. Agoda có thể đã thay đổi cấu trúc hoặc bị chặn. Hãy thử lại sau.")

# ── Results ───────────────────────────────────────────────────────────────────
if st.session_state.scrape_results:
    results = st.session_state.scrape_results
    df = pd.DataFrame(results)
    active_destination = st.session_state.get("active_destination", "data")

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    st.markdown(f"""
    <div class="result-header">
      <span style="font-size:1.5rem">📊</span>
      <h3>Kết quả scraping — {active_destination}</h3>
      <span class="result-badge">{len(df)} khách sạn</span>
    </div>
    """, unsafe_allow_html=True)

    col_m1, col_m2, col_m3, col_m4, col_m5 = st.columns(5)
    with col_m1:
        st.metric("🏨 Tổng khách sạn", len(df))
    with col_m2:
        price_col = "Giá/đêm (chưa gồm thuế)"
        st.metric("💰 Có giá", df[price_col].astype(bool).sum() if price_col in df.columns else 0)
    with col_m3:
        st.metric("⭐ Có hạng sao", df["Hạng sao"].astype(bool).sum() if "Hạng sao" in df.columns else 0)
    with col_m4:
        st.metric("🍳 Có bữa ăn", df["Gói bữa ăn"].astype(bool).sum() if "Gói bữa ăn" in df.columns else 0)
    with col_m5:
        st.metric("🔓 Hủy miễn phí", df["Chính sách hoàn hủy"].str.contains("Hủy miễn phí", na=False).sum() if "Chính sách hoàn hủy" in df.columns else 0)

    st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
    st.markdown("#### 🔍  Tìm kiếm & Lọc")
    filter_col1, filter_col2, filter_col3 = st.columns([2, 1, 1], gap="medium")
    with filter_col1:
        search_text = st.text_input("Tìm theo tên khách sạn", placeholder="🔎  Nhập tên khách sạn...", key="search_filter")
    with filter_col2:
        star_options = ["Tất cả"] + sorted([s for s in df["Hạng sao"].dropna().unique().tolist() if s])
        selected_star = st.selectbox("⭐  Hạng sao", star_options, key="star_filter")
    with filter_col3:
        meal_options = ["Tất cả", "Có bữa ăn", "Không có bữa ăn"]
        selected_meal = st.selectbox("🍳  Bữa ăn", meal_options, key="meal_filter")

    filtered_df = df.copy()
    if search_text:
        filtered_df = filtered_df[filtered_df["Tên khách sạn"].str.contains(search_text, case=False, na=False)]
    if selected_star != "Tất cả":
        filtered_df = filtered_df[filtered_df["Hạng sao"] == selected_star]
    if selected_meal == "Có bữa ăn":
        filtered_df = filtered_df[filtered_df["Gói bữa ăn"].astype(bool)]
    elif selected_meal == "Không có bữa ăn":
        filtered_df = filtered_df[~filtered_df["Gói bữa ăn"].astype(bool)]

    if len(filtered_df) < len(df):
        st.caption(f"Hiển thị {len(filtered_df)} / {len(df)} khách sạn sau khi lọc")

    col_cfg = {
        "Tỉnh thành / Điểm đến": st.column_config.TextColumn("📍 Điểm đến", width="small"),
        "Tên khách sạn": st.column_config.TextColumn("🏨 Tên khách sạn", width="large"),
        "Địa chỉ": st.column_config.TextColumn("📌 Địa chỉ", width="medium"),
        "Địa điểm nổi bật": st.column_config.TextColumn("🗺️ Địa điểm gần", width="large"),
        "Hạng sao": st.column_config.TextColumn("⭐ Sao", width="small"),
        "Điểm đánh giá": st.column_config.TextColumn("📊 Đánh giá", width="small"),
        "Gói bữa ăn": st.column_config.TextColumn("🍳 Bữa ăn", width="small"),
        "Giá/đêm (chưa gồm thuế)": st.column_config.TextColumn("💰 Giá (chưa thuế)", width="medium"),
        "Giá/đêm (đã gồm thuế)": st.column_config.TextColumn("💰 Giá (đã thuế)", width="medium"),
        "Chính sách hoàn hủy": st.column_config.TextColumn("📋 Hủy", width="medium"),
        "Link khách sạn": st.column_config.LinkColumn("🔗 Link", width="small"),
    }

    st.dataframe(filtered_df, use_container_width=True, height=480, column_config=col_cfg)

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    st.markdown("#### 📥  Xuất dữ liệu")
    dl_col1, dl_col2, dl_col3 = st.columns([2, 2, 1], gap="medium")

    with dl_col1:
        output_excel = io.BytesIO()
        with pd.ExcelWriter(output_excel, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Khách sạn Agoda")
            ws = writer.sheets["Khách sạn Agoda"]
            for col_letter, width in {"A": 18, "B": 45, "C": 28, "D": 45, "E": 10, "F": 18, "G": 14, "H": 18, "I": 20, "J": 30, "K": 50}.items():
                ws.column_dimensions[col_letter].width = width
        output_excel.seek(0)
        st.download_button(
            label="📊  Tải về Excel (.xlsx)",
            data=output_excel.getvalue(),
            file_name=f"agoda_{active_destination}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            type="primary"
        )

    with dl_col2:
        csv_data = df.to_csv(index=False, encoding="utf-8-sig")
        st.download_button(
            label="📄  Tải về CSV (.csv)",
            data=csv_data.encode("utf-8-sig"),
            file_name=f"agoda_{active_destination}.csv",
            mime="text/csv",
            use_container_width=True,
        )

    with dl_col3:
        if st.button("🗑️  Xóa & tìm lại", use_container_width=True):
            st.session_state.scrape_results = None
            st.rerun()

st.markdown("""
<div class="footer">
  ⚠️ <strong>Lưu ý:</strong> Tool này chỉ dùng cho mục đích nghiên cứu thị trường.
  Hãy sử dụng có trách nhiệm và tuân thủ điều khoản của Agoda.
</div>
""", unsafe_allow_html=True)
