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
    .main-header { text-align: center; padding: 1rem 0; }
    .status-box {
        background-color: #f0f2f6;
        border-radius: 8px;
        padding: 10px 16px;
        font-size: 14px;
        color: #444;
        margin: 6px 0;
    }
</style>
""", unsafe_allow_html=True)

st.markdown("<div class='main-header'><h1>🏨 Agoda Hotel Scraper</h1><p>Công cụ cào dữ liệu khách sạn từ Agoda phục vụ nghiên cứu thị trường du lịch</p></div>", unsafe_allow_html=True)
st.markdown("---")

if "scrape_results" not in st.session_state:
    st.session_state.scrape_results = None
if "is_scraping" not in st.session_state:
    st.session_state.is_scraping = False

tab1, tab2 = st.tabs(["📋 Nhập cấu hình tìm kiếm", "🔗 Dán URL trực tiếp"])

with tab1:
    st.subheader("Cấu hình tìm kiếm khách sạn")

    destination_form = st.text_input(
        "🗺️ Tỉnh thành / Điểm đến",
        placeholder="VD: Hà Nội, Đà Nẵng, Hội An, Phú Quốc...",
        key="destination_form"
    )

    col3, col4 = st.columns(2)
    with col3:
        today = date.today()
        checkin_date = st.date_input(
            "📅 Ngày Check-in",
            value=today + timedelta(days=7),
            min_value=today,
            key="checkin_date"
        )
    with col4:
        checkout_date = st.date_input(
            "📅 Ngày Check-out",
            value=today + timedelta(days=8),
            min_value=today + timedelta(days=1),
            key="checkout_date"
        )

    col5, col6, col7 = st.columns(3)
    with col5:
        num_rooms = st.number_input("🛏️ Số phòng", min_value=1, max_value=10, value=1, key="num_rooms")
    with col6:
        num_adults = st.number_input("👤 Số người lớn", min_value=1, max_value=20, value=2, key="num_adults")
    with col7:
        num_children = st.number_input("👶 Số trẻ em", min_value=0, max_value=10, value=0, key="num_children")

    child_ages_form = []
    if num_children > 0:
        st.markdown("**Độ tuổi từng trẻ em:**")
        age_cols = st.columns(min(num_children, 5))
        for i in range(num_children):
            with age_cols[i % 5]:
                age = st.number_input(f"Trẻ em {i+1}", min_value=0, max_value=17, value=5, key=f"child_age_{i}")
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
        with st.expander("👁️ Xem URL được tạo"):
            st.code(url_preview, language="text")

    scrape_form = st.button(
        "🚀 Bắt đầu Scraping — Lấy toàn bộ kết quả",
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
    st.subheader("Nhập URL Agoda trực tiếp")
    st.info("💡 Truy cập Agoda, tìm kiếm khách sạn theo ý muốn, rồi copy toàn bộ URL và dán vào đây.")

    direct_url = st.text_area(
        "🔗 URL tìm kiếm Agoda",
        placeholder="https://www.agoda.com/search?city=...",
        height=100,
        key="direct_url"
    )

    dest_url_tab = st.text_input(
        "🗺️ Tên điểm đến (dùng để ghi vào dữ liệu)",
        placeholder="VD: Đà Nẵng",
        key="dest_url_tab"
    )

    scrape_url = st.button(
        "🚀 Bắt đầu Scraping — Lấy toàn bộ kết quả",
        disabled=not direct_url.strip() or not dest_url_tab.strip() or st.session_state.is_scraping,
        key="scrape_url_btn",
        use_container_width=True,
        type="primary"
    )

    if scrape_url and direct_url.strip():
        st.session_state["active_url"] = direct_url.strip()
        st.session_state["active_destination"] = dest_url_tab.strip() or "Không xác định"
        st.session_state["trigger_scrape"] = True

st.markdown("---")

if st.session_state.get("trigger_scrape"):
    st.session_state["trigger_scrape"] = False
    st.session_state.is_scraping = True
    st.session_state.scrape_results = None

    active_url = st.session_state.get("active_url", "")
    active_destination = st.session_state.get("active_destination", "")

    st.subheader("⏳ Đang chạy Scraper...")
    status_messages = []

    def update_status(msg: str):
        status_messages.append(msg)

    with st.spinner("Đang kết nối và tải toàn bộ dữ liệu từ Agoda..."):
        try:
            results = run_scrape(
                url=active_url,
                destination=active_destination,
                status_callback=update_status,
            )
            st.session_state.scrape_results = results
        except Exception as e:
            st.error(f"❌ Lỗi khi chạy scraper: {str(e)}")
            results = []

    st.session_state.is_scraping = False

    if status_messages:
        with st.expander("📋 Log chi tiết quá trình scraping"):
            for msg in status_messages:
                st.markdown(f"<div class='status-box'>• {msg}</div>", unsafe_allow_html=True)

    if results:
        st.success(f"✅ Scraping hoàn tất! Đã thu thập dữ liệu của **{len(results)}** khách sạn.")
    else:
        st.warning("⚠️ Không tìm thấy dữ liệu khách sạn. Có thể Agoda đã thay đổi cấu trúc trang hoặc bị chặn. Hãy thử lại sau.")

if st.session_state.scrape_results:
    results = st.session_state.scrape_results
    df = pd.DataFrame(results)
    active_destination = st.session_state.get("active_destination", "data")

    st.subheader(f"📊 Kết quả: {len(df)} khách sạn")

    col_m1, col_m2, col_m3, col_m4 = st.columns(4)
    with col_m1:
        st.metric("🏨 Tổng số khách sạn", len(df))
    with col_m2:
        st.metric("💰 Có dữ liệu giá", df["Giá thấp nhất (đã gồm thuế & phí)"].astype(bool).sum())
    with col_m3:
        st.metric("⭐ Có hạng sao", df["Hạng sao"].astype(bool).sum())
    with col_m4:
        st.metric("📋 Có chính sách hủy", df["Chính sách hoàn hủy"].astype(bool).sum())

    st.markdown("### 🔍 Preview dữ liệu")
    filter_col1, filter_col2 = st.columns(2)
    with filter_col1:
        search_text = st.text_input("🔎 Tìm kiếm theo tên khách sạn", placeholder="Nhập tên...", key="search_filter")
    with filter_col2:
        star_options = ["Tất cả"] + sorted(df["Hạng sao"].dropna().unique().tolist())
        selected_star = st.selectbox("⭐ Lọc theo hạng sao", star_options, key="star_filter")

    filtered_df = df.copy()
    if search_text:
        filtered_df = filtered_df[filtered_df["Tên khách sạn"].str.contains(search_text, case=False, na=False)]
    if selected_star != "Tất cả":
        filtered_df = filtered_df[filtered_df["Hạng sao"] == selected_star]

    st.dataframe(
        filtered_df,
        use_container_width=True,
        height=450,
        column_config={
            "Tỉnh thành / Điểm đến": st.column_config.TextColumn("📍 Điểm đến", width="small"),
            "Tên khách sạn": st.column_config.TextColumn("🏨 Tên khách sạn", width="large"),
            "Địa chỉ": st.column_config.TextColumn("📌 Địa chỉ", width="medium"),
            "Hạng sao": st.column_config.TextColumn("⭐ Hạng sao", width="small"),
            "Giá thấp nhất (đã gồm thuế & phí)": st.column_config.TextColumn("💰 Giá thấp nhất", width="medium"),
            "Meal Plan": st.column_config.TextColumn("🍽️ Meal Plan", width="medium"),
            "Chính sách hoàn hủy": st.column_config.TextColumn("📋 Chính sách hủy", width="medium"),
        }
    )

    st.markdown("### 📥 Tải xuống dữ liệu")
    dl_col1, dl_col2 = st.columns(2)

    with dl_col1:
        output_excel = io.BytesIO()
        with pd.ExcelWriter(output_excel, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Khách sạn Agoda")
            ws = writer.sheets["Khách sạn Agoda"]
            for col_letter, width in {"A": 20, "B": 40, "C": 30, "D": 12, "E": 25, "F": 25, "G": 25}.items():
                ws.column_dimensions[col_letter].width = width
        output_excel.seek(0)
        st.download_button(
            label="📥 Tải về Excel (.xlsx)",
            data=output_excel.getvalue(),
            file_name=f"agoda_{active_destination}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            type="primary"
        )

    with dl_col2:
        csv_data = df.to_csv(index=False, encoding="utf-8-sig")
        st.download_button(
            label="📥 Tải về CSV (.csv)",
            data=csv_data.encode("utf-8-sig"),
            file_name=f"agoda_{active_destination}.csv",
            mime="text/csv",
            use_container_width=True,
        )

    if st.button("🗑️ Xóa kết quả và tìm kiếm lại", use_container_width=True):
        st.session_state.scrape_results = None
        st.rerun()

st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #888; font-size: 12px;'>
⚠️ <strong>Lưu ý:</strong> Tool này chỉ dùng cho mục đích nghiên cứu thị trường.
Hãy sử dụng có trách nhiệm và tuân thủ điều khoản sử dụng của Agoda.
</div>
""", unsafe_allow_html=True)
