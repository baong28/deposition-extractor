
from indexing import build_index
from pdf_utils import *
from db_utils import *
import time
import pandas as pd
import streamlit as st

st.set_page_config(
    layout="wide",
    page_icon=":balance_scale:",
    page_title="Legal Deposition Issue Extractor",
    initial_sidebar_state ="expanded",
    menu_items={
         'Get Help': 'https://www.extremelycoolapp.com/help',
         'Report a bug': "https://www.extremelycoolapp.com/bug",
         'About': "# This is a header. This is an *extremely* cool app!"
     }
)

st.markdown("""
    <style>
        html, body, [class*="css"] {
            font-family: "Inter", "Helvetica Neue", Arial, sans-serif;
        }

        .block-container {
            padding-top: 3rem;
            padding-left: 4rem;
            padding-right: 4rem;
        }

        .header {
            font-size: 28px;
            font-weight: 600;
            margin-bottom: 4px;
        }

        .subheader {
            color: #6b7280;
            font-size: 14px;
            margin-bottom: 24px;
        }

        .panel {
            background-color: #f9fafb;
            border: 1px solid #e5e7eb;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 24px;
        }

        .status {
            font-size: 14px;
            color: #374151;
        }

        .success {
            color: #065f46;
            font-weight: 500;
        }

        .divider {
            border-top: 1px solid #e5e7eb;
            margin: 24px 0;
        }
    
        .left-scroll {
            height: calc(100vh - 180px); 
            overflow-y: auto;
            padding-right: 12px;
        }

        .left-scroll::-webkit-scrollbar {
            width: 6px;
        }
        .left-scroll::-webkit-scrollbar-thumb {
            background-color: #d1d5db;
            border-radius: 6px;
        }
    
        .left-scroll h3 {
            position: sticky;
            top: 0;
            background: #fff;
            z-index: 10;
            padding-bottom: 8px;
        }
    </style>
    """, unsafe_allow_html=True
)

# ---------- SESSION STATE ----------
if "selected_files" not in st.session_state:
    st.session_state.selected_files = []

if "active_tab" not in st.session_state:
    st.session_state.active_tab = "index"

if "review_file" not in st.session_state:
    st.session_state.review_file = None

if "uploaded_file" not in st.session_state:
    st.session_state.uploaded_file = None

if "extracted" not in st.session_state:
    st.session_state.extracted = False

if "selected_page" not in st.session_state:
    st.session_state.selected_page = None

if "pdf_bytes" not in st.session_state:
    st.session_state.pdf_bytes = None

if "download_page" not in st.session_state:
    st.session_state.download_page = None

if "download_pdf_link" not in st.session_state:
    st.session_state.download_pdf_link  = None

if "confirm_download" not in st.session_state:
    st.session_state.confirm_download = {}

   #📄 
# ---------- Tabs ----------
tab_index, tab_review = st.tabs([
    "📥 Upload & Index",
    "🔎 Review Extracted Deposition"
])

# ---------- TAB 1 — Upload & Index ----------
with tab_index:
    st.subheader("Upload deposition transcript")

    uploaded = st.file_uploader(
        "Upload deposition transcript (PDF)",
        type=["pdf"],
        label_visibility="collapsed"
    )

# if uploaded and not st.session_state.index_done:
#     st.session_state.uploaded_file = uploaded

    if uploaded:
        st.markdown(
            f'<div class="status"><strong>File:</strong> {uploaded.name}</div>',
            unsafe_allow_html=True
        )
        if st.button("🚀 Load file"):
            with st.spinner("Indexing document..."):
                progress = st.progress(0)

                time.sleep(0.3)
                progress.progress(30)

                # 🔥 AUTO RUN ON UPLOAD
                build_index(uploaded)

                time.sleep(0.3)
                progress.progress(100)

            st.session_state.index_done = True
            st.markdown(
                '<div class="status success">Indexing completed</div>',
                unsafe_allow_html=True
    )

# ---------- TAB 2 — Review & Extract ----------
with tab_review:
    # st.subheader("Review indexed files")
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    file_stats = get_file_stats()

    if not file_stats:
        st.warning("No indexed files found.")
    else:
        filenames = list(file_stats.keys())

        # -------- SELECT FILE TO REVIEW --------
        review_file = st.selectbox(
            "Select a file to review extracted issues:",
            options=["— Select a file —"] + filenames
        )

        col1, col2 = st.columns([1, 3])
        with col1:
            review_clicked = st.button(
                "Review Extracted Issues", # 👁️ 
                type='primary',
                use_container_width=False,
                
            )

        if review_clicked and review_file != "— Select a file —":
            st.session_state.review_file = review_file
            st.session_state.extracted = True

        # -------- REVIEW PANEL --------
        if st.session_state.extracted and st.session_state.review_file:
            #st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
            #st.subheader("🔍 Extracted Issues Review")

            rows = get_extracted_issues([st.session_state.review_file])

            if not rows:
                st.info("No extracted issues found for this file.")
            else:
                df = pd.DataFrame(
                    rows,
                    columns=[
                        "issue_id",
                        "chunk_id",
                        "filename",
                        "page",
                        "speaker",
                        "risk",
                        "legal_relevance",
                        "quoted_text",
                        "issue_type",
                        "pdf_link"
                    ]
                )

                risk_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
                df["risk_rank"] = df["risk"].map(risk_order)
                df = df.sort_values(["risk_rank", "page"])

                # ---- Filters ----
                col1, col2, col3 = st.columns(3)

                with col1:
                    risk_filter = st.multiselect(
                        "Risk Level",
                        options=sorted(df["risk"].unique())
                    )

                with col2:
                    issue_filter = st.multiselect(
                        "Issue Type",
                        options=sorted(df["issue_type"].unique())
                    )

                with col3:
                    page_filter = st.multiselect(
                        "Page",
                        options=sorted(df["page"].unique())
                    )

                if risk_filter:
                    df = df[df["risk"].isin(risk_filter)]
                if issue_filter:
                    df = df[df["issue_type"].isin(issue_filter)]
                if page_filter:
                    df = df[df["page"].isin(page_filter)]

                def risk_badge(risk):
                    risk = str(risk).upper()

                    colors = {
                        "HIGH": "#fee2e2",
                        "MEDIUM": "#fef3c7",
                        "LOW": "#ecfeff"
                    }
                    text = {
                        "HIGH": "#991b1b",
                        "MEDIUM": "#92400e",
                        "LOW": "#065f46"
                    }

                    if risk not in colors:
                        return f"<span>{risk}</span>"

                    return f"""
                    <span style="
                        background:{colors[risk]};
                        color:{text[risk]};
                        padding:4px 10px;
                        border-radius:6px;
                        font-size:12px;
                        font-weight:600;
                    ">
                        {risk}
                    </span>
                    """
                    
                col_left, col_right = st.columns([2, 3])

                # ---------- LEFT: ISSUES ----------
                with col_left:
                    st.markdown("""
                        <div style="
                            position: sticky;
                            top: 0;
                            background: white;
                            z-index: 10;
                            padding-bottom: 8px;
                        ">
                            <h3>📁 Extracted Issues</h3>
                        </div>
                    """, unsafe_allow_html=True)

                    scroll_box = st.container(height=900) 

                    with scroll_box:
                        for _, row in df.iterrows():
                            page = int(row["page"])
                            pdf_link = row["pdf_link"].split("?")[0]

                            header = (
                                f"<div style='display:flex; align-items:center; gap:8px;'>"
                                f"{risk_badge(row['risk'])}"
                                f"<strong>Page.{row['page']}</strong>"
                                f"<span>– {row['issue_type']}</span>"
                                f"</div>"
                            )

                            with st.expander("Click to view details", expanded=False):
                                st.markdown(
                                    f"<div style='margin-bottom:8px'>{header}</div>",
                                    unsafe_allow_html=True
                                )

                                st.markdown("**Quoted testimony**")
                                st.code(row["quoted_text"], language="text")

                                st.markdown("**Legal relevance**")
                                st.write(row["legal_relevance"])

                                if st.button(
                                    "👁 View in PDF",
                                    key=f"view_pdf_{row['issue_id']}",
                                    use_container_width=True
                                ):
                                    st.session_state.selected_page = page
                                    st.session_state.current_pdf_link = pdf_link

                     
                # ---------- RIGHT: PDF ----------
                with col_right:
                    for _, row in df.iterrows():
                        page = int(row["page"])
                        pdf_link = row["pdf_link"]
                        pdf_page_url = f"#page={pdf_link}" + f"#page={page}"
                        issue_key = str(row["issue_id"])

                    # init state cho issue này
                    if issue_key not in st.session_state.confirm_download:
                        st.session_state.confirm_download[issue_key] = False
                                                                   
                    st.markdown("""
                        <div style="
                            position: sticky;
                            top: 0;
                            background: white;
                            z-index: 10;
                            padding-bottom: 8px;
                        ">
                            <h3>📄 Deposition Transcript</h3>
                        </div>
                    """, unsafe_allow_html=True)
                    
                    if st.session_state.selected_page is None:
                        st.info("👈 Select an issue to view the corresponding PDF page")
                    else:
                        page = st.session_state.selected_page

                        if st.session_state.pdf_bytes is None:
                            with st.spinner("Loading PDF from Dropbox..."):
                                access_token = get_runtime_access_token()
                                st.session_state.pdf_bytes = download_dropbox_pdf_raw(
                                    pdf_link,
                                    access_token
                                )
                        
                        render_pdfjs_from_bytes(
                            st.session_state.pdf_bytes,
                            page=page,
                            height=900
                        )      
                                     
                        # ----- CONFIRM BUTTON -----
                        if not st.session_state.confirm_download[issue_key]:
                            if st.button(
                                "Confirm to download?",
                                key=f"confirm_{issue_key}",
                                use_container_width=True
                            ):
                                st.session_state.confirm_download[issue_key] = True
                                st.session_state.download_page = page
                                st.session_state.download_pdf_link = row["pdf_link"]

                        # ----- DOWNLOAD BUTTON -----
                        else:
                            page_to_download = st.session_state.download_page

                            if page_to_download is not None:
                                single_page_pdf = extract_single_page_pdf(
                                    st.session_state.pdf_bytes,
                                    page_to_download
                                )

                                st.download_button(
                                    label=f"⬇️ Download Page {page_to_download}",
                                    data=single_page_pdf,
                                    file_name=(
                                        f"{row['filename']}"
                                        f"_page_{page_to_download}"
                                        f"_{row['issue_type']}.pdf"
                                    ),
                                    mime="application/pdf",
                                    use_container_width=True
                                )

                            if st.button("↩ Cancel", key=f"cancel_{issue_key}"):
                                st.session_state.confirm_download[issue_key] = False
                                st.session_state.download_page = None

                        
