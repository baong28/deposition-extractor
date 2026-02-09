import fitz
import io, json, base64, os
import dropbox
import requests
import json
import streamlit as st

CONFIG_FILE = "config.json"
def load_local_config():
    """Load Dropbox credentials from local JSON file."""
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as f:
            data = json.load(f)
            return data
    else:
        return {}
local_cfg = load_local_config()
APP_KEY = st.secrets["dropbox"]["app_key"]
APP_SECRET = st.secrets["dropbox"]["app_secret"]
ACCESS_TOKEN = st.secrets["dropbox"]["access_token"]
REFRESH_TOKEN = st.secrets["dropbox"]["refresh_token"] or local_cfg.get("refresh_token")

def download_dropbox_pdf_raw(shared_link: str, access_token: str) -> bytes:
    url = "https://content.dropboxapi.com/2/sharing/get_shared_link_file"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Dropbox-API-Arg": json.dumps({
            "url": shared_link.split("?")[0]  # remove ?dl=1, rlkey...
        })
    }

    r = requests.post(url, headers=headers)

    # ⚠️ Không dùng r.json() vì response là BINARY
    if r.status_code != 200:
        raise RuntimeError(
            f"Dropbox error {r.status_code}\n"
            f"Headers: {r.headers}\n"
            f"Body (first 500 bytes): {r.content[:500]}"
        )

    return r.content

def get_runtime_access_token():
    dbx = dropbox.Dropbox(
        oauth2_refresh_token=REFRESH_TOKEN,
        app_key=APP_KEY,
        app_secret=APP_SECRET,
    )

    # 🔥 trigger refresh token -> access token
    dbx.users_get_current_account()

    return dbx._oauth2_access_token

def render_pdfjs_from_bytes(pdf_bytes: bytes, page: int = 1, height: int = 900):
    b64_pdf = base64.b64encode(pdf_bytes).decode("utf-8")

    html = f"""
    <iframe
        srcdoc="
        <!DOCTYPE html>
        <html>
        <head>
            <script src='https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.min.js'></script>
            <style>
                body {{ margin:0; background:#f9fafb; }}
                canvas {{ display:block; margin:auto; }}
            </style>
        </head>
        <body>
            <canvas id='pdf-canvas'></canvas>
            <script>
                const pdfData = atob('{b64_pdf}');
                const loadingTask = pdfjsLib.getDocument({{ data: pdfData }});
                loadingTask.promise.then(pdf => {{
                    pdf.getPage({page}).then(page => {{
                        const scale = 1.4;
                        const viewport = page.getViewport({{ scale }});
                        const canvas = document.getElementById('pdf-canvas');
                        const ctx = canvas.getContext('2d');
                        canvas.height = viewport.height;
                        canvas.width = viewport.width;
                        page.render({{
                            canvasContext: ctx,
                            viewport: viewport
                        }});
                    }});
                }});
            </script>
        </body>
        </html>
        "
        width="100%"
        height="{height}"
        style="border:1px solid #e5e7eb; border-radius:8px;"
    ></iframe>
    """

    st.components.v1.html(html, height=height, scrolling=True)

def extract_single_page_pdf(pdf_bytes: bytes, page_number: int) -> bytes:
    """
    page_number: 1-based (page 1 = first page)
    """
    src = fitz.open(stream=pdf_bytes, filetype="pdf")

    if page_number < 1 or page_number > src.page_count:
        raise ValueError("Invalid page number")

    dst = fitz.open()
    dst.insert_pdf(src, from_page=page_number - 1, to_page=page_number - 1)

    buffer = io.BytesIO()
    dst.save(buffer)
    dst.close()
    src.close()

    return buffer.getvalue()


