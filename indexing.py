import os, re, sys, io, json
import tempfile
import fitz  # PyMuPDF
import dropbox
import pytesseract
import hashlib
from PyPDF2 import PdfReader
from PIL import Image
from pdf2image import convert_from_bytes
from concurrent.futures import ThreadPoolExecutor
from sentence_transformers import SentenceTransformer
import streamlit as st
import pytesseract
from sshtunnel import SSHTunnelForwarder
import psycopg

SSH_HOST = st.secrets["ssh"]["SSH_HOST"]
SSH_PORT = st.secrets["ssh"]["SSH_PORT"]
SSH_USER = st.secrets["ssh"]["SSH_USER"]
SSH_PRIVATE_KEY = st.secrets["ssh"]["SSH_PRIVATE_KEY"]
# SSH_KEY = os.path.join(
#     os.environ["USERPROFILE"],
#     ".ssh",
#     st.secrets["ssh"]["SSH_KEY_PATH"]
# )
DB_NAME = st.secrets["database"]["DB_NAME"]
DB_USER = st.secrets["database"]["DB_USER"]
DB_PORT = st.secrets["database"]["DB_PORT"]
DB_HOST = st.secrets["database"]["DB_HOST"]
DB_PASSWORD = st.secrets["database"]["DB_PASSWORD"]

# Nếu Windows, set đường dẫn cụ thể nếu không trong PATH
if sys.platform.startswith("win"):
    pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

# ==============================
# 🔧 1. CONFIGURATION
# ==============================
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
OPENAI_API_KEY = st.secrets["openai"]["api_key"]
FOLDER_PATH = "/Apps/Document Brain/Agent"  
embedding_model = "text-embedding-3-large"
SENTENCE_TRANSFORMER_NAME = "BAAI/bge-large-en-v1.5"  # or bge-small if constrained

CHUNK_SIZE = 800 # 2000
CHUNK_OVERLAP = 120 # 150

# ========== MODEL LOADING (cached for streamlit) ==========
@st.cache_resource(show_spinner=False)
def load_embedding_model(model_name=SENTENCE_TRANSFORMER_NAME):
    return SentenceTransformer(model_name)

def get_dropbox_client():
    """
    Returns an authenticated Dropbox client.
    If no refresh token exists, runs OAuth flow to get one.
    """
    dbx = dropbox.Dropbox(
        oauth2_refresh_token=REFRESH_TOKEN,
        app_key=APP_KEY,
        app_secret=APP_SECRET,
    )

    # Otherwise, run OAuth flow to get new refresh token
    print("⚙️ No refresh token found. Starting Dropbox OAuth flow...")
    auth_flow = dropbox.DropboxOAuth2FlowNoRedirect(
        consumer_key=APP_KEY,
        consumer_secret=APP_SECRET,     
        token_access_type="offline"    
    )

    authorize_url = auth_flow.start()
    print("1️⃣ Go to this URL in your browser:")
    print(authorize_url)
    print("2️⃣ Click 'Allow' and copy the authorization code.")
    auth_code = input("3️⃣ Enter the code here: ").strip()

    oauth_result = auth_flow.finish(auth_code)

    # Save tokens locally
    config_data = {
        "refresh_token": oauth_result.refresh_token,
        "account_id": oauth_result.account_id,
    }
    with open(CONFIG_FILE, "w") as f:
        json.dump(config_data, f, indent=2)

    print(f"✅ Refresh token saved to {CONFIG_FILE}")
    dbx = dropbox.Dropbox(
        oauth2_refresh_token=oauth_result.refresh_token,
        app_key=APP_KEY,
        app_secret=APP_SECRET,
    )
    return dbx

def load_documents_from_streamlit(uploaded_file):
    """
    uploaded_file: streamlit UploadedFile
    """
    docs = []

    pdf_bytes = uploaded_file.read()
    filename = uploaded_file.name

    file_uid = hashlib.md5(filename.encode("utf-8")).hexdigest()[:10]

    pages = extract_pages(pdf_bytes)

    for page_obj in pages:
        page_num = page_obj["page"]
        text = page_obj["text"]

        chunks = smart_chunk_text(
            text,
            CHUNK_SIZE,
            CHUNK_OVERLAP
        )

        for idx, chunk in enumerate(chunks):
            bates_id = f"{file_uid}_{page_num:03d}_{idx:02d}"

            docs.append({
                "id": bates_id,
                "content": chunk,
                "metadata": {
                    "source": filename,
                    "path": filename,
                    "page": page_num,
                    "bates_id": bates_id,
                    "chunk_index": idx,
                    "chunk_chars": len(chunk),
                    "has_ocr": page_obj["has_ocr"],
                    "collection_id": "streamlit_upload"
                }
            })

    print(f"📄 Loaded {len(docs)} chunks from uploaded file: {filename}")
    return docs

def init_postgresql():
    """
        Create table to store metadata if it does not exist.

    """
    # --- write SSH key to temp file ---
    with tempfile.NamedTemporaryFile(delete=False) as key_file:
        key_file.write(SSH_PRIVATE_KEY.encode())
        ssh_key_path = key_file.name

    # --- SSH Tunnel ---
    tunnel = SSHTunnelForwarder(
        (SSH_HOST, SSH_PORT),
        ssh_username=SSH_USER,
        ssh_pkey=ssh_key_path,
        allow_agent=False,
        host_pkey_directories=[],
        remote_bind_address=(DB_HOST, DB_PORT),
    )
    
    tunnel.start()

    try:
        conn = psycopg.connect(
            host=DB_HOST,
            port=tunnel.local_bind_port,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            connect_timeout=5,
        )

        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS chunks (
                chunk_id TEXT PRIMARY KEY,
                filename TEXT,
                path TEXT,
                page INTEGER,
                chunk_index INTEGER,
                chunk_chars INTEGER,
                has_ocr INTEGER,
                collection_id TEXT,
                content TEXT
            )
            """)
            conn.commit()
            conn.close()
    finally:
        tunnel.stop()
        
def insert_metadata(docs):
    """
        Insert new metadata into SQLite, skipping existing chunks.
    """
    
    # --- write SSH key to temp file ---
    with tempfile.NamedTemporaryFile(delete=False) as key_file:
        key_file.write(SSH_PRIVATE_KEY.encode())
        ssh_key_path = key_file.name

    # --- SSH Tunnel ---
    tunnel = SSHTunnelForwarder(
        (SSH_HOST, SSH_PORT),
        ssh_username=SSH_USER,
        ssh_pkey=ssh_key_path,
        allow_agent=False,
        host_pkey_directories=[],
        remote_bind_address=(DB_HOST, DB_PORT),
    )
    
    tunnel.start()

    try:
        conn = psycopg.connect(
            host=DB_HOST,
            port=tunnel.local_bind_port,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            connect_timeout=5,
        )

        with conn.cursor() as cur:
            existing_ids = set(r[0] for r in cur.execute("SELECT chunk_id FROM chunks").fetchall())

            new_rows = []
            for d in docs:
                meta = d["metadata"]
                if meta["bates_id"] in existing_ids:
                    continue
                new_rows.append((
                    meta["bates_id"],
                    meta["source"],
                    meta["path"],
                    meta["page"],
                    meta["chunk_index"],
                    meta["chunk_chars"],
                    int(meta["has_ocr"]),
                    meta["collection_id"],
                    d["content"]
                ))

            cur.executemany("""
                INSERT INTO chunks (
                    chunk_id, filename, path, page, chunk_index, chunk_chars, has_ocr, collection_id, content
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (chunk_id) DO NOTHING
            """, new_rows)

            conn.commit()
            conn.close()
            print(f"💾 Saved {len(new_rows)} metadata entries to PostgreSQL.")
    
    finally:
        tunnel.stop()

_SPEAKER_RE = re.compile(r'^(MR|MS|MRS|DR)\.\s+([A-Z][A-Z\s\-]+):', re.I)

def clean_transcript_text(text: str) -> str:
    if not text:
        return ""

    lines = []

    for ln in text.splitlines():
        l = ln.strip()
        if not l:
            continue

        # bỏ header / footer thực sự
        if re.fullmatch(r'Page\s+\d+(\s+of\s+\d+)?', l, re.I):
            continue
        if re.fullmatch(r'\d+\s*/\s*\d+', l):
            continue

        # ✅ remove line number ở đầu dòng
        # "15 A. Text" → "A. Text"
        l = re.sub(r'^\d+\s+', '', l)

        # normalize Q / A
        if re.match(r'^(Q|Q\.|QUESTION)\b', l, re.I):
            l = re.sub(r'^(Q|Q\.|QUESTION)\b\.?\s*', '[Q] ', l, flags=re.I)
        elif re.match(r'^(A|A\.|ANSWER)\b', l, re.I):
            l = re.sub(r'^(A|A\.|ANSWER)\b\.?\s*', '[A] ', l, flags=re.I)

        # normalize speaker (MR. MILLER:)
        m = _SPEAKER_RE.match(l)
        if m:
            title, name = m.groups()
            l = f"[SPEAKER: {name.title()}] " + l[m.end():].strip()

        # tránh trường hợp còn lại chỉ là số
        if not l or l.isdigit():
            continue

        lines.append(l)

    return " ".join(lines)

# ========== SMART CHUNKER (sentence-accumulation) ==========
_SENTENCE_SPLIT_RE = re.compile(r'(?<=[\.\?\!\n])\s+')
def smart_chunk_text(text: str, chunk_size=CHUNK_SIZE, overlap=CHUNK_OVERLAP):
    if not text:
        return []
    sentences = _SENTENCE_SPLIT_RE.split(text)
    chunks = []
    cur = ""
    for s in sentences:
        if len(cur) + len(s) <= chunk_size:
            if cur:
                cur += " " + s
            else:
                cur = s
        else:
            # finalize current chunk
            if cur:
                chunks.append(cur.strip())
            # if sentence itself bigger than chunk_size, split it raw
            if len(s) > chunk_size:
                # fallback to raw slicing
                start = 0
                while start < len(s):
                    end = start + chunk_size
                    chunks.append(s[start:end].strip())
                    start = end - overlap
                cur = ""
            else:
                cur = s
    if cur:
        chunks.append(cur.strip())
    # add overlap by merging neighbors slightly to preserve context
    if overlap and len(chunks) > 1:
        merged = []
        for i, c in enumerate(chunks):
            if i == 0:
                merged.append(c)
            else:
                prev = merged[-1]
                # create overlap fragment from end of prev
                overlap_fragment = prev[-overlap:] if len(prev) > overlap else prev
                merged.append((overlap_fragment + " " + c).strip())
        chunks = merged
    return chunks

# ========== OCR HELPERS ==========
def ocr_image_bytes(img_bytes, lang="eng"):
    return pytesseract.image_to_string(Image.open(io.BytesIO(img_bytes)), lang=lang)

def ocr_pages_from_pdf_bytes(pdf_bytes, dpi=200, lang="eng", max_workers=4):
    images = convert_from_bytes(pdf_bytes, dpi=dpi)
    texts = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        results = list(ex.map(lambda im: pytesseract.image_to_string(im, lang=lang), images))
    return results

# ========== LOAD & PREPROCESS DOCUMENTS ==========
def _file_uid(entry):
    """
    Tạo ID duy nhất cho mỗi file dựa trên Dropbox path
    """
    return hashlib.md5(entry.path_lower.encode("utf-8")).hexdigest()[:10]

def extract_pages(pdf_bytes):
    doc = fitz.open(stream=io.BytesIO(pdf_bytes), filetype="pdf")
    pages = []

    for page_index in range(doc.page_count):
        page = doc[page_index]
        page_num = page_index + 1

        text = page.get_text("text") or ""

        if not text.strip():
            pix = page.get_pixmap(dpi=200)
            text = ocr_image_bytes(pix.tobytes("png"))

        text = clean_transcript_text(text)
        if not text:
            continue

        pages.append({
            "page": page_num,
            "text": text,
            "has_ocr": False
        })

    return pages

def load_documents_from_dropbox_v2():
    dbx = get_dropbox_client()
    response = dbx.files_list_folder(FOLDER_PATH, recursive=True)
    docs = []

    while True:
        for entry in response.entries:
            if not isinstance(entry, dropbox.files.FileMetadata):
                continue
            if not entry.name.lower().endswith(".pdf"):
                continue

            print(f"📄 Processing PDF: {entry.name}")

            _, res = dbx.files_download(entry.path_lower)
            pdf_bytes = res.content
            file_uid = hashlib.md5(entry.path_lower.encode()).hexdigest()[:10]

            pages = extract_pages(pdf_bytes)

            for page_obj in pages:
                page_num = page_obj["page"]
                text = page_obj["text"]

                chunks = smart_chunk_text(
                    text,
                    CHUNK_SIZE,
                    CHUNK_OVERLAP
                )

                for idx, chunk in enumerate(chunks):
                    bates_id = f"{file_uid}_{page_num:03d}_{idx:02d}"

                    docs.append({
                        "id": bates_id,
                        "content": chunk,
                        "metadata": {
                            "source": entry.name,
                            "path": entry.path_display,
                            "page": page_num,
                            "bates_id": bates_id,
                            "chunk_index": idx,
                            "chunk_chars": len(chunk),
                            "has_ocr": page_obj["has_ocr"],
                            "collection_id": os.path.basename(FOLDER_PATH)
                        }
                    })

        if not response.has_more:
            break
        response = dbx.files_list_folder_continue(response.cursor)

    print(f"Loaded {len(docs)} chunks from {len(set(d['metadata']['source'] for d in docs))} PDFs")
    return docs

def build_index(uploaded_file):
    docs = load_documents_from_streamlit(uploaded_file)

    if not docs:
        print("❌ No content found in uploaded PDF.")
        return

    texts = [doc["content"] for doc in docs]
    metadatas = [doc["metadata"] for doc in docs]

    # --- Lưu metadata vào SQLite ---
    init_postgresql()
    new_docs = [{"content": t, "metadata": m} for t, m in zip(texts, metadatas)]
    insert_metadata(new_docs)

    print(f"✅ Indexed {len(new_docs)} chunks from {len(set(d['metadata']['source'] for d in docs))} PDFs.")
   

