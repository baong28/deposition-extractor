import json
import os, re, sys
import io
import pickle
import faiss
import fitz  # PyMuPDF
import dropbox
import pytesseract
from PyPDF2 import PdfReader
import torch
from PIL import Image
from pdf2image import convert_from_bytes
from concurrent.futures import ThreadPoolExecutor
from sentence_transformers import SentenceTransformer
import streamlit as st
import pytesseract
import sqlite3
import hashlib

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
FAISS_DIR = "data/faiss_store"
INDEX_PATH = os.path.join(FAISS_DIR, "index.faiss")
META_PATH = os.path.join(FAISS_DIR, "metadata.pkl")
SQLITE_DB_PATH = os.path.join(FAISS_DIR, "metadata.db")
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

def init_sqlite():
    """
        Create table to store metadata if it does not exist.

    """
    os.makedirs(FAISS_DIR, exist_ok=True)
    conn = sqlite3.connect(SQLITE_DB_PATH)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS chunks_v2 (
        id TEXT PRIMARY KEY,
        filename TEXT,
        path TEXT,
        page INTEGER,
        chunk_index INTEGER,
        chunk_chars INTEGER,
        has_ocr INTEGER,
        collection_id TEXT,
        content TEXT,
        pdf_link TEXT
    )
    """)
    conn.commit()
    conn.close()

def insert_metadata(docs):
    """
        Insert new metadata into SQLite, skipping existing chunks.
    """
    conn = sqlite3.connect(SQLITE_DB_PATH)
    cur = conn.cursor()
    existing_ids = set(r[0] for r in cur.execute("SELECT id FROM chunks_v2").fetchall())

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
            d["content"],
            meta["pdf_link"]
        ))

    cur.executemany("""
        INSERT OR IGNORE INTO chunks_v2 (
            id, filename, path, page, chunk_index, chunk_chars, has_ocr, collection_id, content, pdf_link
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, new_rows)

    conn.commit()
    conn.close()
    print(f"💾 Saved {len(new_rows)} metadata entries to SQLite.")

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

def clean_page_text(text: str) -> str:
    """
    Làm sạch nội dung trang PDF, loại bỏ header/footer, số trang, watermark, ký tự nhiễu.
    Áp dụng được cho đa dạng loại tài liệu (academic, legal, technical, OCR, v.v.)
    """
    if not text:
        return ""
    
    # Chuẩn hóa ký tự trắng & xuống dòng
    text = text.replace('\xa0', ' ').replace('\t', ' ')
    text = re.sub(r'\s+', ' ', text).strip()

    # Preserve line breaks to detect header/footer lines, then normalize
    lines = [ln.strip() for ln in text.splitlines()]
    cleaned_lines = []

    for line in lines:
        l = line.strip()
        if not l or len(l) < 3:
            continue
        # -----------------------------
        # 🔹 Loại bỏ header/footer phổ biến
        # -----------------------------
        if re.match(r'^(page|p\.)\s*\d+(\s*of\s*\d+)?$', l, re.I): continue
        if re.match(r'^\d+\s*/\s*\d+$', l): continue
        if re.match(r'^\d{1,3}$', l): continue
        if re.search(r'\bdoi\.org/\S+', l, re.I): continue
        if re.search(r'\bISSN\b|\bISBN\b|\bjournal\b|\bmanuscript\b', l, re.I): continue
        if re.search(r'©\s*\d{4}', l) or re.search(r'copyright', l, re.I): continue
        if re.search(r'www\.|http[s]?://', l, re.I): continue
        if re.search(r'(university|faculty|institute|department|school of)', l, re.I): continue
        if re.search(r'(int\.|journal|conference|proceedings|res\.)', l, re.I):
            if len(l.split()) < 10: continue
        if re.search(r'(exhibit|deposition|confidential|attorneys eyes only)', l, re.I): continue
        if re.search(r'(Bates\s*(No|Number|ID)?\s*[:#]?)', l, re.I): continue
        if re.search(r'(draft|internal use only|company confidential)', l, re.I): continue
        if re.search(r'(page \d+)|(continued on next page)', l, re.I): continue
        if re.match(r'^[A-Za-z]$', l): continue  # chỉ 1 chữ cái lẻ
        # if re.search(r'[\u25A0-\u25FF\u2022\u00B7]', l): l = re.sub(r'[\u25A0-\u25FF\u2022\u00B7]', '', l)

        # remove bullet glyphs
        l = re.sub(r'[\u2022\u00B7\u25A0-\u25FF]', '', l)
        # drop lines with only punctuation
        if re.match(r'^[^\w\s]{3,}$', l):
            continue

        cleaned_lines.append(l)

    # -----------------------------
    # 🔹 Hậu xử lý
    # -----------------------------
    cleaned_text = " ".join(cleaned_lines)

    # Xóa khoảng trắng dư thừa, dấu lặp
    cleaned_text = re.sub(r'\s{2,}', ' ', cleaned_text)
    cleaned_text = re.sub(r'-\s+', '', cleaned_text)  # nối các từ bị ngắt dòng
    cleaned_text = cleaned_text.strip()

    return cleaned_text

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
from dropbox.exceptions import ApiError

def get_or_create_shared_link(dbx, path_lower: str) -> str:
    """
    Return a Dropbox shared link (preview link) for a file.
    If not exists, create one.
    """
    try:
        links = dbx.sharing_list_shared_links(
            path=path_lower,
            direct_only=True
        ).links
        if links:
            return links[0].url
    except ApiError:
        pass

    # create new shared link
    try:
        shared_link = dbx.sharing_create_shared_link_with_settings(path_lower)
        return shared_link.url
    except ApiError as e:
        print(f"❌ Cannot create shared link for {path_lower}: {e}")
        return None

def load_documents_from_dropbox(incremental=True):
    dbx = get_dropbox_client()
    response = dbx.files_list_folder(FOLDER_PATH, recursive=True)
    docs = []

    # load existing metadata ids for incremental indexing
    existing_ids = set()
    if incremental and os.path.exists(META_PATH):
        try:
            with open(META_PATH, "rb") as f:
                existing = pickle.load(f)
                for d in existing.get("documents", []):
                    md = d.get("metadata", {})
                    if md.get("bates_id"):
                        existing_ids.add(md["bates_id"])
        except Exception:
            existing_ids = set()

    while True:
        for entry in response.entries:
            if isinstance(entry, dropbox.files.FileMetadata) and entry.name.lower().endswith(".pdf"):
                print(f"📄 Loading PDF from Dropbox: {entry.name}")
                
                
                try:
                    _, res = dbx.files_download(entry.path_lower)
                    # pdf_data = io.BytesIO(res.content)
                    # pdf = fitz.open(stream=pdf_data, filetype="pdf") 
                    pdf_shared_link = get_or_create_shared_link(dbx, entry.path_lower)   
                    pdf_bytes = res.content
                    pdf_stream = fitz.open(stream=io.BytesIO(pdf_bytes), filetype="pdf")

                    # Quick check: does any page have text? If so, don't OCR entire file.
                    has_text_layer = False
                    for p in pdf_stream:
                        text = p.get_text("text").strip()
                        if text:
                            has_text_layer = True
                            break

                    # iterate pages
                    for page_num, page in enumerate(pdf_stream, start=1):
                        text = ""
                        try:
                            if has_text_layer:
                                text = page.get_text("text")
                                if not text or not text.strip():
                                    pix = page.get_pixmap(dpi=200)
                                    img_bytes = pix.tobytes("png")
                                    text = ocr_image_bytes(img_bytes)
                            else:
                                pix = page.get_pixmap(dpi=200)
                                img_bytes = pix.tobytes("png")
                                text = ocr_image_bytes(img_bytes)
                        except Exception as e:
                            print(f"Error extracting page {page_num} from {entry.name}: {e}")
                            continue

                        text = clean_transcript_text(text)
                        if not text:
                            continue

                        chunks = smart_chunk_text(text, chunk_size=CHUNK_SIZE, overlap=CHUNK_OVERLAP)

                        # -----------------------------
                        # 🔹 Tạo page ảo nếu file không có đánh dấu page
                        # -----------------------------
                        if not has_text_layer or all(re.match(r'^\d+$', ln.strip()) for ln in text.splitlines()):
                            # chia chunks ra thành "fake pages"
                            chunks_per_page = 1  # bạn có thể tăng để 1 page = n chunks
                            for idx, chunk in enumerate(chunks):
                                fake_page_num = (idx // chunks_per_page) + 1
                                bates_id = f"{entry.name.replace('.pdf','').upper()}_{fake_page_num:03d}_{idx:02d}"
                                if bates_id in existing_ids:
                                    continue
                                docs.append({
                                    "id": bates_id,
                                    "content": chunk,
                                    "metadata": {
                                        "source": os.path.basename(entry.path_display),
                                        "path": entry.path_display,
                                        "page": fake_page_num,
                                        "bates_id": bates_id,
                                        "chunk_index": idx,
                                        "chunk_chars": len(chunk),
                                        "has_ocr": not has_text_layer,
                                        "custodian": None,
                                        "collection_id": os.path.basename(FOLDER_PATH),
                                        "pdf_link": pdf_shared_link,   # 🔹 NEW
                                    }
                                })
                        else:
                            for i, chunk in enumerate(chunks):
                                bates_id = f"{entry.name.replace('.pdf','').upper()}_{page_num:03d}_{i:02d}"
                                if bates_id in existing_ids:
                                    continue
                                docs.append({
                                    "id": bates_id,
                                    "content": chunk,
                                    "metadata": {
                                        "source": os.path.basename(entry.path_display),
                                        "path": entry.path_display,
                                        "page": page_num,
                                        "bates_id": bates_id,
                                        "chunk_index": i,
                                        "chunk_chars": len(chunk),
                                        "has_ocr": not has_text_layer,
                                        "custodian": None,
                                        "collection_id": os.path.basename(FOLDER_PATH),
                                        "pdf_link": pdf_shared_link,   # 🔹 NEW
                                    }
                                })

                            if bates_id in existing_ids:
                                # skip already indexed chunk
                                continue
                            docs.append({
                                "id": bates_id,
                                "content": chunk,
                                "metadata": {
                                    "source": os.path.basename(entry.path_display),
                                    "path": entry.path_display,
                                    "page": page_num,
                                    "bates_id": bates_id,
                                    "chunk_index": i,
                                    "chunk_chars": len(chunk),
                                    "has_ocr": not has_text_layer,
                                    "custodian": None,
                                    "collection_id": os.path.basename(FOLDER_PATH),
                                    "pdf_link": pdf_shared_link,   # 🔹 NEW
                                }
                            })
                except Exception as e:
                    print(f"Error reading {entry.name}: {e}")

        if not response.has_more:
            break
        response = dbx.files_list_folder_continue(response.cursor)

    print(f"Loaded {len(docs)} new chunks (unique files: {len(set(d['metadata']['source'] for d in docs))}).")
    return docs

def build_faiss_index():
    docs = load_documents_from_dropbox()
    if not docs:
        print("❌ No PDF files found in the Dropbox folder.")
        return

    texts = [doc["content"] for doc in docs]
    metadatas = [doc["metadata"] for doc in docs]

    model = load_embedding_model()
    # choose batch size by hardware
    batch_size = 32 if torch.cuda.is_available() else 8

    print(f"🧠 Encoding {len(texts)} chunks with batch_size={batch_size} ...")
    embeddings = model.encode(
        texts,
        batch_size=batch_size,
        normalize_embeddings=True,
        show_progress_bar=True
    )
    import numpy as np
    embeddings = np.array(embeddings).astype("float32")

    # If index exists, load and append; else create new IP (inner product) index (works with normalized embeddings as cosine)
    os.makedirs(FAISS_DIR, exist_ok=True)
    if os.path.exists(INDEX_PATH):
        try:
            index = faiss.read_index(INDEX_PATH)
            print("Loaded existing FAISS index, appending vectors...")
            index.add(embeddings)
        except Exception as e:
            print(f"Failed to load existing index: {e}. Creating new index.")
            print("💾 Building FAISS index...")
            index = faiss.IndexFlatIP(embeddings.shape[1])
            index.add(embeddings)
    else:
        index = faiss.IndexFlatIP(embeddings.shape[1])
        index.add(embeddings)

    faiss.write_index(index, INDEX_PATH)
    print(f"FAISS index saved to {INDEX_PATH}")

    # --- Lưu metadata vào SQLite ---
    init_sqlite()
    new_docs = [{"content": t, "metadata": m} for t, m in zip(texts, metadatas)]
    insert_metadata(new_docs)

    print(f"✅ Indexed {len(new_docs)} chunks from {len(set(d['metadata']['source'] for d in docs))} PDFs.")
    print(f"📁 SQLite metadata: {SQLITE_DB_PATH}")
    
