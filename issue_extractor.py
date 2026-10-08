# backend/issue_extractor.py
import tempfile
import uuid, json, re, os
import psycopg2
from anthropic import Anthropic
import streamlit as st
from tqdm import tqdm  
from db_utils import get_pg_conn

ANTHROPIC_MODEL = st.secrets["claude"]["anthropic_model"]
ANTHROPIC_API_KEY = st.secrets["claude"]["api_key"]
# DB_PATH = "data/faiss_store/metadata.db"

client = Anthropic(api_key=ANTHROPIC_API_KEY)

PROMPT = """
    You are a legal analyst for U.S. mass tort litigation. Review the deposition excerpt and extract only statements useful to plaintiffs.

    Quote testimony verbatim. Do not paraphrase or infer. Extract only statements with evidentiary or impeachment value.

    Classify each statement using exactly one issue type from this list: failure_to_warn, causation, exposure_pathway, corporate_knowledge, regulatory_compliance, alternative_causes, damages_injury_timeline, other.

    Focus on statements relevant to failure to warn, causation, exposure, corporate knowledge, or regulatory compliance, especially those impacting Daubert admissibility such as methodology, data gaps, uncertainty, or limitations.

    If nothing relevant appears, return an empty issues array.

    Respond with only valid JSON and nothing else, using this structure exactly:

    {
        "issues": [
            {
            "issue_type": "failure_to_warn | causation | exposure_pathway | corporate_knowledge | regulatory_compliance | alternative_causes | damages_injury_timeline | other",
            "quoted_text": "exact quote from the transcript",
            "legal_relevance": "brief legal relevance",
            "risk_level": "high | medium | low"
            }
        ]
    }
"""

REQUIRED_KEYS = {
    "issue_type",
    "quoted_text",
    "legal_relevance",
    "risk_level"
}

def extract_json(text):
    match = re.search(r"\{.*\}", text, re.S)
    return match.group(0) if match else None

def init_issue_tables():

    conn = None
    tunnel = None

    try:

        conn, tunnel = get_pg_conn()

        with conn.cursor() as cur:

            cur.execute("""
                CREATE TABLE IF NOT EXISTS issue_progress (
                    chunk_id TEXT PRIMARY KEY,
                    filename TEXT,
                    extracted INTEGER DEFAULT 0
                )
            """)

        conn.commit()

    finally:

        if conn is not None:
            conn.close()

        if tunnel is not None:
            tunnel.stop()

def run_issue_extraction(filename: str):
    init_issue_tables()

    conn = None
    tunnel = None

    try:

        conn, tunnel = get_pg_conn()

        with conn.cursor() as cur:
            # 🔍 Lấy chunk CHƯA extract cho file được chọn
            cur.execute("""
                SELECT
                    f.chunk_id,
                    f.content,
                    f.page,
                    f.filename,
                    f.pdf_link
                FROM chunks f
                LEFT JOIN issue_progress p
                    ON f.chunk_id = p.chunk_id
                WHERE f.filename = %s
                AND (p.extracted IS NULL OR p.extracted = 0)
            """, (filename,))
            rows = cur.fetchall()

            total = len(rows)
            extracted = 0
            failed = 0

            print(f"🚀 Starting issue extraction for file '{filename}'")
            print(f"   • Chunks to process: {total}")

            for chunk_id, content, page, filename, pdf_link in tqdm(
                rows,
                total=total,
                desc=f"Extracting {filename}",
                unit="chunk"
            ):
                try:
                    resp = client.messages.create(
                        model=ANTHROPIC_MODEL,
                        max_tokens=1024,
                        temperature=0,
                        messages=[
                            {
                                "role": "user",
                                "content": PROMPT + "\n\nTranscript:\n" + content
                            }
                        ]
                    )

                    raw_text = resp.content[0].text
                    json_text = extract_json(raw_text)
                    if not json_text:
                        raise ValueError("No JSON found")

                    data = json.loads(json_text)
                    issues = data.get("issues", [])

                    for it in issues:
                        if not REQUIRED_KEYS.issubset(it):
                            continue

                        cur.execute("""
                            INSERT INTO deposition_issues
                            (
                                issue_id,
                                chunk_id,
                                filename,
                                page,
                                issue_type,
                                quoted_text,
                                legal_relevance,
                                risk_level,
                                pdf_link
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            str(uuid.uuid4()),
                            chunk_id,
                            filename,
                            page,
                            it["issue_type"],
                            it["quoted_text"],
                            it["legal_relevance"],
                            it["risk_level"],
                            pdf_link
                        ))

                        extracted += 1

                    cur.execute("""
                        UPDATE chunks
                        SET issue_extracted = 1
                        WHERE chunk_id = %s
                    """, (chunk_id,))

                    cur.execute("""
                        INSERT INTO issue_progress
                            (chunk_id, filename, extracted)
                        VALUES (%s, %s, 1)
                    """, (chunk_id, filename))

                    conn.commit() 

                except Exception as e:
                    failed += 1
                    print(f"\n[ERROR] chunk_id={chunk_id}: {e}")

            conn.close()

            print("\n✅ DONE")
            print(f"   • File: {filename}")
            print(f"   • Chunks processed: {total}")
            print(f"   • Issues extracted: {extracted}")
            print(f"   • Failed chunks: {failed}")

            return extracted
    finally:
        if tunnel is not None:
            tunnel.stop()

if __name__ == "__main__":
    run_issue_extraction()
