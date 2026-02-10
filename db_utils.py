import psycopg
from sshtunnel import SSHTunnelForwarder
import os
import tempfile
import streamlit as st

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

def get_indexed_filenames():
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
                SELECT DISTINCT filename
                FROM chunks
                ORDER BY filename ASC
            """)
            rows = cur.fetchall()

        conn.close()
        return [r[0] for r in rows]

    finally:
        tunnel.stop()

def get_file_stats():
    """
    Return:
    {
        "file1.pdf": {"pages": 12, "chunks": 134},
        "file2.pdf": {"pages": 8, "chunks": 97}
    }
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
                SELECT
                    filename,
                    COUNT(DISTINCT page) AS page_count,
                    COUNT(*) AS chunk_count
                FROM chunks 
                WHERE TRUE 
                    AND issue_extracted = 1 
                GROUP BY filename
                ORDER BY filename ASC
            """)

            rows = cur.fetchall()
            conn.close()

            stats = {}
            for filename, pages, chunks in rows:
                stats[filename] = {
                    "pages": pages,
                    "chunks": chunks
                }

        return stats
    
    finally:
        tunnel.stop()
        
def get_extracted_issues(filenames: list[str]):
    """
    Return list of issues for selected files
    """
    if not filenames:
        return []

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
                SELECT
                    issue_id,
                    chunk_id,
                    filename,
                    page,
                    speaker_role,
                    risk_level,
                    legal_relevance,
                    quoted_text,
                    issue_type,
                    pdf_link
                FROM deposition_issues
                WHERE filename = ANY(%s)
                ORDER BY filename, page
            """, (filenames,))

            rows = cur.fetchall()
            conn.close()

        return rows

    finally:
        tunnel.stop()

