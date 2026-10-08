import os
import tempfile

import psycopg2
from sshtunnel import SSHTunnelForwarder
import streamlit as st


# ============================================================
# SSH CONFIG
# ============================================================

SSH_HOST = st.secrets["ssh"]["SSH_HOST"]
SSH_PORT = int(st.secrets["ssh"]["SSH_PORT"])
SSH_USER = st.secrets["ssh"]["SSH_USER"]
SSH_PRIVATE_KEY = st.secrets["ssh"]["SSH_PRIVATE_KEY"]


# ============================================================
# POSTGRES CONFIG
# ============================================================

DB_NAME = st.secrets["database"]["DB_NAME"]
DB_USER = st.secrets["database"]["DB_USER"]
DB_PASSWORD = st.secrets["database"]["DB_PASSWORD"]

# PostgreSQL as seen FROM THE SSH SERVER
#
# This matches the successful test script:
#
# REMOTE_DB_HOST = "127.0.0.1"
# REMOTE_DB_PORT = 15432
#
DB_HOST = st.secrets["database"].get(
    "DB_HOST",
    "127.0.0.1"
)

DB_PORT = int(
    st.secrets["database"].get(
        "DB_PORT",
        15432
    )
)

# Local endpoint of SSH tunnel
LOCAL_HOST = "127.0.0.1"


# ============================================================
# GET POSTGRES CONNECTION THROUGH SSH TUNNEL
# ============================================================

def get_pg_conn():
    """
    Create an SSH tunnel and connect PostgreSQL through it.

    Equivalent to the working Airflow pattern:

        SSHHook("my_ssh")
        tunnel.start()

        psycopg2.connect(
            host="127.0.0.1",
            port=tunnel.local_bind_port
        )

    Returns:
        conn, tunnel
    """

    ssh_key_path = None
    tunnel = None

    try:

        # ----------------------------------------------------
        # 1. Normalize SSH private key
        # ----------------------------------------------------

        ssh_key = SSH_PRIVATE_KEY.replace(
            "\\n",
            "\n"
        ).strip()

        # ----------------------------------------------------
        # 2. Write private key to temporary file
        # ----------------------------------------------------

        with tempfile.NamedTemporaryFile(
            mode="w",
            delete=False,
            suffix=".pem"
        ) as key_file:

            key_file.write(ssh_key)
            key_file.flush()

            ssh_key_path = key_file.name

        # ----------------------------------------------------
        # 3. Create SSH tunnel
        # ----------------------------------------------------

        tunnel = SSHTunnelForwarder(

            # SSH gateway
            (SSH_HOST, SSH_PORT),

            ssh_username=SSH_USER,
            ssh_pkey=ssh_key_path,

            allow_agent=False,
            host_pkey_directories=[],

            # PostgreSQL destination
            # as seen from SSH server
            remote_bind_address=(
                DB_HOST,
                DB_PORT
            ),

            # Local tunnel endpoint
            local_bind_address=(
                LOCAL_HOST,
                0
            ),

            set_keepalive=30,
        )

        # ----------------------------------------------------
        # 4. Start SSH tunnel
        # ----------------------------------------------------

        tunnel.start()

        # ----------------------------------------------------
        # 5. PostgreSQL through local tunnel
        # ----------------------------------------------------

        conn = psycopg2.connect(

            # IMPORTANT:
            # PostgreSQL connection goes to LOCAL tunnel
            host=LOCAL_HOST,

            port=tunnel.local_bind_port,

            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,

            connect_timeout=10,
        )

        conn.autocommit = False

        return conn, tunnel

    except Exception:

        # ----------------------------------------------------
        # Cleanup if connection failed
        # ----------------------------------------------------

        if tunnel is not None:

            try:
                tunnel.stop()
            except Exception:
                pass

        if ssh_key_path:

            try:

                if os.path.exists(ssh_key_path):
                    os.unlink(ssh_key_path)

            except Exception:
                pass

        raise


# ============================================================
# GET INDEXED FILENAMES
# ============================================================

def get_indexed_filenames():

    conn = None
    tunnel = None

    try:

        conn, tunnel = get_pg_conn()

        with conn.cursor() as cur:

            cur.execute("""
                SELECT DISTINCT filename
                FROM chunks
                ORDER BY filename ASC
            """)

            rows = cur.fetchall()

        return [
            row[0]
            for row in rows
        ]

    finally:

        if conn is not None:

            try:
                conn.close()
            except Exception:
                pass

        if tunnel is not None:

            try:
                tunnel.stop()
            except Exception:
                pass


# ============================================================
# GET FILE STATS
# ============================================================

def get_file_stats():
    """
    Return:

    {
        "file1.pdf": {
            "pages": 12,
            "chunks": 134
        },
        "file2.pdf": {
            "pages": 8,
            "chunks": 97
        }
    }
    """

    conn = None
    tunnel = None

    try:

        conn, tunnel = get_pg_conn()

        with conn.cursor() as cur:

            cur.execute("""
                SELECT
                    filename,
                    COUNT(DISTINCT page) AS page_count,
                    COUNT(*) AS chunk_count
                FROM chunks
                WHERE issue_extracted = 1
                GROUP BY filename
                ORDER BY filename ASC
            """)

            rows = cur.fetchall()

        stats = {}

        for filename, pages, chunks in rows:

            stats[filename] = {
                "pages": pages,
                "chunks": chunks
            }

        return stats

    except Exception as e:

        st.error(
            f"Database connection failed: "
            f"{type(e).__name__}: {e}"
        )

        return {}

    finally:

        if conn is not None:

            try:
                conn.close()
            except Exception:
                pass

        if tunnel is not None:

            try:
                tunnel.stop()
            except Exception:
                pass


# ============================================================
# GET EXTRACTED ISSUES
# ============================================================

def get_extracted_issues(
    filenames: list[str]
):

    if not filenames:
        return []

    conn = None
    tunnel = None

    try:

        conn, tunnel = get_pg_conn()

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

        return rows

    finally:

        if conn is not None:

            try:
                conn.close()
            except Exception:
                pass

        if tunnel is not None:

            try:
                tunnel.stop()
            except Exception:
                pass