"""
Minimal Streamlit dashboard for the Document Ingestion Pipeline.
Shows document counts by type/status, package list, and recent processing logs.
"""

import psycopg2
import psycopg2.extras
import pandas as pd
import plotly.express as px
import streamlit as st

from app.config import DB_CONNECTION_STRING

st.set_page_config(page_title="Document Pipeline", page_icon="📄", layout="wide")
st.title("📄 Document Ingestion Pipeline")


# ── helpers ───────────────────────────────────────────────────────────────────

def _query(sql: str, params=None) -> list:
    try:
        conn = psycopg2.connect(DB_CONNECTION_STRING)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]
    except Exception as exc:
        st.error(f"Database error: {exc}")
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ── top-level metrics ─────────────────────────────────────────────────────────

status_rows = _query("SELECT status, COUNT(*) AS cnt FROM documents GROUP BY status")
status_map  = {r["status"]: r["cnt"] for r in status_rows}

total     = sum(status_map.values())
persisted = status_map.get("PERSISTED", 0)
failed    = status_map.get("FAILED", 0)
in_flight = total - persisted - failed

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Documents", total)
c2.metric("Persisted", persisted)
c3.metric("Failed", failed)
c4.metric("In-flight", in_flight)

st.divider()

# ── charts ────────────────────────────────────────────────────────────────────

col_l, col_r = st.columns(2)

with col_l:
    st.subheader("By Document Type")
    by_type = _query(
        "SELECT document_type, COUNT(*) AS cnt FROM documents GROUP BY document_type"
    )
    if by_type:
        fig = px.pie(pd.DataFrame(by_type), values="cnt", names="document_type",
                     color_discrete_sequence=px.colors.qualitative.Set2)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No documents yet.")

with col_r:
    st.subheader("By Status")
    if status_rows:
        color_map = {
            "PERSISTED": "#2ecc71", "FAILED": "#e74c3c",
            "PROCESSING": "#f39c12", "RECEIVED": "#3498db", "VALIDATED": "#9b59b6",
        }
        fig = px.bar(pd.DataFrame(status_rows), x="status", y="cnt",
                     color="status", color_discrete_map=color_map,
                     labels={"cnt": "Count", "status": "Status"})
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No documents yet.")

st.divider()

# ── packages ──────────────────────────────────────────────────────────────────

st.subheader("Document Packages")
packages = _query(
    """
    SELECT p.package_name,
           p.status,
           p.total_documents,
           COUNT(d.document_id)                                        AS ingested,
           COUNT(d.document_id) FILTER (WHERE d.status = 'PERSISTED') AS persisted,
           COUNT(d.document_id) FILTER (WHERE d.status = 'FAILED')    AS failed,
           p.created_at
    FROM   document_packages p
    LEFT JOIN documents d ON d.package_id = p.package_id
    GROUP BY p.package_id, p.package_name, p.status, p.total_documents, p.created_at
    ORDER BY p.created_at DESC
    LIMIT  20
    """
)
if packages:
    st.dataframe(pd.DataFrame(packages), use_container_width=True)
else:
    st.info("No packages yet.")

st.divider()

# ── recent documents ──────────────────────────────────────────────────────────

st.subheader("Recent Documents (last 50)")
docs = _query(
    """
    SELECT file_name, document_type, status, extraction_status,
           ROUND(EXTRACT(EPOCH FROM (updated_at - created_at))::numeric, 1) AS elapsed_s,
           created_at
    FROM   documents
    ORDER BY created_at DESC
    LIMIT  50
    """
)
if docs:
    st.dataframe(pd.DataFrame(docs), use_container_width=True)
else:
    st.info("No documents yet.")

st.divider()

# ── processing logs ───────────────────────────────────────────────────────────

st.subheader("Recent Processing Logs (last 30)")
logs = _query(
    """
    SELECT d.file_name, l.stage, l.status, l.message,
           COALESCE(l.error_details, '') AS error,
           l.created_at
    FROM   processing_logs l
    JOIN   documents d ON d.document_id = l.document_id
    ORDER BY l.created_at DESC
    LIMIT  30
    """
)
if logs:
    st.dataframe(pd.DataFrame(logs), use_container_width=True)
else:
    st.info("No processing logs yet.")

# ── Kafka event log ───────────────────────────────────────────────────────────

with st.expander("Kafka Event Log (last 30)"):
    kafka_events = _query(
        """
        SELECT topic_name, event_type, message_key, consumed_at
        FROM   kafka_events
        ORDER BY consumed_at DESC
        LIMIT  30
        """
    )
    if kafka_events:
        st.dataframe(pd.DataFrame(kafka_events), use_container_width=True)
    else:
        st.info("No Kafka events recorded yet.")

st.divider()
if st.button("Refresh"):
    st.rerun()
