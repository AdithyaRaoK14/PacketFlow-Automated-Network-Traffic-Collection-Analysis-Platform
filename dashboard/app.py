import os

import pandas as pd
import psycopg2
import streamlit as st

DB_CONFIG = dict(
    host=os.environ.get("DB_HOST", "app-db"),
    port=os.environ.get("DB_PORT", "5432"),
    dbname=os.environ.get("DB_NAME", "packets_db"),
    user=os.environ.get("DB_USER", "packets"),
    password=os.environ.get("DB_PASSWORD", "packets"),
)

st.set_page_config(page_title="Packet Capture Dashboard", layout="wide")
st.title("Packet Capture Dashboard")


@st.cache_data(ttl=10)
def load_data():
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("SELECT * FROM packets ORDER BY ts DESC LIMIT 5000", conn)
    conn.close()
    return df


df = load_data()

if df.empty:
    st.warning(
        "No packet data yet. Generate some traffic with JMeter/wrk/hey and "
        "wait for the Airflow DAG (runs every 2 minutes) to parse it."
    )
else:
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Packets", len(df))
    col2.metric("Unique Source IPs", df["src_ip"].nunique())
    col3.metric("HTTP Requests", int(df["http_method"].notna().sum()))

    st.subheader("Protocol Distribution")
    st.bar_chart(df["protocol"].value_counts())

    st.subheader("Server Distribution (Destination IP)")
    st.bar_chart(df["dst_ip"].value_counts().head(10))

    st.subheader("Top Requested URLs")
    urls = df["http_url"].dropna().value_counts().head(10)
    if not urls.empty:
        st.bar_chart(urls)
    else:
        st.write("No HTTP request lines parsed yet.")

    st.subheader("Raw Data (latest 200 rows)")
    st.dataframe(df.head(200))
