import streamlit as st
import pandas as pd
import sqlalchemy
import os
from google.cloud.sql.connector import Connector
import time

# 1. DB Connection (Must repeat on every page file)
connector = Connector()
def getconn():
    return connector.connect(os.environ["INSTANCE_CONNECTION_NAME"], "pg8000",
                             user=os.environ["DB_USER"], password=os.environ["DB_PASS"], db=os.environ["DB_NAME"])

engine = sqlalchemy.create_engine("postgresql+pg8000://", creator=getconn)

st.set_page_config(page_title="Trailer Tracker", layout="wide")
st.title("🚛 Trailer Inventory")

def get_trailer_data():
    query = "SELECT * FROM fleet_status_monitor WHERE unit_type = 'trailer' ORDER BY truck_number ASC"
    return pd.read_sql(query, engine)

try:
    df = get_trailer_data()
    df['fuel_level'] = pd.to_numeric(df['fuel_level'], errors='coerce').fillna(0)
except:
    df = pd.DataFrame()

if not df.empty:
    cols = st.columns(4)
    for idx, row in df.iterrows():
        with cols[idx % 4]:
            st.markdown(f"""
                <div style="background:#ffffff; padding:15px; border-radius:10px; border:1px solid #dee2e6; color:#1a202c;">
                    <h3 style="margin:0; color:#1a202c;">🆔 {row['truck_number']}</h3>
                    <p style="color:#1a202c;"><b>Status:</b> {"📍 AT " + (row['current_geofence_name'] or 'YARD') if row['is_in_geofence'] else "🛣️ EN ROUTE"}</p>
                </div>
            """, unsafe_allow_html=True)
else:
    st.info("No trailers detected.")

time.sleep(15)
st.rerun()
