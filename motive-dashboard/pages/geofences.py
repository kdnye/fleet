import streamlit as st
import pandas as pd
import sqlalchemy
import os
from google.cloud.sql.connector import Connector

# Reuse your existing connection logic
connector = Connector()
def getconn():
    return connector.connect(os.environ["INSTANCE_CONNECTION_NAME"], "pg8000",
                             user=os.environ["DB_USER"], password=os.environ["DB_PASS"], db=os.environ["DB_NAME"])

engine = sqlalchemy.create_engine("postgresql+pg8000://", creator=getconn)

st.set_page_config(page_title="Geofence Analytics", layout="wide")
st.title("🏢 Geofence Performance & Dwell Times")

def get_geofence_stats():
    query = """
    WITH latest_visits AS (
        SELECT DISTINCT ON (geofence_name)
            geofence_name, vehicle_id, departure_time
        FROM geofence_dwell_history
        ORDER BY geofence_name, departure_time DESC
    )
    SELECT 
        h.geofence_name,
        ROUND(AVG(h.dwell_minutes)::numeric, 1) as avg_dwell,
        COUNT(h.id) as total_visits,
        lv.departure_time as last_visit,
        f.truck_number,
        f.last_known_driver_name as driver
    FROM geofence_dwell_history h
    JOIN latest_visits lv ON h.geofence_name = lv.geofence_name
    LEFT JOIN fleet_status_monitor f ON lv.vehicle_id = f.vehicle_id
    GROUP BY h.geofence_name, lv.departure_time, f.truck_number, f.last_known_driver_name
    ORDER BY avg_dwell DESC
    """
    return pd.read_sql(query, engine)

df = get_geofence_stats()

if not df.empty:
    # Summary stats at the top
    top_col1, top_col2 = st.columns(2)
    top_col1.metric("Busiest Fence", df.iloc[0]['geofence_name'], f"{df.iloc[0]['total_visits']} visits")
    top_col2.metric("Highest Dwell", f"{df.iloc[0]['avg_dwell']} mins")

    st.divider()

    # Cards Grid
    cols = st.columns(3)
    for idx, row in df.iterrows():
        with cols[idx % 3]:
            with st.container(border=True):
                st.subheader(row['geofence_name'])
                
                c1, c2 = st.columns(2)
                c1.metric("Avg Dwell", f"{row['avg_dwell']}m")
                c2.metric("Total Visits", row['total_visits'])
                
                st.markdown("---")
                st.markdown("**Last Visit Details**")
                st.write(f"🚛 **Truck:** {row['truck_number']}")
                st.write(f"👤 **Driver:** {row['driver']}")
                st.caption(f"Departed: {row['last_visit']}")

else:
    st.info("No geofence history recorded yet. Data will appear once trucks exit a geofence.")
