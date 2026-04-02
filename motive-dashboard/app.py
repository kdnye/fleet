import streamlit as st
import pandas as pd
import sqlalchemy
import os
from google.cloud.sql.connector import Connector
import pydeck as pdk
import time


# 1. DATABASE CONNECTION
connector = Connector()
def getconn():
    return connector.connect(
        os.environ["INSTANCE_CONNECTION_NAME"],
        "pg8000",
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASS"],
        db=os.environ["DB_NAME"]
    )

engine = sqlalchemy.create_engine("postgresql+pg8000://", creator=getconn)


def format_metric(value: object, unit: str, decimals: int = 0) -> str:
    """Format telemetry values while preserving unknown signal state."""
    numeric_value = pd.to_numeric(value, errors='coerce')
    if pd.isna(numeric_value):
        return f"-- {unit}" if unit else "--"

    if decimals == 0:
        return f"{int(round(float(numeric_value)))} {unit}".strip()

    return f"{float(numeric_value):.{decimals}f} {unit}".strip()

def get_data():
    query = """
    SELECT *, 
           COALESCE(last_known_driver_name, 'Unassigned') as driver_display,
           COALESCE(last_known_address, 'Street Address Resolving...') as address_display
    FROM fleet_status_monitor WHERE truck_number IS NOT NULL
    ORDER BY truck_number ASC
    """
    return pd.read_sql(query, engine)

st.set_page_config(page_title="Fleet Monitor", layout="wide")

# UI STYLING
st.markdown("""
    <style>
    .truck-card {
        border-radius: 12px; padding: 1.5rem; margin-bottom: 1rem;
        background-color: var(--secondary-background-color);
        border: 1px solid rgba(128,128,128,0.3);
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
    }
    .addr-box { 
        background-color: var(--background-color); 
        padding: 10px; border-radius: 8px; margin: 10px 0; 
        font-size: 0.85rem; border: 1px solid rgba(128,128,128,0.1);
        color: var(--text-color);
    }
    </style>
""", unsafe_allow_html=True)

# THE MAP FIX: Use standard environment variable
gmaps_key = os.environ.get("GOOGLE_MAPS_API_KEY")

try:
    df = get_data()
    df['fuel_level_numeric'] = pd.to_numeric(df['fuel_level'], errors='coerce')
    df['current_speed_numeric'] = pd.to_numeric(df['current_speed'], errors='coerce')
    df['lat'] = pd.to_numeric(df['last_lat'], errors='coerce').fillna(0)
    df['lon'] = pd.to_numeric(df['last_lon'], errors='coerce').fillna(0)

    df['fuel_display'] = df['fuel_level_numeric'].apply(lambda value: format_metric(value, '%'))
    df['speed_display'] = df['current_speed_numeric'].apply(lambda value: format_metric(value, 'mph'))
except Exception as e:
    st.error(f"Data load failed: {e}")
    df = pd.DataFrame()

st.title("🚛 Real-Time Fleet Monitor")
st.caption("build: 2026-04-02-r1")

with st.expander("Debug Panel", expanded=True):
    key_present = bool(gmaps_key)
    row_count = len(df)
    non_zero_coordinate_count = 0
    if not df.empty and {'lat', 'lon'}.issubset(df.columns):
        non_zero_coordinate_count = ((df['lat'] != 0) & (df['lon'] != 0)).sum()

    st.write(f"Google Maps API key present: {key_present}")
    st.write(f"Row count: {row_count}")
    st.write(f"Non-zero coordinate count: {int(non_zero_coordinate_count)}")

if not df.empty:
    # Force coordinates to numeric for the map
    map_df = df[df['lat'] != 0].copy()
    
    if gmaps_key and not map_df.empty:
        st.pydeck_chart(pdk.Deck(
            map_provider="google_maps",
            map_style="roadmap",
            api_keys={"google_maps": gmaps_key},
            initial_view_state=pdk.ViewState(
                latitude=map_df['lat'].mean(), 
                longitude=map_df['lon'].mean(), 
                zoom=4, pitch=0
            ),
            layers=[
                pdk.Layer(
                    "ScatterplotLayer", map_df,
                    get_position=['lon', 'lat'],
                    get_color="[34, 197, 94, 200]", get_radius=30000, pickable=True
                ),
                pdk.Layer(
                    "TextLayer", map_df,
                    get_position=['lon', 'lat'], get_text="truck_number",
                    get_size=18, get_color=[255, 255, 255], get_pixel_offset=[0, -20]
                )
            ]
        ))
    
    st.divider()

    # CARDS
    cols = st.columns(3)
    for idx, row in df.iterrows():
        with cols[idx % 3]:
            st.markdown(f"""
                <div class="truck-card">
                    <div style="font-weight:800; font-size:1.2rem; color:var(--text-color);">{row['truck_number']}</div>
                    <div style="opacity:0.7; color:var(--text-color);">👤 {row['driver_display']}</div>
                    <div class="addr-box"><b>Location:</b><br/>{row['address_display']}</div>
                    <div style="display:flex; justify-content:space-between; font-weight:600; color:var(--text-color);">
                        <span>💨 {row['speed_display']}</span>
                        <span>⛽ {row['fuel_display']}</span>
                    </div>
                </div>
            """, unsafe_allow_html=True)

time.sleep(15)
st.rerun()
