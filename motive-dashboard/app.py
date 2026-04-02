import streamlit as st
import pandas as pd
import sqlalchemy
import os
from google.cloud.sql.connector import Connector
import pydeck as pdk
import time

connector = Connector()
def getconn():
    return connector.connect(os.environ["INSTANCE_CONNECTION_NAME"], "pg8000",
                             user=os.environ["DB_USER"], password=os.environ["DB_PASS"], db=os.environ["DB_NAME"])

engine = sqlalchemy.create_engine("postgresql+pg8000://", creator=getconn)

def get_enhanced_data():
    query = """
    SELECT *,
           COALESCE(last_known_driver_name, 'Unassigned') as driver_display,
           COALESCE(last_known_address, 'Address resolving...') as address_display
    FROM fleet_status_monitor 
    WHERE truck_number IS NOT NULL
    ORDER BY truck_number ASC
    """
    return pd.read_sql(query, engine)

st.set_page_config(page_title="Fleet Dashboard", layout="wide")

# UI STYLING
st.markdown("""
    <style>
    .truck-card {
        border-radius: 12px; padding: 1.5rem; margin-bottom: 1rem;
        background-color: var(--secondary-background-color);
        border: 1px solid rgba(128, 128, 128, 0.2);
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.2);
    }
    .addr-box { background-color: var(--background-color); padding: 10px; border-radius: 8px; margin: 10px 0; font-size: 0.85rem; }
    .status-badge { font-size: 0.8rem; font-weight: 700; color: #ff4b4b; margin-bottom: 5px; }
    </style>
""", unsafe_allow_html=True)

try:
    df = get_enhanced_data()
    # PREVENT NaN ERRORS
    df['fuel_level'] = pd.to_numeric(df['fuel_level'], errors='coerce').fillna(0)
    df['current_speed'] = pd.to_numeric(df['current_speed'], errors='coerce').fillna(0)
except:
    df = pd.DataFrame()

st.title("🚛 Real-Time Fleet Monitor")

if not df.empty:
    # 4. Map Section
    gmaps_key = os.environ.get("GOOGLE_MAPS_API_KEY")
    st.pydeck_chart(pdk.Deck(
        map_provider="google_maps", map_style="roadmap", api_keys={"google_maps": gmaps_key},
        initial_view_state=pdk.ViewState(latitude=df['last_lat'].mean(), longitude=df['last_lon'].mean(), zoom=4),
        layers=[pdk.Layer("ScatterplotLayer", df, get_position=['last_lon', 'last_lat'], get_color="[34, 197, 94, 200]", get_radius=30000)]
    ))
    
    st.divider()

    # 5. Grid Section
    cols = st.columns(3)
    for idx, row in df.iterrows():
        with cols[idx % 3]:
            status = f"📍 {row['current_geofence_name']}" if row['is_in_geofence'] else "🛣️ En Route"
            st.markdown(f"""
                <div class="truck-card">
                    <div style="font-weight:800; font-size:1.2rem; color: var(--text-color);">{row['truck_number']}</div>
                    <div style="opacity:0.7; color: var(--text-color);">👤 {row['driver_display']}</div>
                    <div class="status-badge">{status}</div>
                    <div class="addr-box"><b>Address:</b><br/>{row['address_display']}</div>
                    <div style="display:flex; justify-content:space-between; font-weight:600; color: var(--text-color);">
                        <span>💨 {int(row['current_speed'])} mph</span>
                        <span>⛽ {int(row['fuel_level'])}%</span>
                    </div>
                </div>
            """, unsafe_allow_html=True)

time.sleep(15)
st.rerun()
