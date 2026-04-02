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

# 2. DATA FETCHING
def get_enhanced_data():
    query = """
    SELECT *,
           COALESCE(last_known_driver_name, 'Unassigned / Pending') as driver_display,
           COALESCE(last_known_address, 'Address resolving...') as address_display
    FROM fleet_status_monitor 
    WHERE truck_number IS NOT NULL AND unit_type = 'vehicle'
    ORDER BY truck_number ASC
    """
    return pd.read_sql(query, engine)

# 3. UI CONFIG & THEME CSS
st.set_page_config(page_title="Fleet Dashboard", layout="wide")

st.markdown("""
    <style>
    .stApp {
        background-color: var(--background-color);
    }
    
    .truck-card {
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 1rem;
        /* Force a distinct background from the main page */
        background-color: var(--secondary-background-color);
        border: 1px solid rgba(128, 128, 128, 0.2);
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2);
    }
    
    .card-header {
        font-weight: 800;
        font-size: 1.3rem;
        margin-bottom: 8px;
        color: var(--text-color);
    }
    
    .status-badge {
        font-size: 0.85rem;
        font-weight: 700;
        color: #ff4b4b; /* Motive Red */
        margin-bottom: 12px;
    }
    
    .addr-box {
        /* Darker/Lighter contrast box inside the card */
        background-color: var(--background-color);
        padding: 12px;
        border-radius: 8px;
        margin: 12px 0;
        font-size: 0.85rem;
        border: 1px solid rgba(128, 128, 128, 0.1);
        color: var(--text-color);
    }
    
    .stats-row {
        display: flex; 
        justify-content: space-between;
        font-size: 0.95rem;
        font-weight: 600;
        color: var(--text-color);
    }
    
    .coord-text {
        font-size: 0.7rem;
        opacity: 0.6;
        margin-top: 15px;
        color: var(--text-color);
    }
    </style>
""", unsafe_allow_html=True)

try:
    df = get_enhanced_data()
    df['fuel_level'] = pd.to_numeric(df['fuel_level'], errors='coerce').fillna(0)
    df['current_speed'] = pd.to_numeric(df['current_speed'], errors='coerce').fillna(0)
    df['lat'] = pd.to_numeric(df['last_lat'], errors='coerce').fillna(0)
    df['lon'] = pd.to_numeric(df['last_lon'], errors='coerce').fillna(0)
except Exception as e:
    st.error(f"Sync Error: {e}")
    df = pd.DataFrame()

st.title("🚛 Real-Time Fleet Monitor")

# 4. MAP SECTION (Corrected for Visibility)
if not df.empty:
    map_df = df[(df['lat'] != 0)].copy()
    if not map_df.empty:
        is_dark = st.get_option("theme.base") == "dark"
        gmaps_key = os.environ.get("GOOGLE_MAPS_API_KEY")
        
        # Define Layers
        layers = [
            pdk.Layer(
                "ScatterplotLayer", map_df,
                get_position=['lon', 'lat'],
                get_color="[34, 197, 94, 200]", # Green
                get_radius=30000, 
                pickable=True
            ),
            pdk.Layer(
                "TextLayer", map_df,
                get_position=['lon', 'lat'],
                get_text="truck_number",
                get_size=20,
                get_color=[255, 255, 255] if is_dark else [31, 41, 55],
                get_pixel_offset=[0, -25],
                background=True,
                get_background_color=[0, 0, 0, 160] if is_dark else [255, 255, 255, 200],
                collidable=True
            )
        ]
        
        # Render Deck with explicit Google Maps settings
        st.pydeck_chart(pdk.Deck(
            map_provider="google_maps",
            map_style="roadmap",
            api_keys={"google_maps": gmaps_key},
            initial_view_state=pdk.ViewState(
                latitude=df['lat'].mean(),
                longitude=df['lon'].mean(),
                zoom=4,
                pitch=0
            ),
            layers=layers,
            tooltip={"text": "{truck_number}\n{driver_display}"}
        ))

st.divider()

# 5. CARDS GRID
if not df.empty:
    cols = st.columns(3)
    for idx, row in df.iterrows():
        with cols[idx % 3]:
            icon = '💨' if row['current_speed'] > 0 else '⚪'
            status_label = f"📍 {row['current_geofence_name']}" if row['is_in_geofence'] else "🛣️ En Route"
            
            card_html = f"""
                <div class="truck-card">
                    <div class="card-header">{icon} {row['truck_number']}</div>
                    <div style="opacity: 0.8; font-size: 0.9rem; margin-bottom:10px; color: var(--text-color);">
                        👤 <b>{row['driver_display']}</b>
                    </div>
                    <div class="status-badge">{status_label}</div>
                    <div class="addr-box">
                        <b>Current Address:</b><br/>
                        {row['address_display']}
                    </div>
                    <div class="stats-row">
                        <span>Speed: {int(row['current_speed'])} mph</span>
                        <span>Fuel: {int(row['fuel_level'])}%</span>
                    </div>
                    <div class="coord-text">
                        LAT: {row['lat']:.5f} | LON: {row['lon']:.5f}<br/>
                        Last Ping: {row['last_updated']}
                    </div>
                </div>
            """
            st.markdown(card_html, unsafe_allow_html=True)
else:
    st.info("No vehicle telemetry found in database.")

# 6. AUTO-REFRESH
time.sleep(15)
st.rerun()
