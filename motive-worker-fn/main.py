import base64, json, os, sqlalchemy, functions_framework, googlemaps
from datetime import datetime

# Setup
DB_USER, DB_PASS, DB_NAME = os.environ.get("DB_USER"), os.environ.get("DB_PASS"), os.environ.get("DB_NAME")
INSTANCE_CONNECTION_NAME = os.environ.get("INSTANCE_CONNECTION_NAME")
gmaps = googlemaps.Client(key=os.environ.get("GOOGLE_MAPS_API_KEY"))

pool = sqlalchemy.create_engine(
    sqlalchemy.engine.url.URL.create(
        drivername="postgresql+pg8000",
        username=DB_USER, password=DB_PASS, database=DB_NAME,
        query={"unix_sock": f"/cloudsql/{INSTANCE_CONNECTION_NAME}/.s.PGSQL.5432"},
    )
)

@functions_framework.cloud_event
def process_motive_webhook(cloud_event):
    payload = json.loads(base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8"))
    if isinstance(payload, list): payload = payload[0]

    v_id = str(payload.get("vehicle", {}).get("id") or payload.get("vehicle_id") or payload.get("id"))
    v_num = payload.get("vehicle", {}).get("number") or payload.get("vehicle_number")
    
    # 1. Driver Logic
    d_obj = payload.get("end_driver") or payload.get("start_driver") or payload.get("driver") or payload.get("user")
    d_name = f"{d_obj.get('first_name', '')} {d_obj.get('last_name', '')}".strip() if (d_obj and isinstance(d_obj, dict)) else None

    # 2. Location & Address Resolution
    lat, lon = payload.get("lat"), payload.get("lon")
    address = None
    if lat and lon:
        try:
            res = gmaps.reverse_geocode((lat, lon))
            address = res[0]['formatted_address'] if res else None
        except: address = None

    # 3. Time & Action
    g_action = payload.get("event_type") or payload.get("geofence_action")
    is_ent = 1 if g_action in ['enter', 'geofence_enter'] else 0
    is_ext = 1 if g_action in ['exit', 'geofence_exit'] else 0
    ts = payload.get("end_time") or payload.get("located_at") or payload.get("created_at")

    with pool.connect() as conn:
        upsert_sql = sqlalchemy.text("""
            INSERT INTO fleet_status_monitor (vehicle_id, truck_number, last_known_driver_name, last_lat, last_lon, last_known_address, last_updated)
            VALUES (:vid, :vnum, :dname, COALESCE(:lat, 0), COALESCE(:lon, 0), :addr, :ts)
            ON CONFLICT (vehicle_id) DO UPDATE SET
                last_known_driver_name = COALESCE(:dname, fleet_status_monitor.last_known_driver_name),
                last_lat = CASE WHEN :lat IS NOT NULL AND :lat != 0 THEN :lat ELSE fleet_status_monitor.last_lat END,
                last_lon = CASE WHEN :lon IS NOT NULL AND :lon != 0 THEN :lon ELSE fleet_status_monitor.last_lon END,
                last_known_address = COALESCE(:addr, fleet_status_monitor.last_known_address),
                last_updated = :ts,
                is_in_geofence = CASE WHEN :ent = 1 THEN TRUE WHEN :ext = 1 THEN FALSE ELSE fleet_status_monitor.is_in_geofence END
        """)
        conn.execute(upsert_sql, {"vid": v_id, "vnum": v_num, "dname": d_name, "lat": lat, "lon": lon, "addr": address, "ts": ts, "ent": is_ent, "ext": is_ext})
        conn.commit()
