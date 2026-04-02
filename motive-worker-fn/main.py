import base64
import json
import logging
import os

import functions_framework
import googlemaps
import sqlalchemy

# Setup
DB_USER, DB_PASS, DB_NAME = os.environ.get("DB_USER"), os.environ.get("DB_PASS"), os.environ.get("DB_NAME")
INSTANCE_CONNECTION_NAME = os.environ.get("INSTANCE_CONNECTION_NAME")
gmaps = googlemaps.Client(key=os.environ.get("GOOGLE_MAPS_API_KEY"))
logger = logging.getLogger(__name__)

pool = sqlalchemy.create_engine(
    sqlalchemy.engine.url.URL.create(
        drivername="postgresql+pg8000",
        username=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        query={"unix_sock": f"/cloudsql/{INSTANCE_CONNECTION_NAME}/.s.PGSQL.5432"},
    )
)


def _is_non_zero_number(value):
    try:
        return float(value) != 0.0
    except (TypeError, ValueError):
        return False


@functions_framework.cloud_event
def process_motive_webhook(cloud_event):
    payload = json.loads(base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8"))
    if isinstance(payload, list):
        payload = payload[0]

    v_id = str(payload.get("vehicle", {}).get("id") or payload.get("vehicle_id") or payload.get("id"))
    v_num = payload.get("vehicle", {}).get("number") or payload.get("vehicle_number")

    # 1. Driver Logic
    d_obj = payload.get("end_driver") or payload.get("start_driver") or payload.get("driver") or payload.get("user")
    d_name = f"{d_obj.get('first_name', '')} {d_obj.get('last_name', '')}".strip() if (d_obj and isinstance(d_obj, dict)) else None

    # 2. Location & Address Resolution
    lat, lon = payload.get("lat"), payload.get("lon")
    has_usable_coords = _is_non_zero_number(lat) and _is_non_zero_number(lon)
    logger.info("Payload coordinate status: vehicle_id=%s has_usable_coords=%s lat=%s lon=%s", v_id, has_usable_coords, lat, lon)

    address = None
    geocode_found_formatted_address = False
    if has_usable_coords:
        try:
            res = gmaps.reverse_geocode((lat, lon))
            address = res[0]["formatted_address"] if res else None
            geocode_found_formatted_address = bool(address)
        except Exception as e:
            logger.warning(
                "Reverse geocode failed: vehicle_id=%s lat=%s lon=%s error=%s",
                v_id,
                lat,
                lon,
                str(e),
            )
            address = None

    logger.info(
        "Geocode result status: vehicle_id=%s attempted=%s has_formatted_address=%s",
        v_id,
        has_usable_coords,
        geocode_found_formatted_address,
    )

    # 3. Time & Action
    g_action = payload.get("event_type") or payload.get("geofence_action")
    is_ent = 1 if g_action in ["enter", "geofence_enter"] else 0
    is_ext = 1 if g_action in ["exit", "geofence_exit"] else 0
    ts = payload.get("end_time") or payload.get("located_at") or payload.get("created_at")

    with pool.connect() as conn:
        upsert_sql = sqlalchemy.text(
            """
            INSERT INTO fleet_status_monitor (vehicle_id, truck_number, last_known_driver_name, last_lat, last_lon, last_known_address, last_updated)
            VALUES (:vid, :vnum, :dname, COALESCE(:lat, 0), COALESCE(:lon, 0), :addr, :ts)
            ON CONFLICT (vehicle_id) DO UPDATE SET
                last_known_driver_name = COALESCE(:dname, fleet_status_monitor.last_known_driver_name),
                last_lat = CASE WHEN :lat IS NOT NULL AND :lat != 0 THEN :lat ELSE fleet_status_monitor.last_lat END,
                last_lon = CASE WHEN :lon IS NOT NULL AND :lon != 0 THEN :lon ELSE fleet_status_monitor.last_lon END,
                last_known_address = COALESCE(:addr, fleet_status_monitor.last_known_address),
                last_updated = :ts,
                is_in_geofence = CASE WHEN :ent = 1 THEN TRUE WHEN :ext = 1 THEN FALSE ELSE fleet_status_monitor.is_in_geofence END
        """
        )
        result = conn.execute(
            upsert_sql,
            {"vid": v_id, "vnum": v_num, "dname": d_name, "lat": lat, "lon": lon, "addr": address, "ts": ts, "ent": is_ent, "ext": is_ext},
        )
        conn.commit()
        logger.info(
            "Upsert completed: vehicle_id=%s addr_state=%s rowcount=%s",
            v_id,
            "non-null" if address else "null",
            result.rowcount,
        )
