"""Read-only diagnostic report for vehicles missing last_known_address.

Usage:
  python diagnose_missing_addresses.py

Required env vars:
  DB_USER, DB_PASS, DB_NAME, INSTANCE_CONNECTION_NAME
Optional env vars:
  STALE_HOURS (default 24)
"""

import os

import sqlalchemy


DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME")
INSTANCE_CONNECTION_NAME = os.environ.get("INSTANCE_CONNECTION_NAME")
STALE_HOURS = int(os.environ.get("STALE_HOURS", "24"))


def get_engine():
    return sqlalchemy.create_engine(
        sqlalchemy.engine.url.URL.create(
            drivername="postgresql+pg8000",
            username=DB_USER,
            password=DB_PASS,
            database=DB_NAME,
            query={"unix_sock": f"/cloudsql/{INSTANCE_CONNECTION_NAME}/.s.PGSQL.5432"},
        )
    )


def main():
    query = sqlalchemy.text(
        """
        SELECT
            vehicle_id,
            truck_number,
            last_lat,
            last_lon,
            last_updated,
            CASE
                WHEN last_updated IS NULL
                    OR last_updated < (NOW() - (CAST(:stale_hours AS TEXT) || ' hours')::interval)
                    THEN 'no_fresh_events'
                ELSE 'fresh_event_missing_address'
            END AS diagnosis_bucket
        FROM fleet_status_monitor
        WHERE COALESCE(last_lat, 0) != 0
          AND COALESCE(last_lon, 0) != 0
          AND last_known_address IS NULL
        ORDER BY last_updated ASC NULLS FIRST
        """
    )

    with get_engine().connect() as conn:
        rows = conn.execute(query, {"stale_hours": STALE_HOURS}).mappings().all()

    if not rows:
        print("No vehicles currently match the diagnostic criteria.")
        return

    print(
        "vehicle_id | truck_number | last_lat | last_lon | last_updated | diagnosis_bucket"
    )
    print("-" * 110)
    for row in rows:
        print(
            f"{row['vehicle_id']} | {row['truck_number']} | {row['last_lat']} | {row['last_lon']} | {row['last_updated']} | {row['diagnosis_bucket']}"
        )

    print("\nInterpretation:")
    print("- no_fresh_events: updates are stale; address may simply not have been refreshed recently.")
    print("- fresh_event_missing_address: check worker logs for this vehicle_id:")
    print("  * if 'has_formatted_address=False', geocoder returned no address or failed.")
    print("  * if 'has_formatted_address=True' but upsert log shows addr_state=null, inspect mapping/serialization.")
    print("  * if 'has_formatted_address=True' and upsert addr_state=non-null, verify DB constraints/triggers.")


if __name__ == "__main__":
    main()
