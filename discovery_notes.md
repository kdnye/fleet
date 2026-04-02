# Discovery results (schema + payload evidence, no implementation code)

## A) Schema inspection SQL

```sql
-- table presence / estimated rows
SELECT n.nspname AS schema_name,
       c.relname AS table_name,
       c.reltuples::bigint AS est_rows
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND n.nspname = 'public'
  AND c.relname IN ('motive_webhooks','fleet_status_monitor','geofence_dwell_history','geofence_performance')
ORDER BY c.relname;
```

```sql
-- columns
SELECT table_schema, table_name, ordinal_position, column_name, data_type, udt_name, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name IN ('motive_webhooks','fleet_status_monitor','geofence_dwell_history','geofence_performance')
ORDER BY table_name, ordinal_position;
```

```sql
-- constraints
SELECT tc.table_schema, tc.table_name, tc.constraint_name, tc.constraint_type,
       kcu.column_name, ccu.table_name AS ref_table, ccu.column_name AS ref_column
FROM information_schema.table_constraints tc
LEFT JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name
 AND tc.table_schema = kcu.table_schema
 AND tc.table_name = kcu.table_name
LEFT JOIN information_schema.constraint_column_usage ccu
  ON tc.constraint_name = ccu.constraint_name
 AND tc.table_schema = ccu.table_schema
WHERE tc.table_schema = 'public'
  AND tc.table_name IN ('motive_webhooks','fleet_status_monitor','geofence_dwell_history','geofence_performance')
ORDER BY tc.table_name, tc.constraint_type, tc.constraint_name, kcu.ordinal_position;
```

```sql
-- indexes
SELECT schemaname, tablename, indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'public'
  AND tablename IN ('motive_webhooks','fleet_status_monitor','geofence_dwell_history','geofence_performance')
ORDER BY tablename, indexname;
```

### Observed from live output
- `fleet_status_monitor`: est_rows `28`.
- `motive_webhooks`: est_rows `166`.
- `geofence_dwell_history` and `geofence_performance`: est_rows `-1` (no recent ANALYZE stats).
- `fleet_status_monitor` PK is `vehicle_id`; key columns include `current_speed`, `fuel_level`, `driver_id`, `last_known_driver_name`, dwell-related columns (`geofence_entered_at`, `last_dwell_minutes`).
- `motive_webhooks` has PK `id`, unique key on `event_id`, and indexes on `action`, `entity_id`, `occured_at`.

## B) Payload inspection SQL

```sql
-- taxonomy scan
SELECT COALESCE(action,'') AS action,
       COALESCE(raw_payload->>'event_type','') AS event_type,
       COALESCE(raw_payload->>'type','') AS payload_type,
       COUNT(*) AS cnt,
       MAX(created_at) AS newest
FROM motive_webhooks
WHERE created_at >= NOW() - INTERVAL '30 days'
GROUP BY 1,2,3
ORDER BY cnt DESC, newest DESC;
```

```sql
-- vehicle location events
SELECT id, created_at, action, raw_payload
FROM motive_webhooks
WHERE action IN ('vehicle_location_received','vehicle_location_updated')
ORDER BY created_at DESC
LIMIT 50;
```

```sql
-- vehicle geofence events
SELECT id, created_at, action, raw_payload
FROM motive_webhooks
WHERE action = 'vehicle_geofence_event'
ORDER BY created_at DESC
LIMIT 50;
```

```sql
-- asset geofence events
SELECT id, created_at, action, raw_payload
FROM motive_webhooks
WHERE action = 'asset_geofence_event'
ORDER BY created_at DESC
LIMIT 50;
```

```sql
-- user/driver-related events
SELECT id, created_at, action, raw_payload
FROM motive_webhooks
WHERE raw_payload ? 'driver_id'
   OR raw_payload ? 'current_driver'
   OR raw_payload ? 'start_driver'
   OR raw_payload ? 'end_driver'
ORDER BY created_at DESC
LIMIT 100;
```

```sql
-- events that can link driver to vehicle
SELECT id, created_at, action, raw_payload
FROM motive_webhooks
WHERE (
       (raw_payload ? 'driver_id' OR raw_payload ? 'current_driver' OR raw_payload ? 'start_driver' OR raw_payload ? 'end_driver')
   AND (raw_payload ? 'vehicle_id' OR raw_payload ? 'vehicle' OR raw_payload ? 'current_vehicle')
)
ORDER BY created_at DESC
LIMIT 100;
```

## C) Evidence table (from actual payload samples)

| target destination column | event type | sample event_id | JSON path | sample value | confidence |
|---|---|---:|---|---|---|
| vehicle_id | vehicle_location_received | `97873311-1d49-4a1c-9bbf-76ab2cb6c8d2` | `$.vehicle_id` | `2060101` | always present (location sample set) |
| vehicle_id | vehicle_geofence_event | `788719936` | `$.vehicle.id` | `2046250` | always present (vehicle geofence sample set) |
| vehicle_id | driver_performance_event_created | `1505143205` | `$.vehicle_id` and `$.current_vehicle.id` | `2060101` | always present (driver perf sample set) |
| truck_number | vehicle_location_received | `97873311-1d49-4a1c-9bbf-76ab2cb6c8d2` | `$.vehicle_number` | `525844_IL` | always present in provided location samples |
| truck_number | vehicle_geofence_event | `788719936` | `$.vehicle.number` | `656082_PA` | always present in provided geofence samples |
| truck_number | driver_performance_event_created | `1505143205` | `$.current_vehicle.number` | `525844_IL` | always present in provided driver perf samples |
| driver_id | vehicle_geofence_event | `788719936` | `$.start_driver.id` / `$.end_driver.id` | `2917627` | sometimes present (`end_driver` null on entry) |
| driver_id | driver_performance_event_updated | `1505067988` | `$.driver_id` and `$.current_driver.id` | `10769630` | always present in provided driver perf samples |
| driver full name | vehicle_geofence_event | `788719936` | `$.start_driver.first_name` + `$.start_driver.last_name` | `Mickey Jadallah` | sometimes present |
| driver full name | driver_performance_event_updated | `1505067988` | `$.current_driver.first_name` + `$.current_driver.last_name` | `Jim Richardson` | always present in provided driver perf samples |
| lat | vehicle_location_received | `97873311-1d49-4a1c-9bbf-76ab2cb6c8d2` | `$.lat` | `41.9616654` | always present in location samples |
| lon | vehicle_location_received | `97873311-1d49-4a1c-9bbf-76ab2cb6c8d2` | `$.lon` | `-88.026063` | always present in location samples |
| speed | vehicle_location_received | `97873311-1d49-4a1c-9bbf-76ab2cb6c8d2` | `$.speed` | `62.5` | always present in location samples |
| speed | driver_performance_event_updated | `1505067988` | `$.end_speed` | `62.878647` | always present in provided driver perf samples |
| fuel (raw) | vehicle_location_received | `97873311-1d49-4a1c-9bbf-76ab2cb6c8d2` | `$.fuel` | `2208.4726273139804` | always present in location samples |
| fuel percentage (preferred) | vehicle_location_received | `97873311-1d49-4a1c-9bbf-76ab2cb6c8d2` | `$.primary_fuel_level` | `64.8` | sometimes present (missing on some `engine_start/engine_stop`) |
| geofence_id | vehicle_geofence_event | `788719936` | `$.geofence_id` | `1259590` | always present in provided vehicle geofence samples |
| geofence name | vehicle_geofence_event | `788719936` | **not found in payload sample** | **N/A** | missing |
| event timestamp | vehicle_location_received | `97873311-1d49-4a1c-9bbf-76ab2cb6c8d2` | `$.located_at` | `2026-04-02T17:51:54Z` | always present in location samples |
| event timestamp | vehicle_geofence_event | `788719936` | `$.start_time` / `$.end_time` | `2026-04-02T16:56:51Z` / `2026-04-02T17:53:08Z` | always present as at least one of start/end |
| enter/exit indicator | vehicle_geofence_event | `788719936` | `$.event_type` | `geofence_exit` | always present in vehicle geofence samples |

### Corrected SQL helper (fixes recursive CTE error)

```sql
WITH RECURSIVE walk AS (
  SELECT id, created_at, action, '$'::text AS path, raw_payload::jsonb AS val
  FROM motive_webhooks
  WHERE created_at >= NOW() - INTERVAL '30 days'

  UNION ALL

  SELECT w.id,
         w.created_at,
         w.action,
         x.path,
         x.val
  FROM walk w
  CROSS JOIN LATERAL (
    SELECT w.path || '.' || k.key AS path, k.value AS val
    FROM jsonb_each(w.val) AS k(key, value)
    WHERE jsonb_typeof(w.val) = 'object'

    UNION ALL

    SELECT w.path || '[' || (a.ord-1)::text || ']' AS path, a.elem AS val
    FROM jsonb_array_elements(w.val) WITH ORDINALITY AS a(elem, ord)
    WHERE jsonb_typeof(w.val) = 'array'
  ) x
)
SELECT *
FROM walk
WHERE jsonb_typeof(val) IN ('string','number','boolean','null')
LIMIT 1000;
```

## D) Proposed state update rules (no code)

1. **Sticky driver**
   - Update `driver_id` / `last_known_driver_name` only when event includes explicit driver object/id.
   - Do not clear driver when omitted in sparse events.
   - Driver precedence per event: `end_driver` (for exit/close events) > `start_driver` > `current_driver` > `driver_id` scalar.

2. **Preserve location on non-location events**
   - Write `last_lat/last_lon` only from events containing valid numeric `lat` + `lon`.
   - Geofence and driver performance events without coordinates do not overwrite coordinates.

3. **Preserve fuel/speed**
   - Prefer `primary_fuel_level` for percentage when present.
   - Treat `fuel` as separate raw metric (unknown unit) and do not map into percent field without approval.
   - Update speed from `speed` on location events; use `end_speed` only for event-specific telemetry if no canonical speed present.

4. **Out-of-order protection**
   - Canonical timestamp precedence:
     - location: `located_at`
     - geofence: `end_time` if exit else `start_time`
     - driver performance: `end_time` fallback `start_time`
     - final fallback: `created_at` (ingest)
   - Reject stale update when `incoming_ts < fleet_status_monitor.last_updated`.

5. **Geofence enter/exit + dwell**
   - Entry (`event_type = geofence_entry`):
     - set in-geofence state true,
     - set `last_arrival_time = start_time`,
     - set `geofence_entered_at = start_time`,
     - set `last_geofence_name` only if a resolvable name exists.
   - Exit (`event_type = geofence_exit`):
     - set in-geofence false,
     - set `last_departure_time = end_time`,
     - compute dwell from `duration` if present; otherwise from `end_time - geofence_entered_at`.

6. **Current vs last vs average dwell**
   - `current_dwell`: live calc only when in geofence and `geofence_entered_at` is set.
   - `last_dwell_minutes`: most recent completed dwell per vehicle.
   - `avg_dwell`: use `geofence_dwell_history` grouped by `(vehicle_id, geofence)` over completed dwells only.

## E) Unresolved gaps requiring approval before coding

1. **Geofence name is missing in sampled geofence payloads**; only `geofence_id` is present. Need source of `geofence_name` lookup.
2. **Fuel semantics are ambiguous**: `fuel` appears large and non-percent; `primary_fuel_level` appears percent-like but is not always present.
3. **Asset geofence payload body not shown in evidence sample** despite taxonomy count; need one real row to confirm field paths.
4. **driver_performance events contain rich driver+vehicle linkage, but may not represent current assignment state**; confirm if these should update sticky driver.
5. **`geofence_dwell_history` vs `geofence_performance` authority** remains unresolved; dashboard currently references dwell history.
