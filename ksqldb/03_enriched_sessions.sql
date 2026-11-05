-- Part E.3: Repartition completed sessions by station_id.
-- Set auto.offset.reset=earliest before creating each persistent query.
CREATE STREAM completed_sessions_by_station
WITH (PARTITIONS = 6) AS
SELECT
    station_id AS station_id,
    session_id,
    start_time,
    stop_time,
    duration_minutes,
    energy_kwh,
    stop_reason
FROM completed_sessions
PARTITION BY station_id
EMIT CHANGES;

-- Enrich with latest station reference data.
CREATE STREAM enriched_sessions AS
SELECT
    s.station_id AS station_id,
    s.session_id AS session_id,
    s.start_time AS start_time,
    s.stop_time AS stop_time,
    s.duration_minutes AS duration_minutes,
    s.energy_kwh AS energy_kwh,
    s.stop_reason AS stop_reason,
    t.station_name AS station_name,
    t.city AS city,
    t.operator AS operator,
    t.tariff_per_kwh AS tariff_per_kwh,
    s.energy_kwh * t.tariff_per_kwh AS total_cost
FROM completed_sessions_by_station s
LEFT JOIN queryable_stations_table t
    ON s.station_id = t.station_id
EMIT CHANGES;

-- Verification (run separately with earliest offset):
-- DESCRIBE completed_sessions_by_station;
-- SELECT session_id, station_id, city, operator, energy_kwh,
--        tariff_per_kwh, total_cost
-- FROM enriched_sessions EMIT CHANGES LIMIT 5;
