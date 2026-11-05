-- Part E.2: Join starts and stops on session_id within a two-hour Kafka timestamp window.
-- Set auto.offset.reset=earliest before creating.
CREATE STREAM completed_sessions AS
SELECT
    s.session_id AS session_id,
    s.station_id AS station_id,
    s.start_time AS start_time,
    e.stop_time AS stop_time,
    (e.stop_time - s.start_time) / 60000.0 AS duration_minutes,
    e.energy_kwh AS energy_kwh,
    e.stop_reason AS stop_reason
FROM session_start_stream s
INNER JOIN session_stop_stream e
    WITHIN 2 HOURS
    ON s.session_id = e.session_id
EMIT CHANGES;

-- Verification (run separately with earliest offset):
-- SELECT session_id, station_id, duration_minutes, energy_kwh
-- FROM completed_sessions EMIT CHANGES LIMIT 5;
