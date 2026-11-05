-- Part E.4: One-hour, non-overlapping city-level tumbling windows.
-- Set auto.offset.reset=earliest before creating each persistent query.
CREATE TABLE hourly_city_metrics AS
SELECT
    city,
    WINDOWSTART AS window_start,
    WINDOWEND AS window_end,
    COUNT(*) AS session_count,
    SUM(energy_kwh) AS total_energy_kwh,
    SUM(total_cost) AS total_revenue
FROM enriched_sessions
WINDOW TUMBLING (SIZE 1 HOUR)
GROUP BY city
EMIT CHANGES;

-- Thirty-minute, overlapping station-level hopping windows, advanced every ten minutes.
CREATE TABLE station_activity_metrics AS
SELECT
    station_id,
    WINDOWSTART AS window_start,
    WINDOWEND AS window_end,
    COUNT(*) AS session_count,
    SUM(energy_kwh) AS total_energy_kwh,
    SUM(total_cost) AS total_revenue
FROM enriched_sessions
WINDOW HOPPING (
    SIZE 30 MINUTES,
    ADVANCE BY 10 MINUTES
)
GROUP BY station_id
EMIT CHANGES;

-- Verification (run separately with earliest offset):
-- SELECT * FROM hourly_city_metrics EMIT CHANGES LIMIT 5;
-- SELECT * FROM station_activity_metrics EMIT CHANGES LIMIT 10;
