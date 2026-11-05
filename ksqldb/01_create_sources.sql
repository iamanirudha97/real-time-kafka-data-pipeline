-- Part E.1: Register existing Kafka topics as ksqlDB sources.
-- Run statements individually in the ksqlDB editor.

CREATE STREAM session_start_stream (
    session_id VARCHAR KEY,
    station_id VARCHAR,
    connector_id INTEGER,
    vehicle_id VARCHAR,
    user_id VARCHAR,
    start_time BIGINT,
    start_soc_pct INTEGER,
    payment_method VARCHAR
) WITH (
    KAFKA_TOPIC = 'session_start',
    KEY_FORMAT = 'KAFKA',
    VALUE_FORMAT = 'AVRO'
);

CREATE STREAM session_stop_stream (
    session_id VARCHAR KEY,
    station_id VARCHAR,
    stop_time BIGINT,
    end_soc_pct INTEGER,
    energy_kwh DOUBLE,
    stop_reason VARCHAR
) WITH (
    KAFKA_TOPIC = 'session_stop',
    KEY_FORMAT = 'KAFKA',
    VALUE_FORMAT = 'AVRO'
);

CREATE TABLE stations_table (
    station_id VARCHAR PRIMARY KEY,
    station_name VARCHAR,
    city VARCHAR,
    operator VARCHAR,
    latitude DOUBLE,
    longitude DOUBLE,
    max_power_kw DOUBLE,
    num_connectors INTEGER,
    tariff_per_kwh DOUBLE,
    status VARCHAR
) WITH (
    KAFKA_TOPIC = 'charging_stations',
    KEY_FORMAT = 'KAFKA',
    VALUE_FORMAT = 'AVRO'
);

-- Materialize station state to support keyed pull queries.
-- Set the query property auto.offset.reset=earliest before creating.
CREATE TABLE queryable_stations_table AS
SELECT *
FROM stations_table
EMIT CHANGES;

-- Verification (run separately):
-- DESCRIBE session_start_stream;
-- DESCRIBE session_stop_stream;
-- DESCRIBE stations_table;
-- PRINT 'charging_stations' FROM BEGINNING LIMIT 5;
-- SELECT * FROM queryable_stations_table WHERE station_id = 'ST-BOS-001';
