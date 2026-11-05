# Part E — ksqlDB Stream Processing

## Overview

This directory documents the ksqlDB portion of the EV charging station Kafka assignment. Three existing Kafka topics are used: `session_start` and `session_stop` (Avro event streams), and `charging_stations` (Avro station reference records, keyed by `station_id`). The pipeline matches charging starts with stops, enriches completed sessions with station metadata, and produces windowed metrics.

## SQL files and execution order

| File | Purpose | Objects created |
| --- | --- | --- |
| `01_create_sources.sql` | Register topics and materialize station state | `session_start_stream`, `session_stop_stream`, `stations_table`, `queryable_stations_table` |
| `02_completed_sessions.sql` | Join starts and stops within two hours | `completed_sessions` |
| `03_enriched_sessions.sql` | Repartition by station and enrich with metadata | `completed_sessions_by_station`, `enriched_sessions` |
| `04_windowed_aggregations.sql` | Compute city and station activity metrics | `hourly_city_metrics`, `station_activity_metrics` |

Execute statements in the listed order, individually in the Confluent Cloud ksqlDB editor. Before each `CREATE ... AS SELECT` persistent query, set the editor's `auto.offset.reset` query property to **Earliest** so that retained input records are replayed. The objects have already been created in the development cluster; **do not blindly rerun CREATE statements** against the same cluster.

## Data model

- `session_start_stream`: Kafka key `session_id`; Avro values include `station_id`, `connector_id`, `vehicle_id`, `user_id`, `start_time`, `start_soc_pct`, and schema-v2 `payment_method`.
- `session_stop_stream`: Kafka key `session_id`; Avro values include `station_id`, `stop_time`, `end_soc_pct`, `energy_kwh`, and `stop_reason`.
- `stations_table`: Kafka key `station_id`; reference metadata includes `station_name`, `city`, `operator`, `tariff_per_kwh`, and status information.
- `queryable_stations_table`: materialized state from `stations_table`, used for keyed lookups and the enrichment join.

## Part E.2 — Completed sessions

`completed_sessions` is created by an **INNER JOIN** between session starts and stops on `session_id`, with a `WITHIN 2 HOURS` window. It calculates `duration_minutes = (stop_time - start_time) / 60000.0` using the payload's epoch-millisecond fields. The join window itself uses ksqlDB/Kafka record timestamps unless explicitly configured otherwise; it does **not** automatically use the payload fields. Only matched sessions appear in this stream.

**Observed:** five sampled sessions had durations of 43, 53, 74, 21, and 18 minutes, with corresponding energy readings.

## Part E.3 — Enrichment

`completed_sessions_by_station` repartitions completed sessions by `station_id` into six partitions. `enriched_sessions` then **LEFT JOINs** this stream with `queryable_stations_table` on `station_id`. The left join preserves completed sessions even if station metadata is absent; such records can have null metadata or `total_cost`. The cost formula is `energy_kwh * tariff_per_kwh`.

**Observed:** `SESS-000110` at `ST-NYC-001` had 39.0 kWh, a tariff of 0.49 per kWh, and a calculated cost of 19.11. Sampled enrichment records contained populated city and operator fields.

## Part E.4 — Windowed aggregations

`hourly_city_metrics` uses **one-hour tumbling windows** to compute completed-session count, total energy, and total revenue per city. Tumbling windows do not overlap.

`station_activity_metrics` uses **30-minute hopping windows advancing every 10 minutes** to compute the same measures per station. Hopping windows overlap, so a session can be counted in multiple windows; sums across overlapping windows should not be interpreted as unique-session totals.

`WINDOWSTART` and `WINDOWEND` are ksqlDB system columns identifying window boundaries in epoch milliseconds. They are not Python or Kafka methods. The window assignments in this implementation use Kafka record timestamps, not the simulated payload `start_time`/`stop_time` fields.

### Observed hourly city metrics

These figures are from the verified ksqlDB result shown during the assignment run; they are not fixed expected values for future replays.

| City | Completed sessions | Energy (kWh) | Revenue (approx.) |
| --- | ---: | ---: | ---: |
| Austin | 86 | 2460.00 | 1130.84 |
| San Francisco | 92 | 2584.50 | 1097.43 |
| Chicago | 102 | 2720.25 | 925.95 |
| Boston | 78 | 2095.50 | 907.57 |
| New York | 104 | 2810.25 | 1154.96 |
| **Total** | **462** | **12670.50** | **5216.75** |

The five city session counts total 462, consistent with the producer's 462 stop events in this run. These results alone do not prove exactly-once behavior or guarantee identical totals after replaying duplicate inputs.

### Observed hopping-window samples

- `ST-SFO-001`: 35 sessions, 1016.25 kWh, approximately 548.78 revenue.
- `ST-CHI-002`: 26 sessions, 662.25 kWh, approximately 178.81 revenue; appeared in multiple overlapping windows.
- `ST-NYC-003`: 29 sessions, 819.75 kWh, approximately 418.07 revenue.

## Validation commands

Run separately in the ksqlDB editor. Use the **Earliest** offset setting for push queries when inspecting retained output topics.

```sql
DESCRIBE session_start_stream;
DESCRIBE session_stop_stream;
DESCRIBE stations_table;
DESCRIBE completed_sessions_by_station;
PRINT 'charging_stations' FROM BEGINNING LIMIT 5;
SELECT * FROM queryable_stations_table WHERE station_id = 'ST-BOS-001';
SELECT session_id, station_id, duration_minutes, energy_kwh
FROM completed_sessions EMIT CHANGES LIMIT 5;
SELECT session_id, city, operator, energy_kwh, tariff_per_kwh, total_cost
FROM enriched_sessions EMIT CHANGES LIMIT 5;
SELECT * FROM hourly_city_metrics EMIT CHANGES LIMIT 5;
SELECT * FROM station_activity_metrics EMIT CHANGES LIMIT 10;
```

## Operational notes

- Kafka retention matters: the `session_stop` topic's earlier records expired after seven days and were regenerated with the v1 producer. Retained source records are necessary for historical replay.
- Creating materialized tables and streams via CTAS/CSAS creates persistent queries and output Kafka topics; they consume resources while running.
- Query results depend on the order in which reference table state and stream records are processed. Check null enrichment fields rather than assuming every row is populated.
- The ksqlDB cluster and persistent queries may incur charges while running. Review resources and stop/delete them when no longer needed, after capturing assignment evidence.
- SQL file names document execution order. The `CREATE` statements are not idempotent and should not be rerun without checking existing objects.
