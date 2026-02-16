/* @bruin

# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

# TODO: Set the asset name (recommended: staging.trips).
name: TODO_SET_ASSET_NAME
# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: TODO

# TODO: Declare dependencies so `bruin run ... --downstream` and lineage work.
# Examples:
# depends:
#   - ingestion.trips
#   - ingestion.payment_lookup
depends:
  - TODO_DEP_1
  - TODO_DEP_2

# TODO: Choose time-based incremental processing if the dataset is naturally time-windowed.
# - This module expects you to use `time_interval` to reprocess only the requested window.
materialization:
  # What is materialization?
  # Materialization tells Bruin how to turn your SELECT query into a persisted dataset.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  #
  # Materialization "type":
  # - table: persisted table
  # - view: persisted view (if the platform supports it)
  type: table
  # TODO: set a materialization strategy.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  # suggested strategy: time_interval
  #
  # Incremental strategies (what does "incremental" mean?):
  # Incremental means you update only part of the destination instead of rebuilding everything every run.
  # In Bruin, this is controlled by `strategy` plus keys like `incremental_key` and `time_granularity`.
  #
  # Common strategies you can choose from (see docs for full list):
  # - create+replace (full rebuild)
  # - truncate+insert (full refresh without drop/create)
  # - append (insert new rows only)
  # - delete+insert (refresh partitions based on incremental_key values)
  # - merge (upsert based on primary key)
  # - time_interval (refresh rows within a time window)
  strategy: TODO
  # TODO: set incremental_key to your event time column (DATE or TIMESTAMP).
  incremental_key: TODO_SET_INCREMENTAL_KEY
  # TODO: choose `date` vs `timestamp` based on the incremental_key type.
  time_granularity: TODO_SET_GRANULARITY

# TODO: Define output columns, mark primary keys, and add a few checks.
columns:
  - name: TODO_pk1
    type: TODO
    description: TODO
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: TODO_metric
    type: TODO
    description: TODO
    checks:
      - name: non_negative

# TODO: Add one custom check that validates a staging invariant (uniqueness, ranges, etc.)
# Docs: https://getbruin.com/docs/bruin/quality/custom
custom_checks:
  - name: TODO_custom_check_name
    description: TODO
    query: |
      -- TODO: return a single scalar (COUNT(*), etc.) that should match `value`
      SELECT 0
    value: 0

@bruin */

-- TODO: Write the staging SELECT query.
--
-- Purpose of staging:
-- - Clean and normalize schema from ingestion
-- - Deduplicate records (important if ingestion uses append strategy)
-- - Enrich with lookup tables (JOINs)
-- - Filter invalid rows (null PKs, negative values, etc.)
--
-- Why filter by {{ start_datetime }} / {{ end_datetime }}?
-- When using `time_interval` strategy, Bruin:
--   1. DELETES rows where `incremental_key` falls within the run's time window
--   2. INSERTS the result of your query
-- Therefore, your query MUST filter to the same time window so only that subset is inserted.
-- If you don't filter, you'll insert ALL data but only delete the window's data = duplicates.

-- SELECT *
-- FROM ingestion.trips
-- WHERE pickup_datetime >= '{{ start_datetime }}'
--   AND pickup_datetime < '{{ end_datetime }}'


WITH normalized_trips AS ( -- Normalize column names from raw data (cast, coalesce, rename)
  SELECT
    vendorid,
    CAST(COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) AS TIMESTAMP) AS pickup_time,
    CAST(COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime) AS TIMESTAMP) AS dropoff_time,
    passenger_count,
    trip_distance,
    store_and_fwd_flag,
    pulocationid AS pickup_location_id,
    dolocationid AS dropoff_location_id,
    CAST(payment_type AS INTEGER) AS payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    taxi_type,
    extracted_at,
  FROM raw.trips_raw
  WHERE 1=1
    AND DATE_TRUNC('month', CAST(COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) AS TIMESTAMP)) BETWEEN DATE_TRUNC('month', CAST('{{ start_datetime }}' AS TIMESTAMP)) AND DATE_TRUNC('month', CAST('{{ end_datetime }}' AS TIMESTAMP))
    AND COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) IS NOT NULL
    AND COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime) IS NOT NULL
    AND pulocationid IS NOT NULL
    AND dolocationid IS NOT NULL
    AND taxi_type IS NOT NULL
)

, enriched_trips AS ( -- Enrich trips with location and payment information using LEFT JOINs
  SELECT
    ct.pickup_time,
    ct.dropoff_time,
    EXTRACT(EPOCH FROM (ct.dropoff_time - ct.pickup_time)) AS trip_duration_seconds,
    ct.pickup_location_id,
    ct.dropoff_location_id,
    ct.taxi_type,
    ct.trip_distance,
    ct.passenger_count,
    ct.fare_amount,
    ct.tip_amount,
    ct.total_amount,
    pl.borough AS pickup_borough,
    pl.zone AS pickup_zone,
    dl.borough AS dropoff_borough,
    dl.zone AS dropoff_zone,
    ct.payment_type,
    pmt.payment_description,
    ct.extracted_at,
    CURRENT_TIMESTAMP AS updated_at,
  FROM normalized_trips AS ct
  LEFT JOIN raw.taxi_zone_lookup AS pl
    ON ct.pickup_location_id = pl.location_id
  LEFT JOIN raw.taxi_zone_lookup AS dl
    ON ct.dropoff_location_id = dl.location_id
  LEFT JOIN raw.payment_lookup AS pmt
    ON ct.payment_type = pmt.payment_type_id
  WHERE 1=1
    -- filter out zero durations (trip cannot end at the same time it starts or before it starts)
    AND EXTRACT(EPOCH FROM (ct.dropoff_time - ct.pickup_time)) > 0
    -- filter out outlier durations that are too long, 8 hours (28800 seconds)
    AND EXTRACT(EPOCH FROM (ct.dropoff_time - ct.pickup_time)) < 28800
    -- filter out negative total amounts
    AND ct.total_amount >= 0
    -- Only include trips that were actually charged
    AND pmt.payment_description IN ('flex_fare', 'credit_card', 'cash')
    -- filter out negative trip distances as they are data quality issues (trip distance cannot be negative)
    AND ct.trip_distance >= 0
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      ct.pickup_time,
      ct.dropoff_time,
      ct.pickup_location_id,
      ct.dropoff_location_id,
      ct.taxi_type,
      ct.trip_distance,
      ct.passenger_count,
      ct.fare_amount,
      ct.tip_amount,
      ct.total_amount,
      ct.payment_type
    ORDER BY ct.extracted_at DESC
  ) = 1
)

SELECT
  pickup_time,
  dropoff_time,
  pickup_location_id,
  dropoff_location_id,
  taxi_type,
  trip_distance,
  passenger_count,
  fare_amount,
  tip_amount,
  total_amount,
  pickup_borough,
  pickup_zone,
  dropoff_borough,
  dropoff_zone,
  trip_duration_seconds,
  payment_type,
  payment_description,
  extracted_at,
  updated_at,
FROM enriched_trips
