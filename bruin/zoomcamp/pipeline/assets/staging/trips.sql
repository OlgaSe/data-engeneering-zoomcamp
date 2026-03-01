/* @bruin

name: staging.trips
type: bq.sql

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

depends:
  - ingestion.trips
  - ingestion.payment_lookup

columns:
  - name: pickup_datetime
    type: timestamp
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
  - name: pickup_location_id
    type: integer
  - name: dropoff_location_id
    type: integer
  - name: fare_amount
    type: float
  - name: taxi_type
    type: string
  - name: payment_type_name
    type: string

custom_checks:
  - name: row_count_greater_than_zero
    value: 1
    query: |-
      SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
      FROM staging.trips

@bruin */

SELECT
    t.pickup_datetime,
    t.dropoff_datetime,
    t.pickup_location_id,
    t.dropoff_location_id,
    t.fare_amount,
    t.taxi_type,
    p.payment_type_name
FROM ingestion.trips t
LEFT JOIN ingestion.payment_lookup p
    ON t.payment_type = p.payment_type_id
WHERE t.pickup_datetime >= '{{ start_datetime }}'
  AND t.pickup_datetime < '{{ end_datetime }}'
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY t.pickup_datetime, t.dropoff_datetime,
                 t.pickup_location_id, t.dropoff_location_id, t.fare_amount
    ORDER BY t.pickup_datetime
) = 1;


-- WITH

-- normalized_trips AS ( -- Normalize column names from raw data (cast, coalesce, rename)
--   SELECT
--     vendorid,
--     CAST(COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) AS TIMESTAMP) AS pickup_time,
--     CAST(COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime) AS TIMESTAMP) AS dropoff_time,
--     passenger_count,
--     trip_distance,
--     store_and_fwd_flag,
--     pulocationid AS pickup_location_id,
--     dolocationid AS dropoff_location_id,
--     CAST(payment_type AS INTEGER) AS payment_type,
--     fare_amount,
--     extra,
--     mta_tax,
--     tip_amount,
--     tolls_amount,
--     improvement_surcharge,
--     total_amount,
--     congestion_surcharge,
--     airport_fee,
--     taxi_type,
--     extracted_at,
--   FROM raw.trips_raw
--   WHERE 1=1
--     AND DATE_TRUNC('month', CAST(COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) AS TIMESTAMP)) BETWEEN DATE_TRUNC('month', CAST('{{ start_datetime }}' AS TIMESTAMP)) AND DATE_TRUNC('month', CAST('{{ end_datetime }}' AS TIMESTAMP))
--     AND COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) IS NOT NULL
--     AND COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime) IS NOT NULL
--     AND pulocationid IS NOT NULL
--     AND dolocationid IS NOT NULL
--     AND taxi_type IS NOT NULL
-- )

-- , enriched_trips AS ( -- Enrich trips with location and payment information using LEFT JOINs
--   SELECT
--     ct.pickup_time,
--     ct.dropoff_time,
--     EXTRACT(EPOCH FROM (ct.dropoff_time - ct.pickup_time)) AS trip_duration_seconds,
--     ct.pickup_location_id,
--     ct.dropoff_location_id,
--     ct.taxi_type,
--     ct.trip_distance,
--     ct.passenger_count,
--     ct.fare_amount,
--     ct.tip_amount,
--     ct.total_amount,
--     pl.borough AS pickup_borough,
--     pl.zone AS pickup_zone,
--     dl.borough AS dropoff_borough,
--     dl.zone AS dropoff_zone,
--     ct.payment_type,
--     pmt.payment_description,
--     ct.extracted_at,
--     CURRENT_TIMESTAMP AS updated_at,
--   FROM normalized_trips AS ct
--   LEFT JOIN raw.taxi_zone_lookup AS pl
--     ON ct.pickup_location_id = pl.location_id
--   LEFT JOIN raw.taxi_zone_lookup AS dl
--     ON ct.dropoff_location_id = dl.location_id
--   LEFT JOIN raw.payment_lookup AS pmt
--     ON ct.payment_type = pmt.payment_type_id
--   WHERE 1=1
--     -- filter out zero durations (trip cannot end at the same time it starts or before it starts)
--     AND EXTRACT(EPOCH FROM (ct.dropoff_time - ct.pickup_time)) > 0
--     -- filter out outlier durations that are too long, 8 hours (28800 seconds)
--     AND EXTRACT(EPOCH FROM (ct.dropoff_time - ct.pickup_time)) < 28800
--     -- filter out negative total amounts
--     AND ct.total_amount >= 0
--     -- Only include trips that were actually charged
--     AND pmt.payment_description IN ('flex_fare', 'credit_card', 'cash')
--     -- filter out negative trip distances as they are data quality issues (trip distance cannot be negative)
--     AND ct.trip_distance >= 0
--   QUALIFY ROW_NUMBER() OVER (
--     PARTITION BY
--       ct.pickup_time,
--       ct.dropoff_time,
--       ct.pickup_location_id,
--       ct.dropoff_location_id,
--       ct.taxi_type,
--       ct.trip_distance,
--       ct.passenger_count,
--       ct.fare_amount,
--       ct.tip_amount,
--       ct.total_amount,
--       ct.payment_type
--     ORDER BY ct.extracted_at DESC
--   ) = 1
-- )

-- SELECT
--   pickup_time,
--   dropoff_time,
--   pickup_location_id,
--   dropoff_location_id,
--   taxi_type,
--   trip_distance,
--   passenger_count,
--   fare_amount,
--   tip_amount,
--   total_amount,
--   pickup_borough,
--   pickup_zone,
--   dropoff_borough,
--   dropoff_zone,
--   trip_duration_seconds,
--   payment_type,
--   payment_description,
--   extracted_at,
--   updated_at,
-- FROM enriched_trips