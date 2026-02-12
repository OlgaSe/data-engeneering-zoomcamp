{{ config(materialized='view') }}

select
    -- identifiers
    dispatching_base_num,
    cast(pulocationid as integer) as pickup_location_id,
    cast(dolocationid as integer) as dropoff_location_id,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    SR_Flag as sr_flag,
    Affiliated_base_number as affiliated_base_number

from {{ source('raw', 'fhv_tripdata') }}
where dispatching_base_num is not null