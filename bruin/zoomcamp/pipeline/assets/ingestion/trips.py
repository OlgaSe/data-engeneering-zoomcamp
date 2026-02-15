"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.11

# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # TODO: pick a strategy.
  # suggested strategy: append
  strategy: append

# TODO: Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns

columns:
  - name: vendorid
    type: DOUBLE
    description: A code indicating the TPEP provider that provided the record
  - name: tpep_pickup_datetime
    type: TIMESTAMP
    description: The date and time when the meter was engaged (yellow taxis only)
  - name: lpep_pickup_datetime
    type: TIMESTAMP
    description: The date and time when the meter was engaged (green taxis only)
  - name: tpep_dropoff_datetime
    type: TIMESTAMP
    description: The date and time when the meter was disengaged (yellow taxis only)
  - name: lpep_dropoff_datetime
    type: TIMESTAMP
    description: The date and time when the meter was disengaged (green taxis only)
  - name: pulocationid
    type: INTEGER
    description: TLC Taxi Zone in which the taximeter was engaged
  - name: dolocationid
    type: INTEGER
    description: TLC Taxi Zone in which the taximeter was disengaged
  - name: taxi_type
    type: VARCHAR
    description: Type of taxi (yellow or green)
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when the data was extracted from the source
  - name: passenger_count
    type: DOUBLE
    description: The number of passengers in the vehicle (entered by the driver)
  - name: trip_distance
    type: DOUBLE
    description: The elapsed trip distance in miles reported by the taximeter
  - name: store_and_fwd_flag
    type: VARCHAR
    description: This flag indicates whether the trip record was held in vehicle memory before sending to the vendor
  - name: payment_type
    type: DOUBLE
    description: A numeric code signifying how the passenger paid for the trip
  - name: fare_amount
    type: DOUBLE
    description: The time-and-distance fare calculated by the meter
  - name: extra
    type: DOUBLE
    description: Miscellaneous extras and surcharges
  - name: mta_tax
    type: DOUBLE
    description: $0.50 MTA tax that is automatically triggered based on the metered rate in use
  - name: tip_amount
    type: DOUBLE
    description: Tip amount (automatically populated for credit card tips, manually entered for cash tips)
  - name: tolls_amount
    type: DOUBLE
    description: Total amount of all tolls paid in trip
  - name: improvement_surcharge
    type: DOUBLE
    description: $0.30 improvement surcharge assessed on hailed trips at the flag drop
  - name: total_amount
    type: DOUBLE
    description: The total amount charged to passengers (does not include cash tips)
  - name: congestion_surcharge
    type: DOUBLE
    description: Congestion surcharge for trips that start, end or pass through the Manhattan Central Business District
  - name: airport_fee
    type: DOUBLE
    description: Airport fee for trips that start or end at an airport

@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python


# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
import pandas as pd
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
import io
import os
import json

def materialize():
    """
    TODO: Implement ingestion using Bruin runtime context.

    Required Bruin concepts to use here:
    - Built-in date window variables:
      - BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD)
      - BRUIN_START_DATETIME / BRUIN_END_DATETIME (ISO datetime)
      Docs: https://getbruin.com/docs/bruin/assets/python#environment-variables
    - Pipeline variables:
      - Read JSON from BRUIN_VARS, e.g. `taxi_types`
      Docs: https://getbruin.com/docs/bruin/getting-started/pipeline-variables

    Design TODOs (keep logic minimal, focus on architecture):
    - Use start/end dates + `taxi_types` to generate a list of source endpoints for the run window.
    - Fetch data for each endpoint, parse into DataFrames, and concatenate.
    - Add a column like `extracted_at` for lineage/debugging (timestamp of extraction).
    - Prefer append-only in ingestion; handle duplicates in staging.
    """
    # return final_dataframe


