# Question 3 SQL query:
SELECT
    count(*)
FROM
    green_taxi_trips t
WHERE
	lpep_pickup_datetime >= '2025-11-01' and lpep_pickup_datetime < '2025-12-01'
	and trip_distance <= 1.00;

Answer: 8007

#Question 4 query:

SELECT * 
FROM 
green_taxi_trips t, 
(
    SELECT max(trip_distance) as max_distance
    FROM green_taxi_trips
    WHERE trip_distance <100
) as max_trip
WHERE t.trip_distance = max_trip.max_distance

Answer: "lpep_pickup_datetime" "2025-11-14 15:36:27"

#Question 5 query:

SELECT 
    z."Zone", 
    SUM(t."total_amount") AS total_earned
FROM 
    green_taxi_trips t
JOIN 
    green_taxi_zone z ON t."PULocationID" = z."LocationID"
WHERE 
    t."lpep_pickup_datetime" >= '2025-11-18 00:00:00' 
    AND t."lpep_pickup_datetime" < '2025-11-19 00:00:00'
GROUP BY 
    z."Zone"
ORDER BY 
    total_earned DESC
LIMIT 1;

Answer: "East Harlem North"

Question 6 query:

SELECT 
    do_zone."Zone" AS dropoff_zone,
    t."tip_amount"
FROM 
    green_taxi_trips t
JOIN 
    green_taxi_zone pu_zone ON t."PULocationID" = pu_zone."LocationID"
JOIN 
    green_taxi_zone do_zone ON t."DOLocationID" = do_zone."LocationID"
WHERE 
    pu_zone."Zone" = 'East Harlem North'
    AND t."lpep_pickup_datetime" >= '2025-11-01 00:00:00'
    AND t."lpep_pickup_datetime" < '2025-12-01 00:00:00'
ORDER BY 
    t."tip_amount" DESC
LIMIT 1;

Answer: "dropoff_zone"	"tip_amount"
"Yorkville West"	81.89


## HOMEWORK 2
Question 1. Executions - Extract - outputFiles - yellow_tripdata_2020-12.csv - 128.3 MB
Answer: 128.3 MB

Question 2. Answer: green_tripdata_2020-04.csv

Question 3. 
    SELECT count(*) 
    FROM public.yellow_tripdata 
Answer: 24,648,499

Question 4. 
    SELECT count(*) 
    FROM public.green_tripdata 
Answer: 1,734,051


Question 5. 
query: 
    SELECT COUNT(*) 
    FROM public.yellow_tripdata 
    WHERE filename = 'yellow_tripdata_2021-03.csv'

Answer: 1925152

SELECT COUNT(*)
FROM `authentic-host-485219-t5.zoomcamp.yellow_tripdata_non_partitioned`
This query will process 0 B when run.

ANSWER: The result of this query is the number of rows in the given table, Big Query store this number as metadata so there is no need to scan all the table. 
SELECT COUNT(*)
FROM `authentic-host-485219-t5.zoomcamp.yellow_tripdata_non_partitioned`
This query will process 0 B when run.

ANSWER: The result of this query is the number of rows in the given table, Big Query store this number as metadata so there is no need to scan all the table. 

Question 6. Answer: Add a timezone property America/New_York


# HOMEWORK 3

Create an external table query:

```
CREATE OR REPLACE EXTERNAL TABLE `authentic-host-485219-t5.zoomcamp.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://olgas-zoomcamp/yellow_tripdata_2024-*.parquet']
);
```

Create materialized non-partitioned table:
```
CREATE OR REPLACE TABLE `authentic-host-485219-t5.zoomcamp.yellow_tripdata_non_partitioned` AS 
SELECT * 
FROM `authentic-host-485219-t5.zoomcamp.external_yellow_tripdata`;
```

### QUESTION 1

**Query:**
Count records for the yellow_tripdata table
```
SELECT COUNT(*)
FROM `authentic-host-485219-t5.zoomcamp.external_yellow_tripdata`
LIMIT 1;
```
**Answer:**
20332093

### QUESTION 2

**Query:**
Count distinct number of PULocationIDs for external and materialized tables 

```
SELECT COUNT( DISTINCT (PULocationID))
FROM `authentic-host-485219-t5.zoomcamp.external_yellow_tripdata`;
```
result: ~0MB

```
SELECT COUNT( DISTINCT (PULocationID)) 
FROM `authentic-host-485219-t5.zoomcamp.yellow_tripdata_non_partitioned`;
```
result: ~155.12MB

**Answer:** 
0 MB for the External Table and 155.12 MB for the Materialized Table


### QUESTION 3
The estimate number for 2 queries are different because the BigQuery is a columnar based database and it ignores the columns that are excluded from processing when runs a query.

Retrieve the PULocationID from the `authentic-host-485219-t5.zoomcamp.yellow_tripdata_non_partitioned`
#retrieve the PULocationID and DOLocationID from the same table.

```
SELECT PULocationID
FROM `authentic-host-485219-t5.zoomcamp.yellow_tripdata_non_partitioned`;
```
Result: ~155.12MB

```
SELECT PULocationID, DOLocationID
FROM `authentic-host-485219-t5.zoomcamp.yellow_tripdata_non_partitioned`;
```
Result: ~310.24MB

**Answer:**
BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.


### QUESTION 4

**Query:**
```
    SELECT COUNT(*) 
    FROM `authentic-host-485219-t5.zoomcamp.yellow_tripdata_non_partitioned`
    WHERE fare_amount = 0;
```
**Answer:**
8333

### QUESTION 5

**Query:**
```
CREATE OR REPLACE TABLE `authentic-host-485219-t5.zoomcamp.yellow_tripdata_optimized`
PARTITION BY DATE (tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `authentic-host-485219-t5.zoomcamp.yellow_tripdata_non_partitioned`;
```

**Answer:**
Partition by tpep_dropoff_datetime and Cluster on VendorID

### QUESTION 6

**Query:**
```
SELECT DISTINCT(VendorID)
FROM `authentic-host-485219-t5.zoomcamp.yellow_tripdata_non_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
```

Materialized non-partitioned table: This query will process 310.24 MB when run.

```
SELECT DISTINCT(VendorID)
FROM `authentic-host-485219-t5.zoomcamp.yellow_tripdata_optimized`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
```

Optimized table partitioned by date: This query will process 26.84 MB when run.

**Answer:** 
310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

### QUESTION 7
**Answer:** GCP Bucket

### QUESTION 8 
**Answer:** 
False. Because clasterization depends on a table usage, it's great if queries will be using filters (like where clause) or aggregations (like group by).

### QUESTION 9
**Query:**
```
SELECT COUNT(*)
FROM `authentic-host-485219-t5.zoomcamp.yellow_tripdata_non_partitioned`
```
This query will process 0 B when run.

**Answer:** 
The result of this query is the number of rows in the given table, Big Query store this number as metadata so there is no need to scan all the table. 
