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

Question 6.
Answer: Add a timezone property America/New_York
