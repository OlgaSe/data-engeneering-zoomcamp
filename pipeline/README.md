# Question 3 SQL query:
SELECT
    count(*)
FROM
    green_taxi_trips t
WHERE
	lpep_pickup_datetime >= '2025-11-01' and lpep_pickup_datetime < '2025-12-01'
	and trip_distance <= 1.00;

#QUestion 4 query:

SELECT * 
FROM 
green_taxi_trips t, 
(
    SELECT max(trip_distance) as max_distance
    FROM green_taxi_trips
    WHERE trip_distance <100
) as max_trip
WHERE t.trip_distance = max_trip.max_distance
