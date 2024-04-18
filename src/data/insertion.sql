-- Insertion des données dans dim_trip
INSERT INTO public.dim_trip (tpep_pickup_date, tpep_pickup_time, passenger_count, trip_distance, fare_amount, payment_type)
SELECT 
    DATE(tpep_pickup_datetime) AS tpep_pickup_date,
    CAST(EXTRACT(HOUR FROM tpep_pickup_datetime) || ':' || EXTRACT(MINUTE FROM tpep_pickup_datetime) || ':' || EXTRACT(SECOND FROM tpep_pickup_datetime) AS TIME) AS tpep_pickup_time,
    passenger_count,
    trip_distance,
    fare_amount,
    payment_type
FROM 
    nyc_raw;

-- Insertion des données dans dim_location
INSERT INTO public.dim_location (pulocationid, dolocationid)
SELECT 
    pulocationid,
    dolocationid
FROM 
    nyc_raw;

-- Insertion des données dans dim_time
INSERT INTO public.dim_time (pickup_datetime)
SELECT 
    tpep_pickup_datetime
FROM 
    nyc_raw;
