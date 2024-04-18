-- Table: public.dim_trip

CREATE TABLE IF NOT EXISTS public.dim_trip
(
    trip_id SERIAL PRIMARY KEY,
    tpep_pickup_date date,
    tpep_pickup_time time,
    passenger_count integer,
    trip_distance double precision,
    fare_amount double precision,
    payment_type bigint
);

-- Table: public.dim_location

CREATE TABLE IF NOT EXISTS public.dim_location
(
    location_id SERIAL PRIMARY KEY,
    pulocationid integer,
    dolocationid integer
);

-- Table: public.dim_time

CREATE TABLE IF NOT EXISTS public.dim_time
(
    pickup_time_id SERIAL PRIMARY KEY,
    pickup_datetime timestamp without time zone
);
