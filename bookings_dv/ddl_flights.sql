CREATE TABLE sl_dv.h_flights AS (
	SELECT md5((flight_no || scheduled_departure)::bytea) AS flights_hash_key,
		   CAST(CURRENT_TIMESTAMP(0) AS timestamp) AS load_date,
		   'my_soure' AS source_load,
		   flight_no,
		   scheduled_departure
	FROM bookings.flights
);
ALTER TABLE sl_dv.h_flights ADD PRIMARY KEY (flights_hash_key);

CREATE TABLE sl_dv.s_flights_flight_id AS (
	SELECT md5((flight_no || scheduled_departure)::bytea) AS flights_hash_key,
		   CAST(CURRENT_TIMESTAMP(0) AS timestamp) AS load_date,
		   '9999-12-31 23:59:59' :: timestamp AS load_end_date,
		   'my_soure' AS source_load,
		   flight_id
	FROM bookings.flights
);

ALTER TABLE sl_dv.s_flights_flight_id ADD PRIMARY KEY (flights_hash_key, load_date);
ALTER TABLE sl_dv.s_flights_flight_id ADD FOREIGN KEY (flights_hash_key) REFERENCES h_flights(flights_hash_key);

CREATE TABLE sl_dv.s_flights_status AS (
	SELECT md5((flight_no || scheduled_departure)::bytea) AS flights_hash_key,
		   CAST(CURRENT_TIMESTAMP(0) AS timestamp) AS load_date,
		   '9999-12-31 23:59:59' :: timestamp AS load_end_date,
		   'my_soure' AS source_load,
		   status
	FROM bookings.flights
);

ALTER TABLE sl_dv.s_flights_status ADD PRIMARY KEY (flights_hash_key, load_date);
ALTER TABLE sl_dv.s_flights_status ADD FOREIGN KEY (flights_hash_key) REFERENCES h_flights(flights_hash_key);

CREATE TABLE sl_dv.s_flights_scheduled_arrival AS (
	SELECT md5((flight_no || scheduled_departure)::bytea) AS flights_hash_key,
		   CAST(CURRENT_TIMESTAMP(0) AS timestamp) AS load_date,
		   '9999-12-31 23:59:59' :: timestamp AS load_end_date,
		   'my_soure' AS source_load,
		   scheduled_arrival
	FROM bookings.flights
);

ALTER TABLE sl_dv.s_flights_scheduled_arrival ADD PRIMARY KEY (flights_hash_key, load_date);
ALTER TABLE sl_dv.s_flights_scheduled_arrival ADD FOREIGN KEY (flights_hash_key) REFERENCES h_flights(flights_hash_key);

CREATE TABLE sl_dv.s_flights_actual_departure AS (
	SELECT md5((flight_no || scheduled_departure)::bytea) AS flights_hash_key,
		   CAST(CURRENT_TIMESTAMP(0) AS timestamp) AS load_date,
		   '9999-12-31 23:59:59' :: timestamp AS load_end_date,
		   'my_soure' AS source_load,
		   actual_departure
	FROM bookings.flights
);

ALTER TABLE sl_dv.s_flights_actual_departure ADD PRIMARY KEY (flights_hash_key, load_date);
ALTER TABLE sl_dv.s_flights_actual_departure ADD FOREIGN KEY (flights_hash_key) REFERENCES h_flights(flights_hash_key);

CREATE TABLE sl_dv.s_flights_actual_arrival AS (
	SELECT md5((flight_no || scheduled_departure)::bytea) AS flights_hash_key,
		   CAST(CURRENT_TIMESTAMP(0) AS timestamp) AS load_date,
		   '9999-12-31 23:59:59' :: timestamp AS load_end_date,
		   'my_soure' AS source_load,
		   actual_arrival
	FROM bookings.flights
);

ALTER TABLE sl_dv.s_flights_actual_arrival ADD PRIMARY KEY (flights_hash_key, load_date);
ALTER TABLE sl_dv.s_flights_actual_arrival ADD FOREIGN KEY (flights_hash_key) REFERENCES h_flights(flights_hash_key);