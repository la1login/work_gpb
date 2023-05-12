DROP VIEW IF EXISTS mart_flights;
CREATE VIEW mart_flights AS (
	SELECT s_id.flight_id,
		   h.flight_no, h.scheduled_departure, 
		   s_sa.scheduled_arrival,
		   s_ad.actual_departure,
		   s_aa.actual_arrival,
		   s_s.status
	FROM sl_dv.h_flights AS h
	INNER JOIN sl_dv.s_flights_flight_id AS s_id
		ON h.flights_hash_key = s_id.flights_hash_key
	INNER JOIN sl_dv.s_flights_status AS s_s
		ON h.flights_hash_key = s_s.flights_hash_key
	INNER JOIN sl_dv.s_flights_scheduled_arrival AS s_sa
		ON h.flights_hash_key = s_sa.flights_hash_key
	INNER JOIN sl_dv.s_flights_actual_departure AS s_ad
		ON h.flights_hash_key = s_ad.flights_hash_key
	INNER JOIN sl_dv.s_flights_actual_arrival AS s_aa
		ON h.flights_hash_key = s_aa.flights_hash_key
);







