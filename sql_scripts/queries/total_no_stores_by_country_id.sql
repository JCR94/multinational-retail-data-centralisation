SELECT	country_code,
		COUNT(store_code) AS total_no_stores
FROM dim_store_details
GROUP BY country_code