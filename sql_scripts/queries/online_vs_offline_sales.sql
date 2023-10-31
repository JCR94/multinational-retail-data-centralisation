WITH online_locality AS
(
	SELECT
		store_code,
		CASE
			WHEN store_type = 'Web Portal' THEN 'Web'
			ELSE 'Offline'
		END AS online_locality
	FROM dim_store_details
)

SELECT	ol.online_locality AS locality,
		COUNT(ot.index) AS number_of_sales,
		SUM(ot.product_quantity) AS product_quantity_count
FROM orders_table AS ot
JOIN online_locality AS ol ON ot.store_code = ol.store_code
GROUP BY ol.online_locality
ORDER BY locality DESC