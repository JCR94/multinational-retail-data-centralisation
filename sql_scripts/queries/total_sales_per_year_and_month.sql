SELECT
	ddt.year,
	ddt.month,
	CAST(ROUND(SUM(ot.product_quantity * dp.product_price)::numeric,2) AS FLOAT) AS total_sales
FROM orders_table AS ot
JOIN dim_products AS dp ON ot.product_code = dp.product_code
JOIN dim_date_times AS ddt ON ot.date_uuid = ddt.date_uuid 
GROUP BY ddt.year, ddt.month
ORDER BY total_sales DESC
LIMIT 10