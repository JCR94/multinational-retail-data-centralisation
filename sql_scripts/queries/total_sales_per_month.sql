SELECT	ddt.month,
		CAST(ROUND(SUM(ot.product_quantity * dp.product_price)::numeric,2) AS FLOAT) AS total_sales
FROM orders_table AS ot
JOIN dim_date_times AS ddt ON ot.date_uuid = ddt.date_uuid
JOIN dim_products AS dp ON ot.product_code = dp.product_code
GROUP BY ddt.month
ORDER BY total_sales DESC
LIMIT 6