SELECT
	dst.store_type,
	dst.country_code,
	CAST(ROUND(SUM(ot.product_quantity * dp.product_price)::numeric,2) AS FLOAT) AS total_sales
FROM orders_table AS ot
JOIN dim_store_details AS dst ON ot.store_code = dst.store_code
JOIN dim_products AS dp ON ot.product_code = dp.product_code
GROUP BY dst.store_type, dst.country_code
HAVING dst.country_code = 'DE'
ORDER BY total_sales