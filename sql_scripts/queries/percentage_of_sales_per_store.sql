WITH total_sales_per_store_table AS
(
	SELECT
		dst.store_type,
		CAST(ROUND(SUM(ot.product_quantity * dp.product_price)::numeric,2) AS FLOAT) AS total_sales
	FROM orders_table AS ot
	JOIN dim_store_details AS dst ON ot.store_code = dst.store_code
	JOIN dim_products AS dp ON ot.product_code = dp.product_code
	GROUP BY dst.store_type
),
total_sales_table AS
(
	SELECT SUM(total_sales) as total_sales
	FROM total_sales_per_store_table
)
SELECT
	store_type,
	total_sales,
	CAST(ROUND((total_sales*100/(SELECT * FROM total_sales_table))::numeric,2) AS FLOAT) AS percentage_total
FROM total_sales_per_store_table
ORDER BY total_sales DESC