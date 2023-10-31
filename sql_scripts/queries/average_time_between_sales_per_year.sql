WITH sorted_dates AS
(
	SELECT
		year,
		date
	FROM dim_date_times
	ORDER BY date
),
sorted_dates_extended AS
(
	SELECT
		year,
		date,
		LEAD(date,1) OVER() AS next_date
	FROM sorted_dates
		ORDER BY date
)
SELECT
	year,
	AVG(next_date - date) AS actual_time_taken
FROM sorted_dates_extended
GROUP BY year
ORDER BY actual_time_taken DESC
LIMIT 5