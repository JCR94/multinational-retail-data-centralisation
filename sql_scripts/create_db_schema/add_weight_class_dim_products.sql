ALTER TABLE dim_products
ADD weight_class TEXT;

UPDATE dim_products
SET weight_class =	CASE
						WHEN weight < 2 THEN 'Light'
						WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
						WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
						ELSE 'Truck_Required'
					END