
select
	
	sum(final_price) as category_revenue,
	discount_in_percent,
	category,
	payment_method,
    -- 1. Get the grand total using a window function
    SUM(SUM(final_price)) OVER () AS total_revenue,
    -- 2. Divide category revenue by grand total and multiply by 100
    ROUND(100.0 * SUM(final_price) / SUM(SUM(final_price)) OVER (), 2) AS contribution_percentage,
    case 
			when discount_in_percent = 0 then 'No discount'
			when discount_in_percent <= 10 then 'Low discount'
			when discount_in_percent = 25 then 'Medium discount'
	ELSE 'High discountee'
	END AS discount_ranges
 


FROM ecom_data
group by category, payment_method, discount_in_percent
order by contribution_percentage
LIMIT 100;
