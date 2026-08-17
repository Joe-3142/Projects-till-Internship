
SELECT 
    category,
    ROUND(AVG(CASE WHEN discount_in_percent > 0 THEN final_price END), 2) AS discounted_aov,
    ROUND(AVG(CASE WHEN discount_in_percent = 0 OR discount_in_percent IS NULL THEN final_price END), 2) AS non_discounted_aov,
    -- Lift percentage: How much higher/lower is discounted AOV compared to regular AOV?
    ROUND(100.0 * (
        AVG(CASE WHEN discount_in_percent > 0 THEN final_price END) - 
        AVG(CASE WHEN discount_in_percent = 0 OR discount_in_percent IS NULL THEN final_price END)
    ) / AVG(CASE WHEN discount_in_percent = 0 OR discount_in_percent IS NULL THEN final_price END), 2) AS lift_percentage
FROM ecom_data
GROUP BY category
ORDER BY lift_percentage DESC;
