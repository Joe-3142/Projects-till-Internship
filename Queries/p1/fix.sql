-- This is needed to fix the initial issue with the purchase_date column in the staging_ecom_data table. The column is currently of type date, but it contains values in different formats (YYYY-MM-DD and DD-MM-YYYY). To handle this, we will change the column type to text and then convert the values to a consistent date format during the insertion into the ecom_data table.

ALTER TABLE staging_ecom_data ALTER COLUMN purchase_date TYPE text;

SELECT
  user_id,
  product_id,
  category,
  price,
  discount_in_percent,
  final_price,
  payment_method,
  to_date(purchase_date, 'DD-MM-YYYY') AS purchase_date
FROM staging_ecom_data;

SELECT
  user_id,
  product_id,
  category,
  price,
  discount_in_percent,
  final_price,
  payment_method,
  CASE
    WHEN purchase_date ~ '^\d{4}-\d{2}-\d{2}$' 
      THEN to_date(purchase_date, 'YYYY-MM-DD')
    WHEN purchase_date ~ '^\d{2}-\d{2}-\d{4}$' 
      THEN to_date(purchase_date, 'DD-MM-YYYY')
    ELSE NULL
  END AS purchase_date
FROM staging_ecom_data;

SELECT
  CASE
    WHEN purchase_date ~ '^\d{4}-\d{2}-\d{2}$' THEN 'ISO (YYYY-MM-DD)'
    WHEN purchase_date ~ '^\d{2}-\d{2}-\d{4}$' THEN 'DMY (DD-MM-YYYY)'
    ELSE 'UNMATCHED: ' || purchase_date
  END AS format_type,
  count(*)
FROM staging_ecom_data
GROUP BY 1
ORDER BY 2 DESC;


BEGIN;

TRUNCATE TABLE ecom_data RESTART IDENTITY;

INSERT INTO ecom_data (
  user_id, product_id, category, price,
  discount_in_percent, final_price, payment_method, purchase_date
)
SELECT
  user_id,
  product_id,
  category,
  price,
  discount_in_percent,
  final_price,
  payment_method,
  CASE
    WHEN purchase_date ~ '^\d{4}-\d{2}-\d{2}$' 
      THEN to_date(purchase_date, 'YYYY-MM-DD')
    WHEN purchase_date ~ '^\d{2}-\d{2}-\d{4}$' 
      THEN to_date(purchase_date, 'DD-MM-YYYY')
    ELSE NULL
  END AS purchase_date
FROM staging_ecom_data;

COMMIT;

SELECT count(*) FROM ecom_data;
SELECT * FROM ecom_data ORDER BY purchase_date LIMIT 10;
