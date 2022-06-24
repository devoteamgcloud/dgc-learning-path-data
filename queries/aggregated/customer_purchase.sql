-- CREATE OR REPLACE TABLE `sandbox-athevenot.aggregated.customer_purchase`
SELECT
  -- ANY_VALUE(c.first_name) AS `first_name`, 
  -- ANY_VALUE(c.last_name)  AS `last_name`, 
  COUNT(b.id_basket_header)    AS `n_basket`,
  ROUND(SUM(b.total_price), 2) AS `total_purchase`,
  MIN(purchase_date)           AS `first_purchase_date`,
  MAX(purchase_date)           AS `last_purchase_date`
FROM `sandbox-athevenot.cleaned.customer` c
INNER JOIN `sandbox-athevenot.cleaned.basket_header` b ON c.id_customer = b.id_customer
GROUP BY 
  c.id_customer
ORDER BY 
  total_purchase DESC;
