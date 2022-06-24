-- CREATE OR REPLACE TABLE `sandbox-athevenot.aggregated.day_sale`
SELECT
  CAST(purchase_date AS DATE) AS `day`,
  SUM(n_product)              AS `total_product`,
  ROUND(SUM(total_price), 2)  AS `total_sale`
FROM `sandbox-athevenot.cleaned.basket_header`
GROUP BY 
  day
ORDER BY 
  day DESC
;