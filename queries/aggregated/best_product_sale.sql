-- CREATE OR REPLACE TABLE `sandbox-athevenot.aggregated.best_product_sale`
SELECT
  product_name,
  DENSE_RANK() OVER(ORDER BY SUM(quantity))              AS `rank_in_quantity`,
  DENSE_RANK() OVER(ORDER BY SUM(quantity * unit_price)) AS `rank_in_sale`,
  SUM(quantity)                                          AS `total_quantity`,
  ROUND(SUM(quantity * unit_price), 2)                   AS `total_sale`
FROM `sandbox-athevenot.cleaned.basket_detail`
GROUP BY 
  product_name
ORDER BY 
  rank_in_quantity,
  rank_in_sale;
