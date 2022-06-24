-- TRUNCATE TABLE `sandbox-athevenot.staging.basket_detail`;
INSERT INTO `sandbox-athevenot.staging.basket_detail`
SELECT 
  id_basket_header,
  detail.product_name,
  detail.quantity,
  detail.unit_price,
  purchase_date,
  update_time,
  insertion_time
FROM `sandbox-athevenot.staging.basket`
INNER JOIN UNNEST(detail) detail;
