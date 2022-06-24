
/*. ______PROCEDURAL QUERY______
*/

DECLARE max_id_basket_header INT64;
SET max_id_basket_header = (SELECT IFNULL(MAX(id_basket_header), 0) FROM `sandbox-athevenot.cleaned.basket_header`);

CREATE OR REPLACE TABLE `sandbox-athevenot.staging.basket_temp` AS
SELECT 
  CAST(SPLIT(id_cash_desk, '-')[SAFE_OFFSET(0)] AS INTEGER) AS `id_store`,
  CAST(SPLIT(id_cash_desk, '-')[SAFE_OFFSET(1)] AS INTEGER) AS `id_cash_desk`,
  id_customer,
  detail,
  CASE payment_mode WHEN "Cash" THEN "Cash" ELSE "Card" END AS `payment_mode`,
  PARSE_DATETIME("%d-%m-%Y %H:%M:%S", purchase_date)        AS `purchase_date`,
  update_time,
  insertion_time
FROM `sandbox-athevenot.raw.basket`
QUALIFY ROW_NUMBER() OVER(
  PARTITION BY 
    id_store, 
    id_cash_desk, 
    id_customer, 
    purchase_date 
  ORDER BY 
    update_time DESC
) = 1; 

/* REALLY REALLY HARD ! */
UPDATE `sandbox-athevenot.staging.basket`
SET 
  detail = ARRAY(
    SELECT AS STRUCT 
      product_name, 
      SUM(quantity)                                        AS `quantity`,
      ROUND(SUM(quantity * unit_price) / SUM(quantity), 2) AS `unit_price`
    FROM UNNEST(detail)
    GROUP BY product_name
  ) 
WHERE (SELECT COUNT(product_name) FROM UNNEST(detail)) <> (SELECT COUNT(DISTINCT product_name) FROM UNNEST(detail));



CREATE OR REPLACE TABLE `sandbox-athevenot.staging.basket` AS
SELECT 
  header.id_basket_header,
  b.id_store,
  b.id_cash_desk,
  b.id_customer,
  b.detail,
  b.payment_mode,
  b.purchase_date,
  b.update_time,
  b.insertion_time
FROM `sandbox-athevenot.staging.basket_temp` b 
LEFT JOIN `sandbox-athevenot.cleaned.basket_header` header 
  ON b.id_store      = header.id_store
 AND b.id_cash_desk  = header.id_cash_desk
 AND b.id_customer   = header.id_customer
 AND b.purchase_date = header.purchase_date;

UPDATE `sandbox-athevenot.staging.basket` b1
SET 
    b1.id_basket_header = b2.id_basket_header
FROM (
    SELECT 
        id_store, 
        id_cash_desk,
        id_customer,
        purchase_date,
        max_id_basket_header + ROW_NUMBER() OVER () AS `id_basket_header`
    FROM `sandbox-athevenot.staging.basket`
    WHERE id_basket_header IS NULL
) b2
WHERE b1.id_store      = b2.id_store
  AND b1.id_cash_desk  = b2.id_cash_desk
  AND b1.id_customer   = b2.id_customer
  AND b1.purchase_date = b2.purchase_date;

DROP TABLE IF EXISTS `sandbox-athevenot.staging.basket_temp`;
