/*  ______ONLY ONE QUERY______
*/

-- INSERT INTO `sandbox-athevenot.staging.basket`
WITH dedup_converted_basket AS (
  SELECT 
    CAST(SPLIT(id_cash_desk, '-')[SAFE_OFFSET(0)] AS INTEGER) AS `id_store`,
    CAST(SPLIT(id_cash_desk, '-')[SAFE_OFFSET(1)] AS INTEGER) AS `id_cash_desk`,
    id_customer,
    ARRAY( /* REALLY REALLY HARD ! */
      SELECT AS STRUCT 
        product_name, 
        SUM(quantity)                                        AS `quantity`,
        ROUND(SUM(quantity * unit_price) / SUM(quantity), 2) AS `unit_price`
      FROM UNNEST(detail)
      GROUP BY product_name
    )                                                         AS `detail`,
    CASE payment_mode WHEN "Cash" THEN "Cash" ELSE "Card" END AS `payment_mode`,
    PARSE_DATETIME("%d-%m-%Y %H:%M:%S", purchase_date)        AS `purchase_date`,
    update_time,
    CURRENT_TIMESTAMP()                                       AS `insertion_time`
  FROM `sandbox-athevenot.raw.basket` 
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY 
      id_store, 
      id_cash_desk, 
      id_customer, 
      purchase_date 
    ORDER BY 
      update_time DESC
  ) = 1
), identification_attempt_basket AS (
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
  FROM dedup_converted_basket b 
  LEFT JOIN `sandbox-athevenot.cleaned.basket_header` header 
   ON b.id_store      = header.id_store
  AND b.id_cash_desk  = header.id_cash_desk
  AND b.id_customer   = header.id_customer
  AND b.purchase_date = header.purchase_date
), max_id AS (
  SELECT 
    IFNULL(MAX(id_basket_header), 13) AS `max_id_basket_header`
  FROM `sandbox-athevenot.cleaned.basket_header`
)
SELECT 
  id_basket_header,
  id_store,
  id_cash_desk,
  id_customer,
  detail,
  payment_mode,
  purchase_date,
  update_time,
  insertion_time
FROM identification_attempt_basket 
WHERE id_basket_header IS NOT NULL
UNION ALL
SELECT 
  (
    SELECT 
      IFNULL(MAX(id_basket_header), 0) 
    FROM `sandbox-athevenot.cleaned.basket_header`
  ) + ROW_NUMBER() OVER() AS `id_basket_header`,
  id_store,
  id_cash_desk,
  id_customer,
  detail,
  payment_mode,
  purchase_date,
  update_time,
  insertion_time
FROM identification_attempt_basket
WHERE id_basket_header IS NULL;
