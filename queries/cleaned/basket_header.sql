MERGE INTO `sandbox-athevenot.cleaned.basket_header` T
USING (
  SELECT
    id_basket_header,
    id_store,
    id_cash_desk,
    id_customer,
    (SELECT SUM(quantity) FROM UNNEST(detail))              AS `n_product`,
    (SELECT COUNT(product_name) FROM UNNEST(detail))        AS `n_product_distinct`,
    (SELECT SUM(quantity * unit_price) FROM UNNEST(detail)) AS `total_price`,
    payment_mode,
    purchase_date,
    update_time,
    insertion_time
  FROM `sandbox-athevenot.staging.basket`
) S
ON S.id_basket_header = T.id_basket_header
WHEN MATCHED /*AND T.update_time < S.update_time*/ THEN 
  UPDATE SET
    T.n_product = S.n_product,
    T.n_product_distinct = S.n_product_distinct,
    T.total_price = S.total_price,
    T.payment_mode = S.payment_mode,
    T.update_time = S.update_time,
    T.insertion_time = S.insertion_time
WHEN NOT MATCHED BY TARGET THEN 
INSERT (
  id_basket_header,
  id_store,
  id_cash_desk,
  id_customer,
  n_product,
  n_product_distinct,
  total_price,
  payment_mode,
  purchase_date,
  creation_time,
  update_time,
  insertion_time
) VALUES (
  S.id_basket_header,
  S.id_store,
  S.id_cash_desk,
  S.id_customer,
  S.n_product,
  S.n_product_distinct,
  S.total_price,
  S.payment_mode,
  S.purchase_date,
  S.update_time,
  S.update_time,
  S.insertion_time
);
