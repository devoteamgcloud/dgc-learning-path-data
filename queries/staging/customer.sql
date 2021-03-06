-- INSERT INTO `sandbox-athevenot.staging.customer`
SELECT 
  id_customer,
  first_name,
  UPPER(last_name)                      AS `last_name`,
  email,
  PARSE_DATE("%d-%b-%y", creation_date) AS `creation_date`,
  update_time,
  CURRENT_TIMESTAMP()                   AS `insertion_time`
FROM `sandbox-athevenot.raw.customer`
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY id_customer
  ORDER BY update_time DESC
) = 1;
