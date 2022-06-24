-- CREATE OR REPLACE TABLE `sandbox-athevenot.aggregated.open_store`
SELECT
  city,
  country,
  coordinate,	
  creation_date
FROM `sandbox-athevenot.cleaned.store`
WHERE NOT is_closed;
