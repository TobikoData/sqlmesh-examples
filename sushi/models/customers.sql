MODEL (
  name sushi.customers,
  kind FULL,
  owner jen,
  cron '@daily',
  start '2023-01-01',
  grain customer_id,
);

WITH distinct_marketing AS (
  SELECT DISTINCT
    customer_id,
    status
  FROM raw.marketing
)
SELECT DISTINCT
  o.customer_id::INT AS customer_id,
  COALESCE(m.status, 'UNKNOWN')::TEXT AS status,
  d.zip::TEXT as zip
FROM raw.orders AS o
LEFT JOIN distinct_marketing AS m
    ON o.customer_id = m.customer_id
LEFT JOIN raw.demographics AS d
    ON o.customer_id = d.customer_id
