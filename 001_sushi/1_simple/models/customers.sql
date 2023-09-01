-- View of customer data.
MODEL (
  name sushisimple.customers,
  kind VIEW,
  cron '@daily',
  grain customer_id,
);

SELECT DISTINCT
  o.customer_id::INT AS customer_id,
  COALESCE(m.status, 'UNKNOWN')::TEXT AS status,
  d.zip::TEXT as zip
FROM raw.orders AS o
LEFT JOIN raw.marketing AS m
    ON o.customer_id = m.customer_id
LEFT JOIN raw.demographics AS d
    ON o.customer_id = d.customer_id
