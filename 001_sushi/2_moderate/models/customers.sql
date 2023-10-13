/*
  Table of customer data for customers who have ever placed an order.
*/
MODEL (
  name sushimoderate.customers,
  kind FULL,
  cron '@daily',
  grain customer_id,
  audits (
        not_null(columns=[customer_id]),
        unique_values(columns=[customer_id])
    ),
);

SELECT DISTINCT
  o.customer_id::INT AS customer_id, /* Customer ID */
  COALESCE(m.status, 'UNKNOWN')::TEXT AS status, /* Customer marketing status */
  d.zip::TEXT as zip /* Customer ZIP code */
FROM sushimoderate.orders AS o
LEFT JOIN raw.marketing AS m
    ON o.customer_id = m.customer_id
LEFT JOIN raw.demographics AS d
    ON o.customer_id = d.customer_id;
