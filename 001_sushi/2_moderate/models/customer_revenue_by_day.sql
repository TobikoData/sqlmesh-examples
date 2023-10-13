/*
  Incremental table of revenue from customers by day.
*/
MODEL (
  name sushimoderate.customer_revenue_by_day,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (ds, 'YYYY-MM-dd'),
  ),
  start "2023-01-01",
  cron "@daily",
  grains [customer_id, ds],
);

WITH order_total AS (
  SELECT
    oi.order_id AS order_id,
    SUM(oi.quantity * i.price) AS total,
    oi.ds AS ds
  FROM sushimoderate.order_items AS oi
  LEFT JOIN sushimoderate.items AS i
    ON oi.item_id = i.id AND oi.ds = i.ds
  WHERE
    oi.ds BETWEEN @start_ds AND @end_ds
  GROUP BY
    oi.order_id,
    oi.ds
)
SELECT
  o.customer_id::INT AS customer_id, /* Customer ID */
  SUM(ot.total)::DOUBLE AS revenue, /* Revenue from orders made by this customer */
  o.ds::TEXT AS ds /* Date */
FROM sushimoderate.orders AS o
LEFT JOIN order_total AS ot
  ON o.id = ot.order_id AND o.ds = ot.ds
WHERE
  o.ds BETWEEN @start_ds AND @end_ds
GROUP BY
  o.customer_id,
  o.ds;
