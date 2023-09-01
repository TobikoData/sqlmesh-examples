-- Incremental table of revenue from customers by day.
MODEL (
  name sushimoderate.customer_revenue_by_day,
  kind incremental_by_time_range (
    time_column (ds, 'YYYY-MM-dd'),
  ),
  start '2023-01-01',
  cron '@daily',
  grain (customer_id, ds),
);

WITH order_total AS (
  SELECT
    oi.order_id AS order_id,
    SUM(oi.quantity * i.price) AS total,
    oi.ds AS ds
  FROM raw.order_items AS oi
  LEFT JOIN raw.items AS i
    ON oi.item_id = i.id AND oi.ds = i.ds
  WHERE
    oi.ds BETWEEN @start_ds AND @end_ds
  GROUP BY
    oi.order_id,
    oi.ds
)
SELECT
  o.customer_id::INT AS customer_id, /* Customer id */
  SUM(ot.total)::DOUBLE AS revenue, /* Revenue from orders made by this customer */
  o.ds::TEXT AS ds /* Date */
FROM raw.orders AS o
LEFT JOIN order_total AS ot
  ON o.id = ot.order_id AND o.ds = ot.ds
WHERE
  o.ds BETWEEN @start_ds AND @end_ds
GROUP BY
  o.customer_id,
  o.ds
