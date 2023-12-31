-- View of revenue generated by each waiter by day.
MODEL (
  name sushisimple.waiter_revenue_by_day,
  kind VIEW,
  cron '@daily',
  grain (waiter_id, ds)
);

SELECT
  o.waiter_id::INT AS waiter_id, -- Waiter id
  SUM(oi.quantity * i.price)::DOUBLE AS revenue, -- Revenue from orders taken by this waiter
  o.ds::TEXT AS ds -- Order date
FROM raw.orders AS o
LEFT JOIN raw.order_items AS oi
  ON o.id = oi.order_id AND o.ds = oi.ds
LEFT JOIN raw.items AS i
  ON oi.item_id = i.id AND oi.ds = i.ds
GROUP BY
  o.waiter_id,
  o.ds
