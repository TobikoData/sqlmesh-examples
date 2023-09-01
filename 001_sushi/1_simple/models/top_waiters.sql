-- View of 10 waiters with highest revenue on most recent day of data.
MODEL (
  name sushisimple.top_waiters,
  kind VIEW,
  cron '@daily',
  grain waiter_id
);

SELECT
  waiter_id::INT AS waiter_id,
  name::TEXT AS waiter_name,
  revenue::DOUBLE AS revenue
FROM sushisimple.waiter_revenue_by_day as r
LEFT JOIN sushisimple.waiter_names AS n
  ON r.waiter_id = n.id
WHERE
  ds = (
    SELECT
      MAX(ds)
    FROM sushisimple.waiter_revenue_by_day
  )
ORDER BY
  revenue DESC
LIMIT 10
