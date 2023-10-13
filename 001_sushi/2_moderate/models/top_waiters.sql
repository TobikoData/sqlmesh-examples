/*
  View of 10 waiters with highest revenue on most recent day of data.
*/
MODEL (
  name sushimoderate.top_waiters,
  kind VIEW,
  cron '@daily',
  grain waiter_id,
  audits (
    unique_values(columns=[waiter_id])
  ),
);

SELECT
  waiter_id::INT AS waiter_id, /* Waiter ID */
  name::TEXT AS waiter_name, /* Waiter name */
  revenue::DOUBLE AS revenue /* Waiter revenue on most recent day of data */
FROM sushimoderate.waiter_revenue_by_day as r
LEFT JOIN sushimoderate.waiter_names AS n
  ON r.waiter_id = n.id
WHERE
  ds = (
    SELECT
      MAX(ds)
    FROM sushimoderate.waiter_revenue_by_day
  )
ORDER BY
  revenue DESC
LIMIT 10;
