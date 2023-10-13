/*
    Incremental table loading orders table from raw.
*/
MODEL(
    name sushimoderate.orders,
    kind INCREMENTAL_BY_TIME_RANGE (
        time_column ds
        ),
    cron "@daily",
    start "2023-01-01",
    audits (
        not_null(columns=[id, customer_id, waiter_id, ds]),
        unique_combination_of_columns(columns=[id, ds])
    ),
    grains [
        "id AS order_id",
        ds
    ],
    references [customer_id, waiter_id]
);

SELECT
    id::int, /* Order ID */
    customer_id::int, /* ID of customer placing order */
    waiter_id::int, /* ID of waiter taking order */
    start_ts::int, /* Order start timestamp */
    end_ts::int, /* Order end timestamp */
    ds::text /* Order date */
FROM
    raw.orders
WHERE
    ds BETWEEN @start_ds AND @end_ds;