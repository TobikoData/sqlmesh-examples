/*
    Incremental table loading order_items table from raw.
*/
MODEL(
    name sushimoderate.order_items,
    kind INCREMENTAL_BY_TIME_RANGE (
        time_column ds
    ),
    cron "@daily",
    start "2023-01-01",
    audits (
        NOT_NULL(columns=[id, order_id, item_id, quantity, ds]),
        UNIQUE_COMBINATION_OF_COLUMNS(columns=[id, ds]),
        FORALL(criteria=[
            quantity > 0,
        ])
    ),
    grains [
        "id as order_item_id",
        ds
    ],
    references [order_id, item_id]
);

SELECT
    id::INT, /* Order items ID */
    order_id::INT, /* Order ID */
    item_id::INT, /* Item ID */
    quantity::INT, /* Quantity of item ordered for this order */
    ds::TEXT /* Date */
FROM
    raw.order_items
WHERE
    ds BETWEEN @start_ds and @end_ds;
