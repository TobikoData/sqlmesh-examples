/*
    Incremental table loading items table from raw.
*/
MODEL(
    name sushimoderate.items,
    kind INCREMENTAL_BY_TIME_RANGE (
        time_column ds
    ),
    start "2023-01-01",
    audits (
        not_null(columns=[id, name, price, ds]),
        unique_combination_of_columns(columns=[id, ds]),
        forall(criteria=[
            LENGTH(name) > 0,
            price > 0,
        ])
    ),
    grains [
        "id as item_id", ds
    ]
);

SELECT
    id::INT, /* Item ID */
    name::TEXT, /* Item name */
    price::DOUBLE, /* Item price on this date */
    ds::TEXT /* Date */
FROM
    raw.items
WHERE
    ds BETWEEN @start_ds AND @end_ds;