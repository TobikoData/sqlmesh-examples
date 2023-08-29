MODEL (
  name sushi.waiter_names,
  kind SEED (
    path '../seeds/waiter_names.csv'
  ),
  columns (
    id INT,
    name TEXT
  ),
  start '2023-01-01',
  grain id
)
