-- Seed data containing water names.
MODEL (
  name sushimoderate.waiter_names,
  kind SEED (
    path '../seeds/waiter_names.csv'
  ),
  columns (
    id INT,
    name TEXT
  ),
  grain id
)
