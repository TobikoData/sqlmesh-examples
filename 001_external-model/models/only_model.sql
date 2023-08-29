MODEL (
  name external_model.full_model,
  kind FULL,
  cron '@daily',
);

select * from