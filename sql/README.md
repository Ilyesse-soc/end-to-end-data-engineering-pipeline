These SQL files are executed automatically by the Postgres container on first startup (mounted to `/docker-entrypoint-initdb.d`).

- `00_databases.sql`: creates `airflow` and `warehouse` databases + roles
- `raw_schema.sql`: `raw` schema + ingestion tables
- `staging_schema.sql`: `staging` schema + curated table + quality results
- `analytics_schema.sql`: `analytics` schema (dbt-managed)
- `zz_grants.sql`: grants (runs last)
