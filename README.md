#The workflow:

1. Raw Input (Physical Partitions): Your daily_user_date.parquet files sit in MinIO, explicitly organized into /event_date=.../ folders. This is the Hive Storage Format (schema-on-read).

2. Trino's View (Hive Table): A standard CREATE TABLE command in Trino registers these folders as a queryable table: hive.raw_zone.daily_activity. Trino is just mapping a schema over the files.

3. dbt Transformation (The Engine): dbt-trino runs your incremental SQL model. It reads the raw activity from the hive source. Crucially, it also reads itself (the Iceberg target table) to find "yesterday's" data ({{ this }}) to determine New vs. Resurrected.

4. Process Logic: The dbt model calculates all your requested features (daily_active_status, weekly_active_status, and accumulates the active_dates array) using CASE and || logic.

5. Output (Iceberg Table): dbt appends the new data to the user_activity table in the iceberg catalog. Logically, it’s a standard table.
