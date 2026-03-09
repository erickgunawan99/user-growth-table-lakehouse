{{ config(
    materialized='incremental',
    incremental_strategy='append',
    format='PARQUET',
    partitioning=['event_date']
) }}

WITH td AS (
    SELECT 
        user_id, 
        CAST(event_date AS DATE) as event_date
    FROM {{ source('hive_raw', 'daily_activity') }} 
    WHERE event_date = '{{ var("run_date") }}' 
    GROUP BY user_id, event_date
),

yd AS (
    {% if is_incremental() %}
    SELECT 
        user_id,
        first_active_date,
        last_active_date,
        daily_active_status,
        active_dates,
        snapshot_date
    FROM {{ this }} 
    WHERE snapshot_date = DATE '{{ var("run_date") }}' - INTERVAL '1' DAY
    {% else %}
    SELECT 
        CAST(NULL AS VARCHAR) AS user_id,
        CAST(NULL AS DATE) AS first_active_date,
        CAST(NULL AS DATE) AS last_active_date,
        CAST(NULL AS VARCHAR) AS daily_active_status,
        CAST(ARRAY[] AS ARRAY(DATE)) AS active_dates,
        CAST(NULL AS DATE) AS snapshot_date
    WHERE 1=0
    {% endif %}
)

SELECT
    COALESCE(td.user_id, yd.user_id) AS user_id,
    COALESCE(yd.first_active_date, td.event_date) AS first_active_date,
    COALESCE(td.event_date, yd.last_active_date) AS last_active_date,
    CASE
        WHEN yd.user_id IS NULL AND td.user_id IS NOT NULL THEN 'New'
        WHEN yd.last_active_date = td.event_date - INTERVAL '1' DAY THEN 'Retained'
        WHEN yd.last_active_date < td.event_date - INTERVAL '1' DAY THEN 'Reactivated'
        WHEN td.user_id IS NULL AND yd.last_active_date = yd.snapshot_date THEN 'Churned'
        ELSE 'Stale'
    END AS daily_active_status,
    CASE
        WHEN yd.user_id IS NULL AND td.user_id IS NOT NULL THEN 'New'
        WHEN td.user_id IS NULL AND yd.last_active_date = yd.snapshot_date  - INTERVAL '6' DAY 
            THEN 'Churned'
        WHEN td.user_id IS NOT NULL AND yd.last_active_date < td.event_date - INTERVAL '7' DAY 
            THEN 'Reactivated'
        WHEN td.user_id IS NOT NULL AND yd.last_active_date >= td.event_date - INTERVAL '7' DAY 
            THEN 'Retained'
        ELSE 'Stale'
    END AS weekly_active_status,
    COALESCE(yd.active_dates, CAST(ARRAY[] AS ARRAY(DATE))) 
        || CASE 
            WHEN td.user_id IS NOT NULL THEN ARRAY[td.event_date]
            ELSE CAST(ARRAY[] AS ARRAY(DATE))
        END AS active_dates,
    COALESCE(td.event_date, yd.snapshot_date + INTERVAL '1' DAY) AS snapshot_date
FROM td 
FULL OUTER JOIN yd ON td.user_id = yd.user_id