{{ config(
    materialized='incremental',
    unique_key='event_date',
    partition_by={"field": "event_date", "data_type": "date"},
    clustered_by=['session_id']
) }}

WITH new_sessions AS (
  SELECT
    DATE(event_time) as event_date,
    page_url,
    session_id
  FROM
    `trial-ww-424107.atomic.shopify_events_table`
  {% if is_incremental() %}
  WHERE event_time > (SELECT MAX(event_time) FROM {{ this }})
  {% endif %}
  GROUP BY event_date, session_id, page_url
)

SELECT
  event_date,
  page_url,
  COUNT(DISTINCT session_id) as number_of_sessions
FROM
  new_sessions
GROUP BY
  event_date, page_url
