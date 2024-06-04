{{ config(
    materialized='incremental',
    unique_key='event_date',
    partition_by={"field": "event_date", "data_type": "date"}
) }}

WITH new_checkout_events AS (
  SELECT 
    DATE(event_time) AS event_date,
    COUNT(*) AS checkout_events, 
    page_url
  FROM 
    `trial-ww-424107.atomic.shopify_events_table`
  WHERE 
    event_name = 'checkout_completed'
  {% if is_incremental() %}
  AND event_time > (SELECT MAX(event_time) FROM {{ this }})
  {% endif %}
  GROUP BY 
    event_date, page_url
)

SELECT 
  event_date, 
  checkout_events,
  page_url
FROM 
  new_checkout_events
