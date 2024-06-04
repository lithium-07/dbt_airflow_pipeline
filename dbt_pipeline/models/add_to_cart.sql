{{ config(
    materialized='incremental',
    unique_key='event_date',
    partition_by={"field": "event_date", "data_type": "date"}
) }}

WITH new_add_to_cart AS (
  SELECT 
    DATE(event_time) AS event_date,
    page_url,
    COUNT(*) AS add_to_cart_events
  FROM 
    `trial-ww-424107.atomic.shopify_events_table`
  WHERE 
    event_name = 'product_added_to_cart'
  {% if is_incremental() %}
  AND event_time > (SELECT MAX(event_time) FROM {{ this }})
  {% endif %}
  GROUP BY 
    event_date, page_url
)

SELECT 
  event_date, 
  add_to_cart_events,
  page_url
FROM 
  new_add_to_cart
