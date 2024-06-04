{{ config(
    materialized='incremental',
    unique_key='event_date',
    partition_by={"field": "event_date", "data_type": "date"}
) }}

WITH new_revenue AS (
  SELECT 
    DATE(event_time) AS event_date,
    page_url,
    SUM(order_value) AS total_revenue
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
  total_revenue,
  page_url
FROM 
  new_revenue
