-- BigQuery specific SQL with partitioning and clustering
CREATE OR REPLACE TABLE customer_analytics
PARTITION BY DATE(event_date)
CLUSTER BY customer_id
AS
SELECT 
    CAST(c.customer_id AS STRING) as customer_id,
    CAST(c.name AS STRING) as name,
    CAST(e.event_type AS STRING) as event_type,
    CAST(e.event_date AS DATE) as event_date,
    CAST(e.revenue AS NUMERIC) as revenue,
    CURRENT_TIMESTAMP() as processed_date,
    COALESCE(e.source, 'direct') as source
FROM 
    raw_customers c
    LEFT JOIN raw_events e ON c.customer_id = e.customer_id
WHERE 
    e.event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
    AND e.event_type IN ('purchase', 'view', 'cart_add');
