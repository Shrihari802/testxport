-- Snowflake specific SQL with warehouse and clustering options
CREATE OR REPLACE TABLE customer_analytics
CLUSTER BY (customer_id)
AS
SELECT 
    CAST(c.customer_id AS VARCHAR) as customer_id,
    CAST(c.name AS VARCHAR) as name,
    CAST(e.event_type AS VARCHAR) as event_type,
    CAST(e.event_date AS DATE) as event_date,
    CAST(e.revenue AS DECIMAL(10,2)) as revenue,
    CURRENT_TIMESTAMP() as processed_date,
    COALESCE(e.source, 'direct') as source
FROM 
    raw_customers c
    LEFT JOIN raw_events e ON c.customer_id = e.customer_id
WHERE 
    e.event_date >= DATEADD(month, -3, CURRENT_DATE())
    AND e.event_type IN ('purchase', 'view', 'cart_add');
