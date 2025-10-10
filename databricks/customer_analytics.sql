-- Databricks specific SQL with Delta Lake features
CREATE OR REPLACE TABLE customer_analytics
USING DELTA
CLUSTER BY (customer_id)
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
)
AS
SELECT 
    CAST(c.customer_id AS STRING) as customer_id,
    CAST(c.name AS STRING) as name,
    CAST(e.event_type AS STRING) as event_type,
    CAST(e.event_date AS DATE) as event_date,
    TRY_CAST(e.revenue AS DECIMAL(10,2)) as revenue,
    current_timestamp() as processed_date,
    COALESCE(e.source, 'direct') as source
FROM 
    raw_customers c
    LEFT JOIN raw_events e ON c.customer_id = e.customer_id
WHERE 
    e.event_date >= date_sub(current_date(), 90)
    AND e.event_type IN ('purchase', 'view', 'cart_add');
