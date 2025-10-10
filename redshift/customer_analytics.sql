-- Redshift specific SQL with DIST and SORT keys
CREATE TABLE customer_analytics
DISTKEY(customer_id)
SORTKEY(event_date, event_type)
AS
SELECT 
    c.customer_id::varchar(50),
    c.name::varchar(100),
    e.event_type::varchar(20),
    e.event_date::date,
    e.revenue::decimal(10,2),
    GETDATE() as processed_date,
    NVL(e.source, 'direct')::varchar(50) as source
FROM 
    raw_customers c
    LEFT JOIN raw_events e ON c.customer_id = e.customer_id
WHERE 
    e.event_date >= DATEADD(month, -3, GETDATE())
    AND e.event_type IN ('purchase', 'view', 'cart_add');
