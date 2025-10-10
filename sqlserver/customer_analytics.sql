-- SQL Server specific SQL with indexes and temporal tables
CREATE TABLE customer_analytics (
    customer_id varchar(50),
    name varchar(100),
    event_type varchar(20),
    event_date date,
    revenue decimal(10,2),
    processed_date datetime2,
    source varchar(50),
    valid_from datetime2 GENERATED ALWAYS AS ROW START,
    valid_to datetime2 GENERATED ALWAYS AS ROW END,
    PERIOD FOR SYSTEM_TIME (valid_from, valid_to)
) WITH (SYSTEM_VERSIONING = ON);

CREATE INDEX idx_customer_analytics_customer_id ON customer_analytics(customer_id);
CREATE INDEX idx_customer_analytics_event_date ON customer_analytics(event_date);

INSERT INTO customer_analytics (customer_id, name, event_type, event_date, revenue, processed_date, source)
SELECT 
    CAST(c.customer_id AS varchar(50)) as customer_id,
    CAST(c.name AS varchar(100)) as name,
    CAST(e.event_type AS varchar(20)) as event_type,
    CAST(e.event_date AS date) as event_date,
    CAST(e.revenue AS decimal(10,2)) as revenue,
    GETDATE() as processed_date,
    ISNULL(e.source, 'direct') as source
FROM 
    raw_customers c
    LEFT JOIN raw_events e ON c.customer_id = e.customer_id
WHERE 
    e.event_date >= DATEADD(month, -3, GETDATE())
    AND e.event_type IN ('purchase', 'view', 'cart_add');
