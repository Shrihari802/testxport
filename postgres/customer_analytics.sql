-- PostgreSQL specific SQL with indexes and partitioning
CREATE TABLE customer_analytics (
    customer_id varchar(50),
    name varchar(100),
    event_type varchar(20),
    event_date date,
    revenue decimal(10,2),
    processed_date timestamp,
    source varchar(50)
) PARTITION BY RANGE (event_date);

CREATE INDEX idx_customer_analytics_customer_id ON customer_analytics(customer_id);
CREATE INDEX idx_customer_analytics_event_date ON customer_analytics(event_date);

INSERT INTO customer_analytics
SELECT 
    c.customer_id::varchar(50),
    c.name::varchar(100),
    e.event_type::varchar(20),
    e.event_date::date,
    e.revenue::decimal(10,2),
    NOW() as processed_date,
    COALESCE(e.source, 'direct')::varchar(50) as source
FROM 
    raw_customers c
    LEFT JOIN raw_events e ON c.customer_id = e.customer_id
WHERE 
    e.event_date >= NOW() - INTERVAL '3 months'
    AND e.event_type IN ('purchase', 'view', 'cart_add');
