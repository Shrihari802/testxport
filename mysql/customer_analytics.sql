-- MySQL specific SQL with storage engine and indexes
CREATE TABLE customer_analytics (
    customer_id varchar(50),
    name varchar(100),
    event_type varchar(20),
    event_date date,
    revenue decimal(10,2),
    processed_date timestamp,
    source varchar(50),
    INDEX idx_customer_id (customer_id),
    INDEX idx_event_date (event_date)
) ENGINE = InnoDB;

INSERT INTO customer_analytics
SELECT 
    CAST(c.customer_id AS CHAR(50)) as customer_id,
    CAST(c.name AS CHAR(100)) as name,
    CAST(e.event_type AS CHAR(20)) as event_type,
    CAST(e.event_date AS DATE) as event_date,
    CAST(e.revenue AS DECIMAL(10,2)) as revenue,
    NOW() as processed_date,
    IFNULL(e.source, 'direct') as source
FROM 
    raw_customers c
    LEFT JOIN raw_events e ON c.customer_id = e.customer_id
WHERE 
    e.event_date >= DATE_SUB(CURDATE(), INTERVAL 3 MONTH)
    AND e.event_type IN ('purchase', 'view', 'cart_add');
