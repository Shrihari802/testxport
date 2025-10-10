-- Databricks specific SQL with window functions and CTEs
-- This query performs customer segmentation based on purchase behavior

WITH customer_purchases AS (
    -- Calculate customer purchase metrics
    SELECT 
        CAST(c.customer_id AS STRING) as customer_id,
        CAST(c.name AS STRING) as name,
        COUNT(DISTINCT o.order_id) as total_orders,
        CAST(SUM(o.total_amount) AS DECIMAL(12,2)) as total_spent,
        CAST(MAX(o.order_date) AS DATE) as last_order_date,
        CAST(MIN(o.order_date) AS DATE) as first_order_date,
        CAST(AVG(oi.quantity) AS DECIMAL(5,2)) as avg_items_per_order
    FROM 
        raw_customers c
        LEFT JOIN raw_orders o ON c.customer_id = o.customer_id
        LEFT JOIN raw_order_items oi ON o.order_id = oi.order_id
    WHERE 
        o.order_date >= date_sub(current_date(), 365)
    GROUP BY 
        c.customer_id, c.name
),
customer_rankings AS (
    -- Add rankings and percentiles
    SELECT 
        *,
        NTILE(4) OVER (ORDER BY total_spent DESC) as spend_quartile,
        RANK() OVER (ORDER BY total_orders DESC) as order_rank,
        LAG(total_spent, 1) OVER (ORDER BY total_spent DESC) as next_higher_spent
    FROM 
        customer_purchases
),
customer_segments AS (
    -- Define customer segments
    SELECT 
        *,
        CASE 
            WHEN spend_quartile = 1 AND total_orders >= 10 THEN 'VIP'
            WHEN spend_quartile = 1 OR spend_quartile = 2 THEN 'High Value'
            WHEN total_orders >= 5 THEN 'Regular'
            ELSE 'Standard'
        END as customer_segment,
        datediff(last_order_date, first_order_date) as customer_lifetime_days
    FROM 
        customer_rankings
)
SELECT 
    cs.*,
    ROUND(
        CAST(total_spent / nullif(customer_lifetime_days, 0) * 30 AS DECIMAL(10,2)),
        2
    ) as monthly_avg_spend,
    ROW_NUMBER() OVER (PARTITION BY customer_segment ORDER BY total_spent DESC) as segment_rank
FROM 
    customer_segments cs
WHERE 
    customer_lifetime_days > 0
USING DELTA
CLUSTER BY (customer_segment, customer_id);
