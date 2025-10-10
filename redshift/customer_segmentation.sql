-- Redshift specific SQL with window functions and CTEs
-- This query performs customer segmentation based on purchase behavior

WITH customer_purchases AS (
    -- Calculate customer purchase metrics
    SELECT 
        c.customer_id::varchar(50),
        c.name::varchar(100),
        COUNT(DISTINCT o.order_id)::int as total_orders,
        SUM(o.total_amount)::decimal(12,2) as total_spent,
        MAX(o.order_date)::date as last_order_date,
        MIN(o.order_date)::date as first_order_date,
        AVG(oi.quantity)::decimal(5,2) as avg_items_per_order
    FROM 
        raw_customers c
        LEFT JOIN raw_orders o ON c.customer_id = o.customer_id
        LEFT JOIN raw_order_items oi ON o.order_id = oi.order_id
    WHERE 
        o.order_date >= DATEADD(year, -1, GETDATE())
    GROUP BY 
        c.customer_id, c.name
),
customer_rankings AS (
    -- Add rankings and percentiles
    SELECT 
        *,
        NTILE(4) OVER (ORDER BY total_spent DESC)::int as spend_quartile,
        RANK() OVER (ORDER BY total_orders DESC)::int as order_rank,
        LAG(total_spent, 1) OVER (ORDER BY total_spent DESC)::decimal(12,2) as next_higher_spent
    FROM 
        customer_purchases
),
customer_segments AS (
    -- Define customer segments
    SELECT 
        *,
        CASE 
            WHEN spend_quartile = 1 AND total_orders >= 10 THEN 'VIP'::varchar(20)
            WHEN spend_quartile = 1 OR spend_quartile = 2 THEN 'High Value'::varchar(20)
            WHEN total_orders >= 5 THEN 'Regular'::varchar(20)
            ELSE 'Standard'::varchar(20)
        END as customer_segment,
        DATEDIFF(day, first_order_date, last_order_date)::int as customer_lifetime_days
    FROM 
        customer_rankings
)
SELECT 
    cs.*,
    ROUND(
        (total_spent / NULLIF(customer_lifetime_days, 0))::decimal(10,2) * 30, 
        2
    )::decimal(10,2) as monthly_avg_spend,
    ROW_NUMBER() OVER (PARTITION BY customer_segment ORDER BY total_spent DESC)::int as segment_rank
FROM 
    customer_segments cs
WHERE 
    customer_lifetime_days > 0
DISTKEY(customer_id)
SORTKEY(customer_segment, total_spent);
