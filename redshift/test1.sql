WITH orders_enriched AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_ts,
        o.order_status,
        o.order_amount,
        o.currency_code,
        o.country_code,
        date_trunc('day', o.order_ts) AS order_date,
        -- Normalize amounts to USD with a FX lookup
        o.order_amount * COALESCE(fx.rate_to_usd, 1.0) AS amount_usd
    FROM bi.orders o
    LEFT JOIN bi.fx_rates fx
        ON fx.currency_code = o.currency_code
       AND fx.rate_date = date_trunc('day', o.order_ts)
),

customer_day_agg AS (
    SELECT
        customer_id,
        order_date,
        COUNT(*) AS orders_count,
        SUM(amount_usd) AS revenue_usd,
        SUM(CASE WHEN order_status = 'CANCELLED' THEN amount_usd ELSE 0 END) AS cancelled_usd,
        COUNT(DISTINCT CASE WHEN order_status = 'CANCELLED' THEN order_id END) AS cancelled_orders,
        MAX(order_ts) AS last_order_ts
    FROM orders_enriched
    GROUP BY
        customer_id,
        order_date
),

customer_lifetime AS (
    SELECT
        cda.*,
        -- Running lifetime revenue
        SUM(revenue_usd) OVER (
            PARTITION BY customer_id
            ORDER BY order_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS lifetime_revenue_usd,
        -- 7-day rolling revenue
        SUM(revenue_usd) OVER (
            PARTITION BY customer_id
            ORDER BY order_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rev_7d_rolling_usd,
        -- Dense rank by daily revenue per customer
        DENSE_RANK() OVER (
            PARTITION BY customer_id
            ORDER BY revenue_usd DESC
        ) AS daily_revenue_rank
    FROM customer_day_agg cda
),

customer_flags AS (
    SELECT
        cl.*,
        CASE
            WHEN lifetime_revenue_usd >= 10000 THEN 'PLATINUM'
            WHEN lifetime_revenue_usd >= 5000  THEN 'GOLD'
            WHEN lifetime_revenue_usd >= 1000  THEN 'SILVER'
            ELSE 'BRONZE'
        END AS loyalty_segment,
        CASE
            WHEN rev_7d_rolling_usd = 0 THEN 1
            ELSE 0
        END AS risk_of_churn_flag
    FROM customer_lifetime cl
)

SELECT
    cf.*
FROM customer_flags cf
-- Keep only the top 3 revenue days per customer for your snapshot
QUALIFY daily_revenue_rank <= 3
ORDER BY
    customer_id,
    order_date;
 