-- bi.raw_events(
--   event_id VARCHAR,
--   user_id  VARCHAR,
--   event_ts TIMESTAMP,
--   event_type VARCHAR,
--   event_payload SUPER  -- contains JSON
-- )

WITH flattened AS (
    SELECT
        r.event_id,
        r.user_id,
        r.event_ts,
        r.event_type,
        -- Extract some JSON fields out of SUPER payload
        json_extract_path_text(r.event_payload, 'device', 'os')              AS device_os,
        json_extract_path_text(r.event_payload, 'device', 'app_version')     AS app_version,
        json_extract_path_text(r.event_payload, 'page', 'url')               AS page_url,
        json_extract_path_text(r.event_payload, 'experiment', 'bucket')      AS experiment_bucket,
        TRY_CAST(json_extract_path_text(r.event_payload, 'commerce', 'value') AS DECIMAL(18,2)) AS commerce_value
    FROM bi.raw_events r
    WHERE r.event_ts >= dateadd(day, -30, current_date)
),

sessionized AS (
    SELECT
        f.*,
        -- Sort events by user/time
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY event_ts
        ) AS rn,
        -- Gap-based sessionization: new session if gap > 30 minutes
        CASE
            WHEN LAG(event_ts) OVER (PARTITION BY user_id ORDER BY event_ts) IS NULL
                 OR DATEDIFF(minute,
                             LAG(event_ts) OVER (PARTITION BY user_id ORDER BY event_ts),
                             event_ts) > 30
            THEN 1
            ELSE 0
        END AS is_new_session
    FROM flattened f
),

session_ids AS (
    SELECT
        s.*,
        -- Session id = cumulative sum of is_new_session per user
        SUM(is_new_session) OVER (
            PARTITION BY user_id
            ORDER BY event_ts
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS session_seq
    FROM sessionized s
),

session_agg AS (
    SELECT
        user_id,
        session_seq,
        MIN(event_ts) AS session_start_ts,
        MAX(event_ts) AS session_end_ts,
        COUNT(*) AS events_in_session,
        COUNT(DISTINCT event_type) AS distinct_event_types,
        MAX(device_os) AS device_os,
        MAX(app_version) AS app_version,
        MAX(experiment_bucket) AS experiment_bucket,
        SUM(CASE WHEN event_type = 'ADD_TO_CART' THEN 1 ELSE 0 END) AS add_to_cart_events,
        SUM(CASE WHEN event_type = 'CHECKOUT' THEN 1 ELSE 0 END) AS checkout_events,
        SUM(COALESCE(commerce_value, 0)) AS session_revenue
    FROM session_ids
    GROUP BY
        user_id,
        session_seq
),

funnel_flags AS (
    SELECT
        sa.*,
        CASE WHEN add_to_cart_events > 0 THEN 1 ELSE 0 END AS did_add_to_cart,
        CASE WHEN checkout_events   > 0 THEN 1 ELSE 0 END AS did_checkout,
        CASE WHEN session_revenue   > 0 THEN 1 ELSE 0 END AS did_purchase
    FROM session_agg sa
)

SELECT
    ff.*,
    -- Simple funnel stage classification
    CASE
        WHEN did_purchase = 1                           THEN 'PURCHASED'
        WHEN did_checkout = 1 AND did_purchase = 0      THEN 'CHECKOUT_DROP'
        WHEN did_add_to_cart = 1 AND did_checkout = 0   THEN 'CART_DROP'
        ELSE 'BROWSE_ONLY'
    END AS funnel_stage
FROM funnel_flags ff
ORDER BY
    user_id,
    session_start_ts;
 