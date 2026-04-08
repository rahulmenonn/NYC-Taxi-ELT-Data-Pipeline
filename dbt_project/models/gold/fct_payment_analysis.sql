WITH silver AS (
    SELECT * FROM {{ ref('stg_trips') }}
),

payment AS (
    SELECT
        payment_type_name,
        COUNT(*)                                           AS total_trips,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct_of_trips,
        ROUND(SUM(total_amount)::NUMERIC, 2)               AS total_revenue,
        ROUND(AVG(total_amount)::NUMERIC, 2)               AS avg_fare,
        ROUND(AVG(tip_amount)::NUMERIC, 2)                 AS avg_tip,
        ROUND(
            (AVG(tip_amount) * 100.0 / NULLIF(AVG(fare_amount), 0))::NUMERIC
        , 2)                                               AS avg_tip_pct,
        ROUND(AVG(trip_distance)::NUMERIC, 2)              AS avg_distance,
        ROUND(AVG(trip_duration_minutes)::NUMERIC, 2)      AS avg_duration_mins
    FROM silver
    GROUP BY payment_type_name
)

SELECT * FROM payment
ORDER BY total_trips DESC