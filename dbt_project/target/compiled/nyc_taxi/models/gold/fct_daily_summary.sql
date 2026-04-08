WITH silver AS (
    SELECT * FROM "nyc_taxi"."silver_silver"."stg_trips"
),

daily AS (
    SELECT
        trip_date,
        day_name,
        CASE
            WHEN day_of_week IN (0, 6) THEN 'Weekend'
            ELSE 'Weekday'
        END                                             AS day_type,
        COUNT(*)                                        AS total_trips,
        ROUND(SUM(total_amount)::NUMERIC, 2)            AS total_revenue,
        ROUND(AVG(total_amount)::NUMERIC, 2)            AS avg_fare,
        ROUND(AVG(trip_distance)::NUMERIC, 2)           AS avg_distance,
        ROUND(AVG(trip_duration_minutes)::NUMERIC, 2)   AS avg_duration_mins,
        SUM(passenger_count)                            AS total_passengers,
        ROUND(AVG(tip_amount)::NUMERIC, 2)              AS avg_tip,
        COUNT(CASE WHEN payment_type_name = 'Credit Card' THEN 1 END) AS credit_card_trips,
        COUNT(CASE WHEN payment_type_name = 'Cash' THEN 1 END)        AS cash_trips,
        ROUND(SUM(congestion_surcharge)::NUMERIC, 2)    AS total_congestion_surcharge,
        ROUND(SUM(airport_fee)::NUMERIC, 2)             AS total_airport_fees
    FROM silver
    GROUP BY trip_date, day_name, day_of_week
)

SELECT * FROM daily
ORDER BY trip_date