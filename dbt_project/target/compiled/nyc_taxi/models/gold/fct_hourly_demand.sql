WITH silver as(
    select * from "nyc_taxi"."silver_silver"."stg_trips"
),
hourly as(
    select
        trip_date,
        pickup_hour,
        day_name,
        day_of_week,
        CASE
            WHEN day_of_week in (0,6) THEN 'Weekend'
            ELSE 'Weekday'
        END as day_type,
        CASE    
            WHEN pickup_hour between 7 and 9 then 'Morning Rush'
            WHEN pickup_hour between 10 and 15 then 'Midday'
            WHEN pickup_hour between 16 and 19 then 'Evening Rush'
            WHEN pickup_hour between 20 and 23 then 'Night'
            ELSE 'Late Night'
        END AS time_period,
        COUNT(*) total_trips,
        round(sum(total_amount)::NUMERIC,2) AS total_revenue,
        round(avg(total_amount)::NUMERIC,2) AS avg_fare,
        round(avg(trip_distance)::NUMERIC,2) AS avg_distance,
        round(avg(trip_duration_minutes)::NUMERIC,2) AS avg_duration_mins,
        sum(passenger_count) as total_passengers
    FROM silver
    GROUP BY trip_date,pickup_hour,day_name,day_of_week

)

select * FROM hourly
order by trip_date,pickup_hour