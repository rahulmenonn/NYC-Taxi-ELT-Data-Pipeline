-- Silver Layer: stg_trips.sql
-- Cleans raw bronze data by:
-- 1. Removing trips with invalid dates
-- 2. Removing trips with nonsensical values
-- 3. Adding derived columns useful for analysis
-- 4. Casting types correctly

WITH source AS (
    SELECT * FROM bronze.raw_trips
),

cleaned AS (
    SELECT
        -- Core identifiers
        vendor_id,
        
        -- Timestamps
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        
        -- Trip duration in minutes (derived)
        ROUND(
            EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60.0
        , 2) AS trip_duration_minutes,
        
        -- Date parts for analysis
        DATE(tpep_pickup_datetime)                    AS trip_date,
        EXTRACT(HOUR FROM tpep_pickup_datetime)::INT  AS pickup_hour,
        EXTRACT(DOW FROM tpep_pickup_datetime)::INT   AS day_of_week,
        TO_CHAR(tpep_pickup_datetime, 'Day')          AS day_name,
        EXTRACT(MONTH FROM tpep_pickup_datetime)::INT AS trip_month,

        -- Trip details
        COALESCE(passenger_count, 1)::INT             AS passenger_count,
        trip_distance,
        pu_location_id,
        do_location_id,

        -- Payment
        payment_type::INT                             AS payment_type,
        CASE payment_type::INT
            WHEN 1 THEN 'Credit Card'
            WHEN 2 THEN 'Cash'
            WHEN 3 THEN 'No Charge'
            WHEN 4 THEN 'Dispute'
            WHEN 5 THEN 'Unknown'
            ELSE 'Other'
        END                                           AS payment_type_name,

        -- Fares
        fare_amount,
        tip_amount,
        tolls_amount,
        total_amount,
        congestion_surcharge,
        COALESCE(airport_fee, 0)                      AS airport_fee,

        -- Metadata
        source_file,
        ingested_at

    FROM source
    WHERE
        -- Remove trips outside Jan 2024 (bad timestamps)
        tpep_pickup_datetime >= '2024-01-01'
        AND tpep_pickup_datetime < '2024-02-01'
        -- Remove trips with zero or negative fares
        AND fare_amount > 0
        -- Remove trips with zero distance
        AND trip_distance > 0
        -- Remove trips with negative duration
        AND tpep_dropoff_datetime > tpep_pickup_datetime
        -- Remove unrealistic trip durations (over 5 hours)
        AND EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60.0 <= 300
        -- Remove unrealistic distances (over 100 miles)
        AND trip_distance <= 100
)

SELECT * FROM cleaned