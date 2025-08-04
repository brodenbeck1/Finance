{{ config(materialized='view') }}

-- Staging model for NQ1! continuous contract
-- Creates a 1-minute data grain and switches contracts based on daily volume

WITH daily_volume AS (
    SELECT 
        DATE(REPLACE(ts_event, 'Z', '')) AS trade_date,
        symbol AS raw_symbol,
        SUM(volume) AS daily_volume
    FROM {{ source('raw_data', 'ohlcv_1m') }}
    GROUP BY trade_date, raw_symbol
),

volume_threshold AS (
    SELECT 
        trade_date,
        max(daily_volume) AS max_daily_volume
    FROM daily_volume
    GROUP BY trade_date
),

volume_filtered_data AS (
    SELECT
    dv.* 
    FROM daily_volume dv
    JOIN volume_threshold v
    ON dv.trade_date = v.trade_date
    AND dv.daily_volume = v.max_daily_volume
    -- Ensure we only keep the highest volume contract for each day 
),


filtered_data AS (
    SELECT 
        ts_event,
        rtype,
        publisher_id,
        instrument_id,
        open,
        high,
        low,
        close,
        volume,
        symbol,
        DATE(REPLACE(ts_event, 'Z', '')) AS trade_date
    FROM {{ source('raw_data', 'ohlcv_1m') }}
),

continuous_contract AS (
    SELECT 
        f.ts_event,
        f.rtype,
        f.publisher_id,
        f.instrument_id,
        CAST(f.open AS decimal(10,2)) AS open_price,
        CAST(f.high AS decimal(10,2)) AS high_price,
        CAST(f.low AS decimal(10,2)) AS low_price,
        CAST(f.close AS decimal(10,2)) AS close_price,
        CAST(f.volume AS bigint) AS volume,
        f.symbol AS raw_symbol,
        f.trade_date,
        EXTRACT(hour FROM CAST(REPLACE(f.ts_event, 'Z', '') AS timestamp)) AS hour_of_day
    FROM filtered_data f
    JOIN volume_filtered_data v
    ON f.trade_date = v.trade_date 
    AND f.symbol = v.raw_symbol
)

SELECT 
    ts_event AS event_timestamp,
    rtype AS record_type,
    publisher_id,
    instrument_id,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    raw_symbol,
    trade_date,
    hour_of_day
FROM continuous_contract
