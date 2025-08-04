{{ config(materialized='table') }}

-- Daily aggregated market data for NQ futures
-- Provides daily OHLCV with technical indicators for algorithm analysis

SELECT 
    trade_date,
    raw_symbol,
    instrument_id,
    
    -- Daily OHLCV
    MIN(event_timestamp) AS market_open_time,
    MAX(event_timestamp) AS market_close_time,
    
    -- Price action using window functions to get true daily OHLCV
    FIRST_VALUE(open_price) OVER (
        PARTITION BY trade_date, raw_symbol 
        ORDER BY event_timestamp 
        ROWS UNBOUNDED PRECEDING
    ) AS daily_open,
    
    MAX(high_price) AS daily_high,
    MIN(low_price) AS daily_low,
    
    LAST_VALUE(close_price) OVER (
        PARTITION BY trade_date, raw_symbol 
        ORDER BY event_timestamp 
        ROWS UNBOUNDED FOLLOWING
    ) AS daily_close,
    
    -- Volume metrics
    SUM(volume) AS daily_volume,
    AVG(volume) AS avg_minute_volume,
    MAX(volume) AS max_minute_volume,
    
    -- Intraday metrics
    COUNT(*) AS minutes_traded,
    MAX(high_price) - MIN(low_price) AS daily_range,
    
    -- Technical indicators (end of day values)
    LAST_VALUE(sma_10) OVER (
        PARTITION BY trade_date, raw_symbol 
        ORDER BY event_timestamp 
        ROWS UNBOUNDED FOLLOWING
    ) AS eod_sma_10,
    
    LAST_VALUE(sma_20) OVER (
        PARTITION BY trade_date, raw_symbol 
        ORDER BY event_timestamp 
        ROWS UNBOUNDED FOLLOWING
    ) AS eod_sma_20,
    
    LAST_VALUE(sma_50) OVER (
        PARTITION BY trade_date, raw_symbol 
        ORDER BY event_timestamp 
        ROWS UNBOUNDED FOLLOWING
    ) AS eod_sma_50,
    
    -- Volatility metrics
    STDDEV(close_price) AS price_volatility,
    AVG(price_range) AS avg_minute_range,
    
    -- Trend metrics
    COUNT(CASE WHEN price_change_1m > 0 THEN 1 END) AS up_minutes,
    COUNT(CASE WHEN price_change_1m < 0 THEN 1 END) AS down_minutes,
    
    -- Market session classification
    CASE 
        WHEN EXTRACT(hour FROM MIN(event_timestamp)) >= 9 
         AND EXTRACT(hour FROM MAX(event_timestamp)) <= 16 THEN 'REGULAR_SESSION'
        ELSE 'EXTENDED_SESSION'
    END AS session_type

FROM {{ ref('int_technical_indicators') }}

GROUP BY trade_date, raw_symbol, instrument_id

-- Only include complete trading days
HAVING COUNT(*) > 100  -- At least 100 minutes of data
