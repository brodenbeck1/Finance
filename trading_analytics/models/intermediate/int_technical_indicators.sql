{{ config(materialized='view') }}

-- Intermediate model to calculate technical indicators
-- Adds moving averages, volatility measures, and price momentum

WITH price_data AS (
    SELECT *
    FROM {{ ref('stg_ohlcv_1m') }}
),

technical_indicators AS (
    SELECT 
        *,
        
        -- Simple Moving Averages
        AVG(close_price) OVER (
            PARTITION BY raw_symbol 
            ORDER BY event_timestamp 
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS sma_10,
        
        AVG(close_price) OVER (
            PARTITION BY raw_symbol 
            ORDER BY event_timestamp 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS sma_20,
        
        AVG(close_price) OVER (
            PARTITION BY raw_symbol 
            ORDER BY event_timestamp 
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS sma_50,
        
        -- Price momentum
        close_price - LAG(close_price, 1) OVER (
            PARTITION BY raw_symbol 
            ORDER BY event_timestamp
        ) AS price_change_1m,
        
        close_price - LAG(close_price, 5) OVER (
            PARTITION BY raw_symbol 
            ORDER BY event_timestamp
        ) AS price_change_5m,
        
        close_price - LAG(close_price, 15) OVER (
            PARTITION BY raw_symbol 
            ORDER BY event_timestamp
        ) AS price_change_15m,
        
        -- Volatility (using high-low range)
        (high_price - low_price) AS price_range,
        
        AVG(high_price - low_price) OVER (
            PARTITION BY raw_symbol 
            ORDER BY event_timestamp 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS avg_range_20,
        
        -- Volume indicators
        AVG(volume) OVER (
            PARTITION BY raw_symbol 
            ORDER BY event_timestamp 
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS avg_volume_10,
        
        -- Price position within the bar
        CASE 
            WHEN high_price = low_price THEN 0.5
            ELSE (close_price - low_price) / (high_price - low_price)
        END AS close_position,
        
        -- Typical price for additional analysis
        (high_price + low_price + close_price) / 3 AS typical_price
        
    FROM price_data
)

SELECT 
    *,
    
    -- Trend indicators
    CASE 
        WHEN close_price > sma_20 THEN 'UPTREND'
        WHEN close_price < sma_20 THEN 'DOWNTREND'
        ELSE 'SIDEWAYS'
    END AS trend_direction,
    
    -- Momentum classification
    CASE 
        WHEN price_change_1m > 0 AND price_change_5m > 0 THEN 'STRONG_UP'
        WHEN price_change_1m > 0 AND price_change_5m <= 0 THEN 'WEAK_UP'
        WHEN price_change_1m < 0 AND price_change_5m < 0 THEN 'STRONG_DOWN'
        WHEN price_change_1m < 0 AND price_change_5m >= 0 THEN 'WEAK_DOWN'
        ELSE 'NEUTRAL'
    END AS momentum_classification,
    
    -- Volatility classification
    CASE 
        WHEN price_range > avg_range_20 * 1.5 THEN 'HIGH_VOLATILITY'
        WHEN price_range < avg_range_20 * 0.5 THEN 'LOW_VOLATILITY'
        ELSE 'NORMAL_VOLATILITY'
    END AS volatility_classification

FROM technical_indicators
