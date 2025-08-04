{{ config(materialized='table') }}

-- Algorithm signal generation for NQ futures trading
-- Generates buy/sell signals based on technical indicators

WITH signal_base AS (
    SELECT 
        *,
        
        -- Moving Average Crossover Signals
        CASE 
            WHEN sma_10 > sma_20 
             AND LAG(sma_10, 1) OVER (PARTITION BY raw_symbol ORDER BY event_timestamp) <= 
                 LAG(sma_20, 1) OVER (PARTITION BY raw_symbol ORDER BY event_timestamp)
            THEN 'MA_CROSS_UP'
            
            WHEN sma_10 < sma_20 
             AND LAG(sma_10, 1) OVER (PARTITION BY raw_symbol ORDER BY event_timestamp) >= 
                 LAG(sma_20, 1) OVER (PARTITION BY raw_symbol ORDER BY event_timestamp)
            THEN 'MA_CROSS_DOWN'
            
            ELSE NULL
        END AS ma_crossover_signal,
        
        -- Breakout Signals
        CASE 
            WHEN close_price > LAG(high_price, 5) OVER (PARTITION BY raw_symbol ORDER BY event_timestamp)
             AND volume > avg_volume_10 * 1.5
            THEN 'BREAKOUT_UP'
            
            WHEN close_price < LAG(low_price, 5) OVER (PARTITION BY raw_symbol ORDER BY event_timestamp)
             AND volume > avg_volume_10 * 1.5
            THEN 'BREAKOUT_DOWN'
            
            ELSE NULL
        END AS breakout_signal,
        
        -- Mean Reversion Signals
        CASE 
            WHEN close_price < sma_20 * 0.995  -- 0.5% below 20-period MA
             AND price_change_1m > 0  -- but price is recovering
             AND volatility_classification = 'HIGH_VOLATILITY'
            THEN 'MEAN_REVERT_UP'
            
            WHEN close_price > sma_20 * 1.005  -- 0.5% above 20-period MA
             AND price_change_1m < 0  -- but price is falling
             AND volatility_classification = 'HIGH_VOLATILITY'
            THEN 'MEAN_REVERT_DOWN'
            
            ELSE NULL
        END AS mean_reversion_signal,
        
        -- Momentum Signals
        CASE 
            WHEN momentum_classification = 'STRONG_UP'
             AND close_position > 0.7  -- close near high of bar
             AND volume > avg_volume_10
            THEN 'MOMENTUM_UP'
            
            WHEN momentum_classification = 'STRONG_DOWN'
             AND close_position < 0.3  -- close near low of bar
             AND volume > avg_volume_10
            THEN 'MOMENTUM_DOWN'
            
            ELSE NULL
        END AS momentum_signal
        
    FROM {{ ref('int_technical_indicators') }}
),

signal_aggregation AS (
    SELECT 
        *,
        
        -- Combine signals into overall signal strength
        CASE 
            WHEN COALESCE(ma_crossover_signal, '') LIKE '%UP%' THEN 1 ELSE 0 END +
        CASE 
            WHEN COALESCE(breakout_signal, '') LIKE '%UP%' THEN 1 ELSE 0 END +
        CASE 
            WHEN COALESCE(mean_reversion_signal, '') LIKE '%UP%' THEN 1 ELSE 0 END +
        CASE 
            WHEN COALESCE(momentum_signal, '') LIKE '%UP%' THEN 1 ELSE 0 END
        AS bullish_signal_count,
        
        CASE 
            WHEN COALESCE(ma_crossover_signal, '') LIKE '%DOWN%' THEN 1 ELSE 0 END +
        CASE 
            WHEN COALESCE(breakout_signal, '') LIKE '%DOWN%' THEN 1 ELSE 0 END +
        CASE 
            WHEN COALESCE(mean_reversion_signal, '') LIKE '%DOWN%' THEN 1 ELSE 0 END +
        CASE 
            WHEN COALESCE(momentum_signal, '') LIKE '%DOWN%' THEN 1 ELSE 0 END
        AS bearish_signal_count
        
    FROM signal_base
)

SELECT 
    event_timestamp,
    trade_date,
    hour_of_day,
    raw_symbol,
    instrument_id,
    close_price,
    volume,
    
    -- Individual signals
    ma_crossover_signal,
    breakout_signal,
    mean_reversion_signal,
    momentum_signal,
    
    -- Signal strength
    bullish_signal_count,
    bearish_signal_count,
    
    -- Final algorithm decision
    CASE 
        WHEN bullish_signal_count >= 2 AND bearish_signal_count = 0 THEN 'STRONG_BUY'
        WHEN bullish_signal_count >= 1 AND bearish_signal_count = 0 THEN 'BUY'
        WHEN bearish_signal_count >= 2 AND bullish_signal_count = 0 THEN 'STRONG_SELL'
        WHEN bearish_signal_count >= 1 AND bullish_signal_count = 0 THEN 'SELL'
        ELSE 'HOLD'
    END AS algorithm_signal,
    
    -- Risk metrics for position sizing
    price_range / close_price AS risk_ratio,
    volatility_classification,
    
    -- Technical context
    trend_direction,
    momentum_classification,
    
    -- Additional context for decision making
    CASE 
        WHEN hour_of_day BETWEEN 9 AND 16 THEN 'REGULAR_HOURS'
        ELSE 'EXTENDED_HOURS'
    END AS trading_session

FROM signal_aggregation

-- Only output when we have a signal
WHERE (bullish_signal_count > 0 OR bearish_signal_count > 0)
  AND NOT has_invalid_ohlc
  
ORDER BY event_timestamp DESC
