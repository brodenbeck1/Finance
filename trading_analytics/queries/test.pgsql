select *
from trading_dev_staging.stg_ohlcv_1m
where date(event_timestamp) = '2010-06-30'
;
select *
from raw_data.ohlcv_1m
where date(ts_event) = '2010-06-30';

;
--DROP VIEW IF EXISTS trading_dev_staging.stg_ohlcv_1m

;

 SELECT 
        DATE(REPLACE(ts_event, 'Z', '')) AS trade_date,
        symbol AS raw_symbol,
        SUM(volume) AS daily_volume
    FROM raw_data.ohlcv_1m
    WHERE DATE(ts_event) = '2010-06-30'
    GROUP BY trade_date, raw_symbol
    ;
