{{ config(materialized='view') }}

-- Staging model for symbology data
-- Maps instrument IDs to symbols and provides contract metadata

SELECT 
    raw_symbol,
    instrument_id,
    DATE(date) as valid_date,
    
    -- Extract contract information from symbol
    CASE 
        WHEN raw_symbol LIKE 'NQ%' THEN 'NQ'
        ELSE SUBSTRING(raw_symbol, 1, 2)
    END AS underlying_symbol,
    
    -- Extract contract month/year
    CASE 
        WHEN LENGTH(raw_symbol) >= 4 THEN 
            SUBSTRING(raw_symbol, 3, 1)
        ELSE NULL
    END AS contract_month_code,
    
    CASE 
        WHEN LENGTH(raw_symbol) >= 5 THEN 
            SUBSTRING(raw_symbol, 4, 1)
        ELSE NULL
    END AS contract_year_code,
    
    -- Determine if this is a spread or outright contract
    CASE 
        WHEN raw_symbol LIKE '%-%' THEN 'SPREAD'
        ELSE 'OUTRIGHT'
    END AS contract_type,
    
    -- Map month codes to actual months
    CASE 
        WHEN SUBSTRING(raw_symbol, 3, 1) = 'F' THEN 'January'
        WHEN SUBSTRING(raw_symbol, 3, 1) = 'G' THEN 'February'
        WHEN SUBSTRING(raw_symbol, 3, 1) = 'H' THEN 'March'
        WHEN SUBSTRING(raw_symbol, 3, 1) = 'J' THEN 'April'
        WHEN SUBSTRING(raw_symbol, 3, 1) = 'K' THEN 'May'
        WHEN SUBSTRING(raw_symbol, 3, 1) = 'M' THEN 'June'
        WHEN SUBSTRING(raw_symbol, 3, 1) = 'N' THEN 'July'
        WHEN SUBSTRING(raw_symbol, 3, 1) = 'Q' THEN 'August'
        WHEN SUBSTRING(raw_symbol, 3, 1) = 'U' THEN 'September'
        WHEN SUBSTRING(raw_symbol, 3, 1) = 'V' THEN 'October'
        WHEN SUBSTRING(raw_symbol, 3, 1) = 'X' THEN 'November'
        WHEN SUBSTRING(raw_symbol, 3, 1) = 'Z' THEN 'December'
        ELSE 'Unknown'
    END AS contract_month_name

FROM {{ source('raw_data', 'symbology') }}

WHERE raw_symbol LIKE 'NQ%'
