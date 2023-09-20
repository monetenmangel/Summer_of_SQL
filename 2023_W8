-- Preppin' Data 2023 Week 08

-- Create a 'file date' using the month found in the file name
--     - The Null value should be replaced as 1
-- Clean the Market Cap value to ensure it is the true value as 'Market Capitalisation'
--     - Remove any rows with 'n/a'
-- Categorise the Purchase Price into groupings
    -- 0 to 24,999.99 as 'Low'
    -- 25,000 to 49,999.99 as 'Medium'
    -- 50,000 to 74,999.99 as 'High'
    -- 75,000 to 100,000 as 'Very High'
-- Categorise the Market Cap into groupings
    -- Below $100M as 'Small'
    -- Between $100M and below $1B as 'Medium'
    -- Between $1B and below $100B as 'Large' 
    -- $100B and above as 'Huge'
-- Rank the highest 5 purchases per combination of: file date, Purchase Price Categorisation and Market Capitalisation Categorisation.
-- Output only records with a rank of 1 to 5

select * from PD2023_WK08_01;

WITH main AS (
SELECT 1 as file,* FROM pd2023_wk08_01

UNION ALL 

SELECT 2 as file,* FROM pd2023_wk08_02

UNION ALL 

SELECT 3 as file,* FROM pd2023_wk08_03

UNION ALL 

SELECT 4 as file,* FROM pd2023_wk08_04

UNION ALL 

SELECT 5 as file,* FROM pd2023_wk08_05

UNION ALL 

SELECT 6 as file,* FROM pd2023_wk08_06

UNION ALL 

SELECT 7 as file,* FROM pd2023_wk08_07

UNION ALL 

SELECT 8 as file,* FROM pd2023_wk08_08

UNION ALL 

SELECT 9 as file,* FROM pd2023_wk08_09

UNION ALL 

SELECT 10 as file,* FROM pd2023_wk08_10

UNION ALL 

SELECT 11 as file,* FROM pd2023_wk08_11

UNION ALL 

SELECT 12 as file,* FROM pd2023_wk08_12
)
select *
from (
    select 
        date_from_parts(2023, file, 01) as month,
        ID, FIRST_NAME, LAST_NAME, TICKER, SECTOR, MARKET, STOCK_NAME,
        -- Convert purchase price in FLOAT
        replace(PURCHASE_PRICE, '$', '')::float as purchase_price,
    
        -- Introduce purchase price categories
        case
            when replace(PURCHASE_PRICE, '$', '')::float <=25000 then 'Low'
            when replace(PURCHASE_PRICE, '$', '')::float <=50000 then 'Medium'
            when replace(PURCHASE_PRICE, '$', '')::float <=75000 then 'High'
            when replace(PURCHASE_PRICE, '$', '')::float <=100000 then 'Very High'
            else 'Extraordinary'
        END as purchase_price_category,
    
        -- introduce purchase price categories
        -- bc they end with M for millions or B for billions transform them
        -- those which are normal stay like they are
        -- strip $ signs off
        CASE 
            WHEN RIGHT(market_cap, 1) = 'M' THEN
                REPLACE(REPLACE(market_cap, '$', ''), 'M', '')::FLOAT * 1000000
            WHEN RIGHT(market_cap, 1) = 'B' THEN
                REPLACE(REPLACE(market_cap, '$', ''), 'B', '')::FLOAT * 1000000000
            when market_cap = 'n/a' then null
            ELSE
                REPLACE(market_cap, '$', '')::FLOAT
        END AS market_cap_converted,
    
        -- introduce market cap groups
        case
            when market_cap_converted < 100000000 then 'Small'
            when market_cap_converted < 1000000000 then 'Medium'
            when market_cap_converted < 100000000000 then 'Large'
            else 'Huge' 
        END as market_cap_category,
    
        ROW_NUMBER() OVER(PARTITION BY purchase_price_category, market_cap_category, month ORDER BY purchase_price) AS rank
        
    from main
    where 
        market_cap_converted is not null
    )

where rank <= 5
;
