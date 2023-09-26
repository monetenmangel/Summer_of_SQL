-- Preppin' Data 2023 Week 09
-- Ben Mangel
-- 2023-09-25

-- For the Transaction Path table:
--     Make sure field naming convention matches the other tables
--         - i.e. instead of Account_From it should be Account From
-- Filter out the cancelled transactions
-- Split the flow into incoming and outgoing transactions 
-- Bring the data together with the Balance as of 31st Jan 
-- Work out the order that transactions occur for each account
--     Hint: where multiple transactions happen on the same day, assume the highest value transactions happen first
-- Use a running sum to calculate the Balance for each account on each day 
-- The Transaction Value should be null for 31st Jan, as this is the starting balance
-- Output the data

select * from PD2023_WK07_ACCOUNT_INFORMATION;
select * from PD2023_WK07_TRANSACTION_DETAIL;
select * from PD2023_WK07_TRANSACTION_PATH;

-- clean account information
with account_information_clean as (
select 
    distinct account_number,
    balance_date,
    balance
-- split accounts which are in 1 cell to rows
from PD2023_WK07_ACCOUNT_INFORMATION, lateral split_to_table(account_holder_id, ',')
where account_holder_id is not null
),

-- incoming stream -> everything that goes into account (account_to)
income_stream as (
select
    p.account_to as account_number,
    cast(transaction_date as date) as transaction_date,
    d.value
from PD2023_WK07_TRANSACTION_DETAIL as d
join PD2023_WK07_TRANSACTION_PATH as p on d.transaction_id = p.transaction_id
where cancelled_ = 'N'
),

-- outgoing stream -> everything that is leaving the account
outgoing_stream as (
select
    p.account_from as account_number,
    cast(transaction_date as date) as transaction_date,
    -- make the amount negative, because it is leaving the account
    d.value * (-1)
from PD2023_WK07_TRANSACTION_DETAIL as d
join PD2023_WK07_TRANSACTION_PATH as p on d.transaction_id = p.transaction_id
where cancelled_ = 'N'
),

first_transaction as (
select 
    account_number,
    cast(balance_date as date) as transaction_date,
    NULL as value    
from account_information_clean
),

-- union ingoing and outgoing transactions together + fake transactions for a starting point with NULL values
transactions_union as (
select *
from income_stream
UNION
select * from outgoing_stream
UNION
select * from first_transaction
)

-- join unioned transactions and balance together and calculate the new balance

select
    t.account_number,
    t.transaction_date,
    t.value,
    --a.balance
    case
        when value is null then balance
        else
        -- calculate the cumultative / running sum for every account ordered by date and value
        sum(value) over (partition by t.account_number order by transaction_date, value desc rows between unbounded preceding and current row) -- cumulative sum
        -- add the original balance
        + balance
    end as balance_calculated
from transactions_union as t
join account_information_clean as a on t.account_number = a.account_number
;
