
-- Preppin' Data 2023 Week 10

-- Aggregate the data so we have a single balance for each day already in the dataset, for each account
-- Scaffold the data so each account has a row between 31st Jan and 14th Feb
-- Make sure new rows have a null in the Transaction Value field
-- Create a parameter so a particular date can be selected
-- Filter to just this date
-- Output the data 

-- Prepare the data as in Week 09

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
),

-- join unioned transactions and balance together and calculate the new balance
transactions as (
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
),

-- select * from transactions limit 5;


-- #######################################################################################
-- Aggregate the data so we have a single balance for each day already in the dataset, for each account
-- Scaffold the data so each account has a row between 31st Jan and 14th Feb
-- Make sure new rows have a null in the Transaction Value field
-- Create a parameter so a particular date can be selected
-- Filter to just this date
-- Output the data 
-- #######################################################################################

-- Assuming you set your parameter like this
-- SET MaxDate = '2023-02-09'

last_balance as (
select *
from (
        select 
            account_number,
            transaction_date,
            balance_calculated,
            --sum(value) as aggregated_value,
            row_number() over (Partition by account_number, transaction_date order by transaction_date, value asc) as rn
        from transactions
        )
where rn = 1
),

aggregated_transactions as (
select 
    t.account_number,
    t.transaction_date,
    sum(t.value) as aggregated_value,
    l.balance_calculated
from transactions as t
join last_balance as l on t.account_number = l.account_number AND t.transaction_date = l.transaction_date
group by
    t.account_number,
    t.transaction_date,
    l.balance_calculated
)

-- select * from aggregated_transactions where account_number = 10005367;

,account_numbers as (
select 
    distinct account_number
from aggregated_transactions
)

-- select * from account_numbers;

,min_date as (
select 
    min(transaction_date) as min_transaction_date,
    a.account_number
from aggregated_transactions
join account_numbers as a on 1=1
group by a.account_number
)
-- select * from min_date;

,scaffold as (
select 
    min_transaction_date as start_date,
    account_number
from min_date

union all 

select 
    dateadd('day', 1, start_date) as end_date,
    account_number
from scaffold
where end_date < '2023-02-28'
)

,scaffold_unique as (
select 
    one.start_date,
    one.account_number
from scaffold as one
inner join scaffold as two on one.start_date = two.start_date AND one.account_number = two.account_number
)

-- select * from scaffold where account_number = 10005367;

,scaffolded_transactions as (
select
    s.account_number,
    s.start_date,
    t.balance_calculated as balance,
    t.aggregated_value as value
--from (select * from scaffold_unique where account_number = 10005367) as s
from scaffold_unique as s
left join aggregated_transactions as t 
    on t.account_number = s.account_number 
    AND t.transaction_date = s.start_date
--where t.account_number = 10005367
order by start_date
)

-- get rownumbers for every account
,rownumbers as (
select
    account_number,
    start_date,
    balance,
    value,
    row_number() over (partition by account_number order by start_date) as rn
from scaffolded_transactions
)

-- keep only the row numbers for the rows where I have a valid balance
,valid_nulls as (
select
    account_number,
    start_date,
    balance,
    value,
    rn,
    case
        when balance is not NULL
        then rn
        else NULL
    END as valid_rn
from rownumbers
)

-- take the max 
,fill_row_numbers as(
select
    account_number,
    start_date,
    balance,
    value,
    rn,
    max(valid_rn) over (partition by account_number order by start_date rows unbounded preceding) as filled_rn
from valid_nulls
)

-- join the filled row number with the original row number
select
    f.account_number,
    f.start_date as date,
    f.value,
    r.balance
from fill_row_numbers as f
-- join the (balance) filled row number on the old row number
left join rownumbers as r on f.account_number = r.account_number and f.filled_rn = r.rn
where f.start_date = '2023-02-01'
order by 
    f.account_number,
    f.start_date
;
