-- preppin' data challenge 2023 W1
-- https://preppindata.blogspot.com/2023/01/2023-week-1-data-source-bank.html
-- Ben Mangel
-- 2023-08-23

-- Output 1: Total Values of Transactions by each bank

select 
    split_part(transaction_code, '-', 1) as Bank,
    sum(value) as Value
from TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK01 as db
group by Bank;

--------------------------------------------------------------------------
-- Output 2: Total Values by Bank, Day of the Week and Type of Transaction

select 
    case online_or_in_person
        when 1 then 'Online'
        when 2 then 'In-Person'
    End as ONLINE_OR_IN_PERSON,
    DAYNAME(DATE(transaction_date,'dd/MM/yyyy hh24:mi:ss')) as day_of_week,
    split_part(transaction_code, '-', 1) as Bank,
    sum(value) as Value
from TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK01 as db
group by bank, online_or_in_person, day_of_week
;

-- Output 3: Total Values by Bank and Customer Code

select
    split_part(transaction_code, '-', 1) as Bank,
    sum(value) as Value,
    to_varchar(customer_code) as customer_code
from TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK01 as db
group by bank, customer_code
;



