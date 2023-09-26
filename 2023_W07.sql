-- Preppin Data Challenge 2023 W7
-- Ben Mangel
-- 2023-09-20

-- As in many organisations, sometimes the answer to a question can only be found by combining multiple datasets together. 
--Data Source Bank are looking to find transactions which may possibly be fraudulent. 
--They define potentially fraudulent transactions as:
    -- Being more than £1,000 in value
    -- Excluding cancelled transactions 
    -- Platinum Bank Accounts have different rules for identifying fraudulent transactions so we will exclude them from our analysis
-- What the fraud team need is the ability to make phone calls to customers to double check whether these transactions are genuine.

-- For the Transaction Path table:
    -- Make sure field naming convention matches the other tables
       -- i.e. instead of Account_From it should be Account From
-- For the Account Information table:
    -- Make sure there are no null values in the Account Holder ID
    -- Ensure there is one row per Account Holder ID
        -- Joint accounts will have 2 Account Holders, we want a row for each of them
-- For the Account Holders table:
    -- Make sure the phone numbers start with 07
-- Bring the tables together
-- Filter out cancelled transactions 
-- Filter to transactions greater than £1,000 in value 
-- Filter out Platinum accounts



select * from PD2023_WK07_ACCOUNT_HOLDERS;
select * from PD2023_WK07_ACCOUNT_INFORMATION limit 5;
select * from PD2023_WK07_TRANSACTION_DETAIL;
select * from PD2023_WK07_TRANSACTION_PATH;

-- Split Values in account_holder_id column into rows
with account_information_clean as (
select 
    account_number,
    account_type,
    value as account_holder_id,
    balance_date,
    balance
from PD2023_WK07_ACCOUNT_INFORMATION, lateral split_to_table(account_holder_id, ',')
where account_holder_id is not null
),

account_holders as (
select*
from PD2023_WK07_ACCOUNT_HOLDERS
where startswith(contact_number, '7')
)

, account_holder_joined_platin as (
select *
from account_holders as holders
join account_information_clean as info on holders.account_holder_id = info.account_holder_id
where account_type != 'Platinum'
),

transactions_joined_fraud as (
select 
    detail.transaction_id,
    transaction_date,
    value,
    cancelled_,
    account_to,
    account_from
from PD2023_WK07_TRANSACTION_DETAIL as detail
join PD2023_WK07_TRANSACTION_PATH as path on detail.transaction_id = path.transaction_id
where
    value > 1000
    AND cancelled_ = 'N'
)

select 
    trans.transaction_id
    account_to,
    trans.transaction_date,
    value,
    account_number,
    account_type,
    balance_date,
    balance,
    name,
    date_of_birth,
    contact_number,
    first_line_of_address
from account_holder_joined_platin as acc
join transactions_joined_fraud as trans 
    on acc.account_number = trans.account_from
;

















