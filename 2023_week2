-- preppin' data challenge 2023 W2
-- https://preppindata.blogspot.com/2023/01/2023-week-2-international-bank-account.html
-- Ben Mangel
-- 2023-08-23

select 
    trans.transaction_id,
    concat(
        'GB', 
        to_varchar(swift.check_digits), 
        swift.swift_code,
        to_varchar(replace(sort_code, '-', '')),
        to_varchar(trans.account_number)
        ) as IBAN
from TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK02_TRANSACTIONS as trans
inner join TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK02_SWIFT_CODES as swift on trans.bank = swift.bank
;
