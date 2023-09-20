-- Preppin' Data Challenge 2023 W6 // https://preppindata.blogspot.com/2023/02/2023-week-6-dsb-customer-ratings.html
-- Ben Mangel
-- 2023-09-20


-- Reshape the data so we have 5 rows for each customer, with responses for the Mobile App and Online Interface being in separate fields on the same row
-- Clean the question categories so they don't have the platform in from of them
--     - e.g. Mobile App - Ease of Use should be simply Ease of Use
-- Exclude the Overall Ratings, these were incorrectly calculated by the system
-- Calculate the Average Ratings for each platform for each customer 
-- Calculate the difference in Average Rating between Mobile App and Online Interface for each customer
-- Catergorise customers as being:
--     - Mobile App Superfans if the difference is greater than or equal to 2 in the Mobile App's favour
--     - Mobile App Fans if difference >= 1
--     - Online Interface Fan
--     - Online Interface Superfan
--     - Neutral if difference is between 0 and 1
-- Calculate the Percent of Total customers in each category, rounded to 1 decimal place


-- get overview
select * from TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK06_DSB_CUSTOMER_SURVEY
limit 5;

-- ##################################################################################
-- Actual query:

with unpivoted as (
-- Split pivot_columns from subquery
select 
    customer_id,
    split_part(pivot_columns, '___', 1) as platform,
    split_part(pivot_columns, '___', 2) as category,
    value
FROM (
    -- unpivot every columns, besides customer_id
    -- one column for the values (value)
    -- pivot columns for the column header
    SELECT *
    from PD2023_WK06_DSB_CUSTOMER_SURVEY
    unpivot(
        value for pivot_columns
        in (
        MOBILE_APP___EASE_OF_USE, MOBILE_APP___EASE_OF_ACCESS, MOBILE_APP___NAVIGATION, MOBILE_APP___LIKELIHOOD_TO_RECOMMEND, MOBILE_APP___OVERALL_RATING, ONLINE_INTERFACE___EASE_OF_USE, ONLINE_INTERFACE___EASE_OF_ACCESS, ONLINE_INTERFACE___NAVIGATION, ONLINE_INTERFACE___LIKELIHOOD_TO_RECOMMEND, ONLINE_INTERFACE___OVERALL_RATING)
    ))
where Category != 'OVERALL_RATING'
),
-- Get aggregated avg values for each customer on every platform
avg_values as (
select 
    customer_id,
    platform,
    avg(value) as avg_value
from unpivoted
group by customer_id, platform
),
Superfan as (
-- Pivot values for every platform in seperate columns, to substract them from each other
select 
    customer_id,
    ("'MOBILE_APP'" - "'ONLINE_INTERFACE'") as difference,
    case
        when difference >= 2 then 'Mobile App Superfan'
        when difference >= 1 then 'Mobile App Fan'
        when difference <= -2 then 'Online Interface Superfan'
        when difference <= -1 then 'Online Interface Fan'
        else 'Neutral'
    End as preference
from avg_values
pivot(SUM(avg_value) FOR platform IN ('MOBILE_APP','ONLINE_INTERFACE')) as p
)
select
    round(count(preference) / (select count(customer_id) from superfan) * 100, 1) as percentage,
    preference
from superfan
group by preference;
