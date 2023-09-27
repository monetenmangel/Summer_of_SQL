-- Preppin' Data 2023 Week 11
-- Ben Mangel
-- 2023-09-27

-- Append the Branch information to the Customer information
-- Transform the latitude and longitude into radians
-- Find the closest Branch for each Customer
--     Make sure Distance is rounded to 2 decimal places
-- For each Branch, assign a Customer Priority rating, the closest customer having a rating of 1
-- Output the data

select * from PD2023_WK11_DSB_BRANCHES;
select * from PD2023_WK11_DSB_CUSTOMER_LOCATIONS;

with cte as (
    select 
        customer,
        radians(address_long) as address_long_radian,
        radians(address_lat) as address_lat_radian,
        radians(branch_lat) as branch_lat_radian,
        radians(branch_long) as branch_long_radian,
        radians(branch_long) - radians(address_long) as long_diff,
        branch
    from PD2023_WK11_DSB_CUSTOMER_LOCATIONS
    cross join PD2023_WK11_DSB_BRANCHES
)
, distances as (
select 
    customer,
    branch,
    round(
        3963 * acos(
            (sin(address_lat_radian) * sin(branch_lat_radian)) 
            + 
            (cos(address_lat_radian) * cos(branch_lat_radian) * cos(long_diff))
            )
        , 2) as distance,
    row_number() over (partition by customer order by distance asc) as rn,
    address_lat_radian,
    address_long_radian,
    branch_lat_radian,
    branch_long_radian
from cte
order by customer, rn
)

select 
    customer,
    branch,
    distance,
    row_number() over (partition by branch order by distance asc) as priority,
    address_lat_radian,
    address_long_radian,
    branch_lat_radian,
    branch_long_radian
from distances
where rn = 1
;
