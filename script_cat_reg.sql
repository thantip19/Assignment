CREATE TABLE cat_reg AS
select tb.category as "Category",cast(sum(tb.east) as integer) as "East",
cast(sum(tb.west) as integer) as "West",cast(sum(tb.east)+sum(tb.west) as integer) as "Grand Total" 
from(select category,
CASE region WHEN 'East' THEN totalprice  ELSE 0 END AS East,
CASE region WHEN 'West' THEN totalprice  ELSE 0 END AS West
from food_sales) as tb 
group by tb.category 
order by tb.category