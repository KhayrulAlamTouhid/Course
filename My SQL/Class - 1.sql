/*
	1. some select statment.
    2. if else statment.
    3. case when and then statment.
	4. Alias 
    5. create table and insert values.
*/

-- Create table with schema
CREATE TABLE employe (
    id INT,
    name VARCHAR(255),
    country VARCHAR(255),
    age INT,
    job_type VARCHAR(255),
    city VARCHAR(255),
    salary FLOAT
);

-- How to go to inside the database.
use practice;

-- 
SELECT DISTINCT
    price
FROM
    bde_lporject
ORDER BY price DESC;
SELECT 
    *,
    Price * 5 AS bonus,
    IF(Price > 3600, Price * .1, 0) AS total
FROM
    bde_lporject
ORDER BY total DESC;

select 
	Transaction_data, 
	Price, 
    Name, 
    City, 
	Country, 
	case 
		when(price > 3600) then "High" 
        when(price > 2100) then "Avarege" 
        when(price > 1800) then "Low" 
        when(price > 1200) then "Very Low"
        else "On Market" 
	end as Total 
from bde_lporject
order by Total desc;

CREATE TABLE salary_cast AS SELECT Name,
    City,
    Country,
    price,
    CASE
        WHEN (price > 3600) THEN 'High'
        WHEN (price > 2100) THEN 'Avarege'
        WHEN (price > 1800) THEN 'Low'
        WHEN (price > 1200) THEN 'Very Low'
        ELSE 'On Market'
    END AS Total FROM
    bde_lporject
ORDER BY Total DESC;

select distinct total from salary_cast;

select * from bde_lporject;
select 
	Transaction_data, 
	Price, 
    Name, 
    City, 
	Country, 
	case 
		when(Country = "Bangladesh") then "Bd" 
        else Country 
	end as Total 
from bde_lporject;

select transaction_data, Name, replace(Country, "Australia", "Ocenia") from bde_lporject where replace(Country, "Australia", "Ocenia") = 'Ocenia';