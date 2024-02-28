/*
	1. create table and insert value on this table.
	2. how to use format, round, ceil and floor functions.
    3. formating date column and some operation.
    4. slove 2 simple task with date column.
    5. DDL operations.
*/

create table test_for_trim_value (id int, name varchar(10), salary decimal, dob varchar(15));
insert into test_for_trim_value values
(1, '  Touhid  ', 45555.6565, '25=05=2008'),
(2, '  Tamim', 54587.45654, '23=03=2005'),
(3, 'Sakib  ', 53353.7896, '28=09=2002'),
(4, '  Miraz  ', 456564.7898, '15=12=2004'),
(5, ' musfiq  ', 546887.7865, '05=04=2006');

-- Some DDL operations.
ALTER TABLE employee CHANGE COLUMN name Full_Name VARCHAR(20); -- alter column name and type.
alter table employee rename to employees_update; -- alter column values
ALTER TABLE employee ADD COLUMN department VARCHAR(50); -- adding column
create table employee as select * from employees_update; -- create table using existing table.
drop table employee; -- drop table.
UPDATE employee SET Full_name = 'Khayrul Alam' WHERE id = 1; -- update column value.
UPDATE employee SET department = 'Diploma'; -- for update column value.
alter table employee drop column department; -- for drop column on employee table.
select * from employee;
select * from test_for_trim_value;


-- For Removing data space.
select name, length(name) as name_length from test_for_trim_value; -- for using length we can find how many charecter are there. my data have some space that's why length function show that much.
select name, length(trim(name)) as trim_length from test_for_trim_value; -- if i want to exact lenth than i have to use trim function for remove space.
select name, trim(name) as trim_length from test_for_trim_value; -- for remove all spach i used trim() functions.
select name, ltrim(name) as trim_length from test_for_trim_value; -- from removeing left side space.
select name, rtrim(name) as trim_length from test_for_trim_value; -- for removing right side space.

-- formate, round, ceil and floor functions.
-- formate function for using formating value.
-- if i use round function may data like 25.56 than they provide me 26 or my data like 25.46 than they provide 25.
-- if i use ceil function and my data like 25.56 or 25.46 it alawes provide 26.
-- if i use floor function and my data like 25.56 or 25.46 it alawes provide 25. 
select salary, format(salary,2) as format_salary, round(salary) as round_of_salary, ceil(salary) as ceil_of_salary, floor(salary) as floor_of_salary from test_for_trim_value;

-- let's Play with date of birth column.
-- Formating Date Column.
SELECT str_to_date(dob, '%d=%m=%Y') AS formatted_dob FROM test_for_trim_value;
select name, date_of_birth, datediff(current_date(), date_of_birth) / 365 as age, country from employee; -- how old are they.
select name, date_of_birth, 
 date_format(date_of_birth, '%d') as Date_Number, -- this command use for check the day.
 date_format(date_of_birth, '%D') as Date_with_extention, -- this command use for check the day with extention.
 date_format(date_of_birth, '%m') as Month_Number, -- this command use for check the month number.
 date_format(date_of_birth, '%M') as Month_with_extention, -- this command use for check the month with extention.
 date_format(date_of_birth, '%y') as Year_last_two_digit, -- this command use for check the last two digite of year.
 date_format(date_of_birth, '%Y') as Year_all_digite, -- this command use for check the year.
 date_format(date_of_birth, '%w') as Weak_Number, -- this comman use for check the day of weak. which day of weak. this day start is monday
 date_format(date_of_birth, '%W') as Weak -- this command use for check weak.
 from employee;
 
 select *, monthname(date_of_birth), day(date_of_birth), dayname(date_of_birth), dayofweek(date_of_birth), dayofyear(date_of_birth) from employee;
 
 -- adding day and month on exesting table.
SELECT *,
       DATE_ADD(date_of_birth, INTERVAL 25 DAY) AS date_of_birth_plus_25_days,
       DATE_ADD(date_of_birth, INTERVAL 13 MONTH) AS date_of_birth_plus_13_months
FROM employee;

 -- i have a two task, first task is i have a quiry like that which employee born in holiday. and which employee born in 
select id, name, age, date_of_birth, country, salary from employee where year(date_of_birth) in('1999','1989'); -- Second Task
select id, name, age, date_of_birth, country, salary, date_format(date_of_birth, '%W') from employee where date_format(date_of_birth, '%W') in ("Friday","Saturday"); -- First Task
