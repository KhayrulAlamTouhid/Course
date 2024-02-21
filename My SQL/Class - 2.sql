/*
  1. how to create view and also replace view.
  2. describe table and view. 
  3. drop table and drop view.
  4. create, check and delete index.
  5. count, min, max, avg finding.
  6. create just table not value from existing table.
  7. load data to my new table from my existing table.
  8. Time formating.
  9. how to find which student born in holiday.
  10. make email last 3charecter in uppercase.
*/


use practice;
show tables;
select * from my_first_view;

 -- create or replace view
create or replace view my_first_view as select id, name, country, age, job_type, salary from employe where job_type = "Engineer";
select * from my_first_view;
show create view my_first_view;

-- how to describe table.
describe select * from employe
show create table employe;

/*	 why did i explain cause i wanna show if i wright query, this query scan my full table. 
	for solving this problem i can create index, if i create index than my query won't scan my full table, it just scan my index column.
    index are using for perfect low qurdinality column.
*/
EXPLAIN SELECT id, name FROM employe WHERE city = "London";
explain employe;

-- create check and drop index.
create index city_indes on employe(city);
drop index job_type_indes on employe;
show indexes in employe;

-- how can i count my full table. if i wanna count job_type wise than i have to do second query.
-- if i wanna check min, max, avg that i have to write this command.
select count(*) from employe;
select job_type, count(*) from employe group by job_type;
select job_type, max(salary) from employe group by job_type;
select job_type, min(salary) from employe group by job_type;
select job_type, avg(salary) from employe group by job_type;

-- when i wanna create table to existing table but i don't wanna data.
create table new_employe like employe;
select * from new_employe;

-- how can i load data to my new table from my existing table.
insert into new_employe select * from employe where job_type = "Engineer";

-- date formating.
select id, name, salary, state, dob from student;
SELECT DATE_FORMAT(dob, '%Y-%m-%d') AS formatted_date FROM student;
SELECT DAYNAME(dob) AS day_of_week FROM student;
SELECT id, name, salary, state, email, DATE_FORMAT(dob, '%W') as dobs, dob FROM student where DATE_FORMAT(dob, '%W') not in ("Saturday", "Friday");


WITH new_student AS (
    SELECT 
        TRIM(SUBSTRING_INDEX(email, '.', 1)) AS firstmail,
        TRIM(SUBSTRING_INDEX(email, '.', -1)) AS lastmail
    FROM student
), newanother as(
SELECT firstmail, upper(lastmail) as lastmail FROM new_student
) select concat(firstmail,".",lastmail) as Email from newanother

