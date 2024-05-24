################  Operation Analytics and Investigating Metric Spike  ###################


SELECT 
    *
FROM
    job_data;
select count(distinct j_language) from job_data as total;
select j_language as Languages, round(100 *count(*)/6, 2) as Percentage
from job_data
cross join (SELECT COUNT(*) FROM job_data) jobs
group by j_language;


#Persian language is highest with 37.5% total.

# duplicates
select actor_id, count(*) as times 
from job_data
group by actor_id 
having count(*)>1;




#2nd task
#creating database

create database Project3;
use project3;

#table 1 users
CREATE TABLE users (
    user_id INT,
    created_at VARCHAR(100),
    company_id INT,
    language VARCHAR(50),
    activated_at VARCHAR(100),
    state VARCHAR(50)
);

show variables like 'secure_file_priv';
#files should be at path described in above querie's output

#importing dataset
load data infile"C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv"
into table users
fields terminated by','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

select * from users;

alter table users add column temp_created_at datetime;
update users set temp_created_at = STR_TO_DATE(created_at, '%d-%m-%Y %H:%i');   # after doing sql_safe_updates=0
alter table users drop column created_at;
alter table users change column temp_created_at created_at datetime;

# table 2 events
create table events(
user_id int,
occurred_at varchar(100) ,
event_type varchar(50) ,
event_name varchar(100) ,
location varchar(50) ,
device varchar(50) ,
user_type int  );
show variables like 'secure_file_priv';

load data infile"C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv"
into table events
fields terminated by','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

alter table events add column temp_occurred_at datetime;
SET SQL_SAFE_UPDATES = 0;
update events set temp_occurred_at = STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i');   # after doing sql_safe_updates=0
alter table events drop column occurred_at;
alter table events change column temp_occurred_at occurred_at datetime;

#table-3

create table EmailEvents(user_id int, occurred_at varchar(100),action varchar(100),user_type int);

load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv"
into table EmailEvents
fields terminated by','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

alter table EmailEvents add column temp_occurred_at datetime;
SET SQL_SAFE_UPDATES = 0;
update EmailEvents set temp_occurred_at = STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i');   # after doing sql_safe_updates=0
alter table EmailEvents drop column occurred_at;
alter table EmailEvents change column temp_occurred_at occurred_at datetime;

select * from users;
select * from events;
select * from EmailEvents;
# weekly user engagement
# no of users per week

SELECT 
    EXTRACT(WEEK FROM occurred_at) AS 'week',
    COUNT(DISTINCT user_id) AS 'active users'
FROM
    events
WHERE
    event_type = 'engagement'
GROUP BY week;

#amount of user growth for product

select months, Users, round(((Users/LAG(Users,1) over (order by months) - 1) *100),2)as "growth in %" from
(
select extract(MONTH FROM created_at) as months, count(activated_at)as Users
from users
where activated_at not in("")
group by 1
order by 1
)sub;

select months, Users, round(((Users/LAG(Users,1) over (order by months) - 1) *100),2)as "growth in %" from (
select extract(MONTH FROM created_at) as months, count(activated_at)as Users
from users
where activated_at not in("")
group by 1
order by 1
)growth;



SELECT first AS "Week Numbers",

SUM(CASE WHEN week_number=0 THEN 1 ELSE 0 END) AS "Week 0",

SUM(CASE WHEN week_number=1 THEN 1 ELSE 0 END) AS "Week 1",

SUM(CASE WHEN week_number=2 THEN 1 ELSE 0 END) AS "Week 2",

SUM(CASE WHEN week_number=3 THEN 1 ELSE 0 END) AS "Week 3",

SUM(CASE WHEN week_number=4 THEN 1 ELSE 0 END) AS "Week 4",

SUM(CASE WHEN week_number=5 THEN 1 ELSE 0 END) AS "Week 5",

SUM(CASE WHEN week_number=6 THEN 1 ELSE 0 END) AS "Week 6",

SUM(CASE WHEN week_number=7 THEN 1 ELSE 0 END) AS "Week 7",

SUM(CASE WHEN week_number=8 THEN 1 ELSE 0 END) AS "Week 8",

SUM(CASE WHEN week_number=9 THEN 1 ELSE 0 END) AS "Week 9",

SUM(CASE WHEN week_number=10 THEN 1 ELSE 0 END) AS "Week 10",

SUM(CASE WHEN week_number=11 THEN 1 ELSE 0 END) AS "Week 11",

SUM(CASE WHEN week_number=12 THEN 1 ELSE 0 END) AS "Week 12",

SUM(CASE WHEN week_number=13 THEN 1 ELSE 0 END) AS "Week 13",

SUM(CASE WHEN week_number=14 THEN 1 ELSE 0 END) AS "Week 14",

SUM(CASE WHEN week_number=15 THEN 1 ELSE 0 END) AS "Week 15",

SUM(CASE WHEN week_number=16 THEN 1 ELSE 0 END) AS "Week 16",

SUM(CASE WHEN week_number=17 THEN 1 ELSE 0 END) AS "Week 17",

SUM(CASE WHEN week_number=18 THEN 1 ELSE 0 END) AS "Week 18"
FROM
(
SELECT m.user_id, m.login_week, n.first, m.login_week-first AS week_number
FROM
(SELECT user_id, EXTRACT( WEEK FROM occurred_at) AS login_week FROM events
GROUP BY 1,2)m,
(SELECT user_id, MIN(EXTRACT(WEEK FROM occurred_at)) AS first FROM events
GROUP BY 1)n
WHERE m.user_id=n.user_id)
sub
GROUP BY first
ORDER BY first;
