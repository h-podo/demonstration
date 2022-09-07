-- Creating the Database and Tables, Importing the Data, Setting Primary and Foreign Keys
-- DROP SCHEMA IF EXISTS `nexamp_database_2`;
-- CREATE SCHEMA `nexamp_database_2`; (Code to create new schema, commented out).
USE nexamp_database_2;

SET FOREIGN_KEY_CHECKS = 0;
SHOW GLOBAL VARIABLES LIKE 'local_infile';
SET GLOBAL local_infile = true;

CREATE TABLE customers (
customer_id	INT NOT NULL,
customer_name VARCHAR(255),
total_credits_delivered_kwh NUMERIC (10,2),
total_credits_delivered_dollar NUMERIC (10,2),
total_amount_billed_dollar NUMERIC (10,2),
PRIMARY KEY (customer_id)
);

LOAD DATA LOCAL INFILE 'C:/Users/Ray Tetreault/Documents/Roux/ITC 6000/NexampDB CSVs/customers_fd.csv'
INTO TABLE customers
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

CREATE TABLE projects (
project_id INT NOT NULL,
project_name VARCHAR(50),
project_utility VARCHAR(25),
project_size_AC INT,
crediting_method VARCHAR(25),

PRIMARY KEY (project_id)
);

LOAD DATA LOCAL INFILE 'C:/Users/Ray Tetreault/Documents/Roux/ITC 6000/NexampDB CSVs/projects_fd.csv'
INTO TABLE projects
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

CREATE TABLE billing_data (
FK_subscription_id INT NOT NULL,
utility_account_number VARCHAR(25),
FK_project_id INT NOT NULL,
period_end_date DATE,
amount_billed_kwh INT,
amount_billed_dollar NUMERIC(6,2),
invoice_status VARCHAR(25),
PRIMARY KEY (FK_subscription_id, period_end_date)
);

LOAD DATA LOCAL INFILE 'C:/Users/Ray Tetreault/Documents/Roux/ITC 6000/NexampDB CSVs/billing_data_fd.csv'
INTO TABLE billing_data
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

CREATE TABLE credit_data (
FK_utility_account_number VARCHAR(25),
FK_project_id INT NOT NULL,
FK_subscription_id INT NOT NULL,
period_start DATE,
period_end DATE,
credits_delivered_kWh INT,
credits_delivered_dollar NUMERIC (6,2),
PRIMARY KEY (FK_subscription_id, period_end),
FOREIGN KEY (FK_subscription_id) REFERENCES subscription_data(subscription_id),
FOREIGN KEY (FK_project_id) REFERENCES subscription_data(FK_project_id) 
);

LOAD DATA LOCAL INFILE 'C:/Users/Ray Tetreault/Documents/Roux/ITC 6000/NexampDB CSVs/credit_data_fd.csv'
INTO TABLE credit_data
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

CREATE TABLE project_generation (
FK_project_id INT NOT NULL,
period_end DATE,
generation_kwh INT,
FOREIGN KEY (FK_project_id) REFERENCES subscription_data(FK_project_id),
PRIMARY KEY (FK_project_id, period_end) 
);

LOAD DATA LOCAL INFILE 'C:/Users/Ray Tetreault/Documents/Roux/ITC 6000/NexampDB CSVs/project_generation.csv'
INTO TABLE project_generation
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

CREATE TABLE subscription_data (
subscription_id INT NOT NULL,
FK_customer_id INT NOT NULL,
FK_utility_account_number VARCHAR(25),
FK_project_id INT NOT NULL,
kW_allocation NUMERIC(4,1),
sub_startdate DATE,
sub_enddate DATE,
sub_status VARCHAR(100),
PRIMARY KEY (subscription_id), 
FOREIGN KEY (FK_customer_id) REFERENCES customers(customer_id),
FOREIGN KEY (subscription_id) REFERENCES billing_data(FK_subscription_id),
FOREIGN KEY (FK_project_id) REFERENCES projects(project_id) 
);

LOAD DATA LOCAL INFILE 'C:/Users/Ray Tetreault/Documents/Roux/ITC 6000/NexampDB CSVs/subscription_data_fd.csv'
INTO TABLE subscription_data
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

UPDATE subscription_data
SET sub_enddate = NULL
WHERE sub_enddate = 0000-00-00; 

UPDATE subscription_data
SET sub_startdate = NULL
WHERE sub_startdate = 0000-00-00;
-- adding nulls for blank dates
SELECT * FROM subscription_data;
-- End of Table Setup and Data Import

-- Use case 1
SELECT customer_name,sub_status,project_name
FROM customers AS c
JOIN subscription_data AS s
ON c.customer_id = s.FK_customer_id
JOIN projects AS P
ON s.FK_project_id = p.project_id
WHERE p.project_name = 'Somonauk Road Solar I'
AND sub_status = 'Accepted\r';

-- Use case 2
SELECT project_name,
COUNT(subscription_id) AS total_subscribers,
SUM(kW_allocation) AS total_kW_allocation,
ROUND(AVG(project_size_AC),0) AS project_size
FROM subscription_data AS s
LEFT JOIN projects AS p
ON s.FK_project_id = p.project_id
WHERE sub_status = 'Accepted\r'
GROUP BY project_name
ORDER BY project_size DESC
LIMIT 3
;

-- Use case 3
SELECT DISTINCT customer_name,
project_name,
total_amount_billed_dollar
FROM customers AS c
JOIN subscription_data AS s
ON c.customer_id = s.FK_customer_id
JOIN projects AS P
ON s.FK_project_id = p.project_id
-- WHERE p.project_name = 'Somonauk Road Solar I'
WHERE sub_status = 'Accepted\r'
AND project_name = 'Somonauk Road Solar I'
ORDER BY total_amount_billed_dollar DESC
LIMIT 5;

-- KPI 1: Percent of Project Allocated to Customers
WITH subscribed AS (
SELECT SUM(kW_allocation) AS kW_Allocated, FK_project_id
FROM subscription_data
WHERE sub_status = 'Accepted\r'
GROUP BY FK_project_id
)

SELECT ROUND((s.kW_Allocated / p.project_size_AC) * 100, 1) AS Percent_Subscribed, 
p.project_name AS Project
FROM subscribed AS s
JOIN projects AS p
ON s.FK_project_id = p.project_id;

-- KPI 2: Percent of Generation Billed by Project and Month
/* Create two CTE's, one hosting the sum of billed kWh
by project and month called 'billed', and the other hosting the 
total energy generated by project and month.*/
WITH billed AS( 
    SELECT SUM(amount_billed_kwh) AS kW_Billed, 
    FK_project_id, 
    MONTH(period_end_date) AS gen_month
	FROM billing_data
	GROUP BY FK_project_id, gen_month
	ORDER BY FK_project_id, gen_month),
	generation AS 
    (SELECT SUM(pg.generation_kwh) AS generation, 
    pg.FK_project_id, 
    MONTH(DATE_ADD(pg.period_end,INTERVAL -1 DAY)) AS gen_month, 
    p.project_name
	FROM project_generation AS pg
	JOIN projects AS p
	ON pg.FK_project_id = p.project_id
    GROUP BY FK_project_id, gen_month)
/* Divide the amount billed by the amount generated, 
joining on project ID and generation month.*/
SELECT ROUND((b.kW_Billed / g.generation) * 100, 1) AS pct_billed,
g.project_name, g.gen_month
FROM billed AS b
JOIN generation AS g
ON b.FK_project_id = g.FK_project_id AND b.gen_month = g.gen_month;

-- KPI 3: Customer churn by project.
WITH cancellation_detail AS (
SELECT subscription_id,
kW_allocation,
FK_project_id AS project_id,
project_name,
project_size_AC,
sub_startdate AS start,
sub_enddate AS end,
DATEDIFF(sub_enddate, sub_startdate)+ 1 AS subscription_length, 
/* date diff finds number of days between two dates, 
with +1 added to make this calculation inclusive of the first day. */
sub_status
FROM subscription_data AS s
LEFT JOIN projects
ON s.FK_project_id = projects.project_id
WHERE sub_enddate IS NOT NULL
)

SELECT  
project_name, 
COUNT(subscription_id) AS cancelled_subs,
ROUND(AVG(subscription_length),0) AS mean_cancelled_sub_length, 
ROUND(AVG(project_size_AC),0) AS project_size, 
-- workaround to preserve project_size field within an aggregate function
SUM(kw_allocation) AS kW_cancelled,
ROUND((-SUM(kw_allocation)/AVG(project_size_AC) * 100),2) AS percentage_lost
FROM cancellation_detail
GROUP BY project_name
ORDER BY percentage_lost ASC;

-- KPI 4: Generation Change by Project and Month
WITH 
project_generation_change AS (
SELECT 
	project_name, 
    FK_project_id, 
    period_end,
    generation_kwh,
    (generation_kwh/
    (LAG(generation_kwh, 1) OVER (
		PARTITION BY FK_project_id
		ORDER BY period_end) ) - 1 ) * 100 AS prev_month_generation_change,
   
   CASE WHEN month(period_end) >= 1 AND  month(period_end) <= 3 THEN 'Winter'  
    WHEN month(period_end)  >= 4 AND  month(period_end) <= 6 THEN 'Spring' 
    WHEN month(period_end)  >= 7 AND  month(period_end) <= 9 THEN 'Summer'
    ELSE 'Fall' END AS season
FROM project_generation
JOIN projects ON project_generation.FK_project_id = projects.project_id)
-- summarizing project_generation_change: NEED TO RUN ALONE WITH CTE SCRIPT ABOVE FOR OUTPUT
SELECT 
project_name,
CONCAT(month(period_end), '-' , year(period_end))AS period,
generation_kwh AS monthly_generation,
ROUND(prev_month_generation_change,2) AS pct_change,
season
FROM project_generation_change;

-- KPI #5: Project Generation Vs. the Seasonal Territory Average
WITH 
project_generation_change AS (
SELECT 
	project_name, 
    FK_project_id, 
    period_end,
    generation_kwh,
    (generation_kwh/
    (LAG(generation_kwh, 1) OVER (
		PARTITION BY FK_project_id
		ORDER BY period_end) ) - 1 ) * 100 AS prev_month_generation_change,
   CASE WHEN month(period_end) >= 1 AND  month(period_end) <= 3 THEN 'Winter'  
    WHEN month(period_end)  >= 4 AND  month(period_end) <= 6 THEN 'Spring' 
    WHEN month(period_end)  >= 7 AND  month(period_end) <= 9 THEN 'Summer'
    ELSE 'Fall' END AS season
FROM project_generation
JOIN projects ON project_generation.FK_project_id = projects.project_id),

avg_seasonal_gen AS (
SELECT
AVG(generation_kwh) AS seasonal_avg,
season
FROM project_generation_change
GROUP BY season)

SELECT project_name,
ROUND(AVG(generation_kwh),0) AS project_generation,
ROUND(a.seasonal_avg,0) AS territory_average,
ROUND(AVG(generation_kwh)/a.seasonal_avg*100,1) AS pct_of_average,
a.season
FROM project_generation_change AS p
JOIN avg_seasonal_gen AS a
ON p.season = a.season
GROUP BY project_name, season; 