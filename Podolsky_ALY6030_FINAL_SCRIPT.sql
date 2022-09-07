USE 6030_FINAL; 

DROP TABLE IF EXISTS drug_manufacture_dim;

CREATE TABLE drug_manufacture_dim (
manufacture_code INT NOT NULL,
manufacture_desc VARCHAR(100),
PRIMARY KEY (manufacture_code)
);

LOAD DATA LOCAL INFILE '/Users/harrypodolsky/Desktop/ALY 6030/Data/Podolsky_drug_generic_dim.csv'
INTO TABLE drug_manufacture_dim
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

DROP TABLE IF EXISTS drug_form_dim;

CREATE TABLE drug_form_dim (
form_code CHAR(2),
form_desc VARCHAR(100),
PRIMARY KEY (form_code)
);

LOAD DATA LOCAL INFILE '/Users/harrypodolsky/Desktop/ALY 6030/Data/Podolsky_drug_form_dim.csv'
INTO TABLE drug_form_dim
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

DROP TABLE IF EXISTS drug_detail_dim;

CREATE TABLE drug_detail_dim (
drug_ndc INT NOT NULL,
drug_name VARCHAR(100),
PRIMARY KEY (drug_ndc)
);

LOAD DATA LOCAL INFILE '/Users/harrypodolsky/Desktop/ALY 6030/Data/Podolsky_drug_detail_dim.csv'
INTO TABLE drug_detail_dim
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

DROP TABLE IF EXISTS customer_dim;

CREATE TABLE customer_dim (
member_id INT NOT NULL,
first_name VARCHAR(100),
last_name VARCHAR(100),
birth_date DATE,
age INT,
gender VARCHAR(25),
PRIMARY KEY (member_id)
);

LOAD DATA LOCAL INFILE '/Users/harrypodolsky/Desktop/ALY 6030/Data/Podolsky_customer_dim.csv'
INTO TABLE customer_dim
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

DROP TABLE IF EXISTS fill_detail_fact;

CREATE TABLE fill_detail_fact (
fill_id INT NOT NULL,
member_id INT,
drug_ndc INT,
form_code CHAR(2),
manufacture_code INT,
fill_date DATE,
copay INT,
insurance_paid INT,
PRIMARY KEY (fill_id),
FOREIGN KEY (member_id) REFERENCES customer_dim(member_id) ON DELETE RESTRICT,
FOREIGN KEY (drug_ndc) REFERENCES drug_detail_dim(drug_ndc) ON DELETE RESTRICT,
FOREIGN KEY (form_code) REFERENCES drug_form_dim(form_code) ON DELETE RESTRICT,
FOREIGN KEY (manufacture_code) REFERENCES drug_manufacture_dim(manufacture_code) ON DELETE RESTRICT
);

LOAD DATA LOCAL INFILE '/Users/harrypodolsky/Desktop/ALY 6030/Data/Podolsky_fill_detail_fact.csv'
INTO TABLE fill_detail_fact
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

# ANALYTICS AND REPORTING

SELECT drug_name, COUNT(*) AS number_of_transactions 
FROM fill_detail_fact
LEFT JOIN drug_detail_dim USING (drug_ndc)
GROUP BY drug_ndc;

WITH cte1 AS (
	SELECT f.*, CASE 
		WHEN c.age > 65 THEN "age 65+"
		WHEN c.age <= 65 THEN "age < 65"
	END AS age_category 
	FROM fill_detail_fact f
	LEFT JOIN customer_dim c 
	USING (member_id)
)
SELECT age_category,
COUNT(*) AS total_transactions, 
COUNT(DISTINCT member_id) AS member_count, 
SUM(copay) AS copay_spend,
SUM(insurance_paid) AS insurance_paid
FROM cte1
GROUP BY age_category;

SELECT t.member_id id, t.first_name first_name, 
t.last_name last_name, t.drug_name drug_name, 
t.recent_fill recent_fill, t.insurance_paid insurance_paid
FROM (
SELECT member_id, first_name, last_name, drug_name,fill_date, 
LAST_VALUE(fill_date) OVER (PARTITION BY MEMBER_ID) recent_fill, 
insurance_paid
FROM customer_dim 
LEFT JOIN fill_detail_fact USING (member_id)
LEFT JOIN drug_detail_dim USING (drug_ndc)
) t
WHERE fill_date = recent_fill
;