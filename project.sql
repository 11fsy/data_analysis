CREATE SCHEMA IF NOT EXISTS dh;
-- CREATE TABLES
DROP TABLE IF EXISTS dh.transactions;
CREATE TABLE dh.transactions(
household_id INT,
basket_id BIGINT,
transaction_day INT,
product_id INT,
quantity NUMERIC,
sales_value NUMERIC,
store_id INT,
retail_disc NUMERIC,
transaction_time INT,
week_no INT,
coupon_disc NUMERIC,          
coupon_match_disc NUMERIC
);

DROP TABLE IF EXISTS dh.products;
CREATE TABLE dh.products(
product_id INT PRIMARY KEY,
manufacturer INT,
department TEXT,
brand TEXT,
commodity_desc TEXT,
sub_commodity_desc TEXT,
current_size TEXT
);

DROP TABLE IF EXISTS dh.households;
CREATE TABLE dh.households(
age_desc TEXT,
marital_status TEXT,
income_desc TEXT,
homeowner_desc TEXT,
comp_desc TEXT,
household_size_desc TEXT,
kid_category_desc TEXT, 
household_id INT PRIMARY KEY
);

-- CREATE INDEX FOR SPEEDING UP DATA RETRIEVAL
CREATE INDEX ON dh.transactions(household_id);
CREATE INDEX ON dh.transactions(basket_id);
CREATE INDEX ON dh.transactions(product_id);

--BUILD A DATA CALENDAR
--VERIFICATION
WITH day_range AS(
SELECT MIN(transaction_day) AS min_day,
MAX(transaction_day) as max_day
FROM dh.transactions
)
SELECT * FROM day_range;

-- BUILD ORDER TABLE
DROP VIEW IF EXISTS dh.orders;
CREATE VIEW dh.orders AS
SELECT 
customer_id,
order_id,
(DATE'2017-01-02'+(day_index - min_day_all))::date AS order_date,
date_trunc('week',(DATE'2017-01-02'+(day_index - min_day_all)))::date AS week_start,
date_trunc('month',(DATE'2017-01-02'+(day_index - min_day_all)))::date AS month_start,
units,revenue, total_discount,distinct_skus,store_id
FROM(
SELECT
t.household_id AS customer_id,
t.basket_id AS order_id,
MIN(t.transaction_day) AS day_index,
MIN(MIN(t.transaction_day)) OVER() min_day_all,
SUM(t.quantity) AS units,
SUM(t.sales_value) AS revenue,
SUM(COALESCE(retail_disc,0)+ COALESCE(t.coupon_disc,0)+COALESCE(t.coupon_match_disc,0)) AS total_discount,
COUNT(DISTINCT t.product_id) AS distinct_skus,
MIN(t.store_id)AS store_id
FROM dh.transactions AS t
GROUP BY t.household_id,t.basket_id);
-- FIRST ORDER PER CUSTOMER
DROP VIEW IF EXISTS dh.first_orders;
CREATE VIEW dh.first_orders AS
SELECT 
customer_id,
MIN(order_date) AS first_order_date,
DATE_TRUNC('month', MIN(order_date))::date AS cohort_month
FROM dh.orders
GROUP BY customer_id;
--MONTHLY SUMMARY
DROP VIEW IF EXISTS dh.month_summary;
CREATE VIEW dh.month_summary AS
SELECT 
o.month_start,
COUNT(DISTINCT o.customer_id) AS active_customers,
COUNT(*) AS orders,
ROUND(1.0*SUM(o.revenue),2) as revenue,
ROUND(1.0*SUM(o.revenue)/COALESCE(COUNT(*),0),2) AS AOV,
1.0*COUNT(*)/COALESCE(COUNT(DISTINCT o.customer_id),0) AS orders_per_active
FROM dh.orders AS o
GROUP BY o.month_start
ORDER BY o.month_start;
--GROWTH ACCOUNTING
-- active: customers with >=1 order in month m
-- new: customers whose first order is in month m
-- resurrected:active in month m,but not active in m-1, but had orders before m-1
-- churned: active in m-1, but not active in m 
DROP VIEW IF EXISTS dh.active_by_month;
CREATE VIEW dh.active_by_month AS
SELECT DISTINCT
o.customer_id,
o.month_start AS active_month
FROM dh.orders AS o;

--GROWTH COMPONENTS
DROP VIEW IF EXISTS dh.growth_components;
CREATE VIEW dh.growth_components AS
WITH months AS(
SELECT DISTINCT active_month AS month
FROM dh.active_by_month
),
active_now AS(
SELECT active_month,
customer_id
FROM dh.active_by_month
),
active_prev AS(
SELECT m.month,
a.customer_id
FROM dh.active_by_month AS a
JOIN months AS m ON a.active_month = (m.month - INTERVAL'1 month')
)
SELECT 
m.month,
--active
(SELECT COUNT(*) FROM active_now an WHERE an.active_month = m.month) AS active,
--new
(SELECT COUNT(*) FROM active_now an JOIN dh.first_orders f ON f.customer_id = an.customer_id
WHERE an.active_month = m.month 
AND f.cohort_month = m.month)::INT AS new_customers,
--resurrected
(SELECT COUNT(*) FROM active_now an LEFT JOIN active_prev ap ON ap.month = an.active_month AND ap.customer_id = an.customer_id
WHERE an.active_month = m.month 
AND ap.customer_id IS NULL
AND EXISTS(
SELECT 1 FROM dh.orders o
WHERE o.customer_id = an.customer_id
AND o.month_start < (m.month - INTERVAL'1 month')
)
)::INT AS resurrected,
--churned
(SELECT COUNT(*) FROM active_prev ap 
WHERE ap.month = m.month 
AND ap.customer_id NOT IN(
SELECT an.customer_id
FROM active_now an
WHERE an.active_month = m.month
)
)::INT AS churned
FROM months m
GROUP BY m.month;

--active = new + resurrected - churned
DROP VIEW IF EXISTS dh.growth_accounting;
CREATE VIEW dh.growth_accounting AS
SELECT month,
active,
new_customers,
resurrected,
churned,
(new_customers+resurrected-churned)AS delta_active
FROM dh.growth_components
ORDER BY month;

--revenue factorization
revenue = active * orders/active * AOV
DROP VIEW IF EXISTS dh.revenue_factors;
CREATE VIEW dh.revenue_factors AS
SELECT m.month_start AS month,
m.active_customers,
m.orders_per_active,
m.AOV,
m.revenue
FROM dh.month_summary m
ORDER BY m.month_start;

--cohort LTV(4/8/12/24 weeks from first order)
DROP VIEW IF EXISTS dh.ltv_weeks;
CREATE VIEW dh.ltv_weeks AS
WITH firsts AS(
SELECT customer_id, 
MIN(order_date)AS first_date
FROM dh.orders
GROUP BY customer_id
),
events AS(
SELECT o.customer_id,
o.order_id,
(o.revenue - COALESCE(o.total_discount,0)) AS net_revenue,
f.first_date,
(o.order_date - f.first_date) AS day_since_first
FROM dh.orders o
JOIN firsts f ON o.customer_id = f.customer_id
),
bucket AS(
SELECT customer_id,
DATE_TRUNC('month',first_date)::date AS cohort_month,
net_revenue,
CASE
WHEN day_since_first <=30 THEN 30
WHEN day_since_first <=60 THEN 60
WHEN day_since_first <=90 THEN 90
WHEN day_since_first <=180 THEN 180
END AS day_bucket
FROM events
WHERE day_since_first<=180
)
SELECT cohort_month,
day_bucket,
COUNT(DISTINCT customer_id) AS cohort_size,
SUM(net_revenue) AS net_revenue_total,
ROUND(1.0*SUM(net_revenue)/NULLIF(COUNT(DISTINCT customer_id),0),2) AS ltv_per_customer
FROM bucket
GROUP BY cohort_month,day_bucket
ORDER BY cohort_month,day_bucket;





