CREATE SCHEMA IF NOT EXISTS operationals;

DROP TABLE IF EXISTS cleaned_calendar;
DROP TABLE IF EXISTS cleaned_customers; 
DROP TABLE IF EXISTS cleaned_products;
DROP TABLE IF EXISTS cleaned_sales; 
DROP TABLE IF EXISTS cleaned_stores;

-- This table will contain cleaned calendar data with proper data types and formatting
CREATE TABLE IF NOT EXISTS operationals.cleaned_calendar (
    calendar_key INT,
    calendar_date DATE,
    year INT,
    month INT,
    week INT,
    day_of_week INT,
    quarter VARCHAR(10),
    season VARCHAR(20),
    day_type VARCHAR(20),
    calendar_date_formatted VARCHAR(50)
    
);


-- This table will contain cleaned customer data with proper data types and formatting
CREATE TABLE IF NOT EXISTS operationals.cleaned_customers (
    customer_key INT,
    customer_id VARCHAR(50),
    age INT,
    gender VARCHAR(10),
    loyalty_member INT,
    join_date DATE,
    tenure_days INT,
    tenure_months VARCHAR(20),
    tenure_years VARCHAR(20),
    customer_segment VARCHAR(20)
);  

-- This table will contain cleaned product data with proper data types and formatting
CREATE TABLE IF NOT EXISTS operationals.cleaned_products (
    product_key INT,
    product_id VARCHAR(50),
    product_name VARCHAR(100),
    brand VARCHAR(50),
    category VARCHAR(50),
    cocoa_percent FLOAT,
    weight_g FLOAT,
    brand_tier VARCHAR(20)
);

-- This table will contain cleaned sales data with proper data types and formatting
CREATE TABLE IF NOT EXISTS operationals.cleaned_sales (
    order_id VARCHAR(50),
    order_date DATE,
    product_id VARCHAR(50),
    store_id VARCHAR(50),
    customer_id VARCHAR(50),
    quantity INT,
    unit_price FLOAT,
    discount FLOAT,
    revenue FLOAT,
    cost FLOAT,
    profit FLOAT,
    revenue_bucket VARCHAR(20),
    profit_margin FLOAT,
    margin_bucket VARCHAR(20),
    outlier_flag VARCHAR(20),
    time_of_day VARCHAR(20)
);


-- This table will contain cleaned store data with proper data types and formatting
CREATE TABLE IF NOT EXISTS operationals.cleaned_stores (
    store_key INT,
    store_id VARCHAR(50),
    store_name VARCHAR(100),
    city VARCHAR(100),
    country VARCHAR(100),
    store_type VARCHAR(50),
    region VARCHAR(20),
    region_tier VARCHAR(20)
);

COMMIT;