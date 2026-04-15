CREATE SCHEMA IF NOT EXISTS analytics;


DROP TABLE IF EXISTS analytics.fact_sales;
DROP TABLE IF EXISTS analytics.dim_calendar;
DROP TABLE IF EXISTS analytics.dim_customers; 
DROP TABLE IF EXISTS analytics.dim_products;
DROP TABLE IF EXISTS analytics.dim_stores;

-- DIMENSION TABLES

CREATE TABLE IF NOT EXISTS analytics.dim_calendar (
    calendar_key BIGINT PRIMARY KEY,
    calendar_date VARCHAR(20) NOT NULL UNIQUE,
    quarter VARCHAR(10) NOT NULL,
    season VARCHAR(20) NOT NULL,
    day_type VARCHAR(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics.dim_customers (
    customer_key BIGINT PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL UNIQUE,
    age INT CHECK (age >= 0),
    gender VARCHAR(10),
    join_date VARCHAR(20),
    tenure_days INT CHECK (tenure_days >= 0),
    tenure_months VARCHAR(20),
    tenure_years VARCHAR(20),
    customer_segment VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS analytics.dim_products (
    product_key BIGINT PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL UNIQUE,
    product_name VARCHAR(100) NOT NULL,
    brand VARCHAR(50),
    category VARCHAR(50),
    brand_tier VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS analytics.dim_stores (
    store_key BIGINT PRIMARY KEY,
    store_id VARCHAR(50) NOT NULL UNIQUE,
    store_name VARCHAR(100) NOT NULL,
    country VARCHAR(100),
    city VARCHAR(100),
    store_type VARCHAR(50),
    region VARCHAR(50),
    region_tier VARCHAR(20)
);

-- FACT TABLE

CREATE TABLE IF NOT EXISTS analytics.fact_sales (
    order_id VARCHAR(50) PRIMARY KEY,

    calendar_key BIGINT NOT NULL,
    product_key BIGINT NOT NULL,
    store_key BIGINT NOT NULL,
    customer_key BIGINT NOT NULL,

    quantity INT NOT NULL CHECK (quantity >= 0),
    unit_price FLOAT NOT NULL CHECK (unit_price >= 0),
    discount FLOAT CHECK (discount >= 0),
    revenue FLOAT CHECK (revenue >= 0),
    cost FLOAT CHECK (cost >= 0),
    profit FLOAT,
    revenue_bucket VARCHAR(20),
    profit_margin FLOAT,
    margin_bucket VARCHAR(20),
    outlier_flag VARCHAR(20),
    time_of_day VARCHAR(20),

    FOREIGN KEY (calendar_key) REFERENCES analytics.dim_calendar(calendar_key) ON DELETE RESTRICT,
    FOREIGN KEY (product_key) REFERENCES analytics.dim_products(product_key) ON DELETE RESTRICT,
    FOREIGN KEY (store_key) REFERENCES analytics.dim_stores(store_key) ON DELETE RESTRICT,
    FOREIGN KEY (customer_key) REFERENCES analytics.dim_customers(customer_key) ON DELETE RESTRICT
);

-- INDEXES FOR DIMENSIONS
-- Natural key lookups (used during ETL surrogate key assignment)
CREATE INDEX IF NOT EXISTS idx_dim_calendar_calendar_date
    ON analytics.dim_calendar (calendar_date);

CREATE INDEX IF NOT EXISTS idx_dim_customers_customer_id
    ON analytics.dim_customers (customer_id);

CREATE INDEX IF NOT EXISTS idx_dim_products_product_id
    ON analytics.dim_products (product_id);

CREATE INDEX IF NOT EXISTS idx_dim_stores_store_id
    ON analytics.dim_stores (store_id);

-- INDEXES FOR FACT TABLE

-- Foreign key indexes (critical for joins)
CREATE INDEX IF NOT EXISTS idx_fact_sales_calendar_key
    ON analytics.fact_sales (calendar_key);

CREATE INDEX IF NOT EXISTS idx_fact_sales_product_key
    ON analytics.fact_sales (product_key);

CREATE INDEX IF NOT EXISTS idx_fact_sales_store_key
    ON analytics.fact_sales (store_key);

CREATE INDEX IF NOT EXISTS idx_fact_sales_customer_key
    ON analytics.fact_sales (customer_key);

-- Analytical filter columns
CREATE INDEX IF NOT EXISTS idx_fact_sales_revenue_bucket
    ON analytics.fact_sales (revenue_bucket);

CREATE INDEX IF NOT EXISTS idx_fact_sales_margin_bucket
    ON analytics.fact_sales (margin_bucket);

CREATE INDEX IF NOT EXISTS idx_fact_sales_time_of_day
    ON analytics.fact_sales (time_of_day);

COMMIT;
