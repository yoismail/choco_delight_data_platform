CREATE SCHEMA IF NOT EXISTS raw_data;

-- Drop existing tables
DROP TABLE IF EXISTS raw_data.sales;
DROP TABLE IF EXISTS raw_data.calendar;
DROP TABLE IF EXISTS raw_data.customers;
DROP TABLE IF EXISTS raw_data.products;
DROP TABLE IF EXISTS raw_data.stores;   


CREATE TABLE IF NOT EXISTS raw_data.sales (
    order_id VARCHAR(50),
    order_date VARCHAR(50),
    product_id VARCHAR(50),
    store_id VARCHAR(50),
    customer_id VARCHAR(50),
    quantity INT,
    unit_price FLOAT,
    discount FLOAT,
    revenue FLOAT,
    cost FLOAT,
    profit FLOAT
 
);

CREATE TABLE IF NOT EXISTS raw_data.calendar (
    date INT,
    year INT,
    month INT,
    week INT,
    day_of_week INT
);


CREATE TABLE IF NOT EXISTS raw_data.customers (
    customer_id VARCHAR(50),
    age INT,
    gender VARCHAR(10),
    loyalty_member INT,
    join_date VARCHAR(50)
);


CREATE TABLE IF NOT EXISTS raw_data.products (
    product_id VARCHAR(50),
    product_name VARCHAR(100),
    brand VARCHAR(50),
    category VARCHAR(50),
    cocoa_percent INT,
    weight_g INT
);


CREATE TABLE IF NOT EXISTS raw_data.stores (
    store_id VARCHAR(50),
    store_name VARCHAR(100),
    city VARCHAR(100),
    country VARCHAR(100),
    store_type VARCHAR(50)
);


COMMIT;



