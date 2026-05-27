---SQL script to set up the database for the Choco Delight case study
CREATE DATABASE choco_delight_db;

--- Connect to the newly created database
\c choco_delight_db;

--- Create schemas for raw, operational and analytical data
CREATE SCHEMA raw_data;
CREATE SCHEMA operationals;
CREATE SCHEMA analytics;


--- psql -U postgres -d choco_delight_db (connect to the database and run the above SQL commands)
--- psql -U postgres -d choco_delight_db -f sql\raw_schema.sql (run the SQL script directly from the command line)
