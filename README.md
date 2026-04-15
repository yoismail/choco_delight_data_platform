# 🍫 About the ChocoDelight Data Platform

The ChocoDelight Data Platform is an end‑to‑end data engineering project designed to transform a flat, messy chocolate‑sales dataset into a **high‑performance, analytics‑ready data system**. This project demonstrates my ability to architect scalable pipelines, enforce data integrity, and deliver business‑focused insights through SQL and Python‑based engineering.

---

## 🎯 Project Purpose

ChocoDelight International is a fast‑growing premium chocolate company operating across multiple regions. While the business captures sales data effectively, the original dataset suffers from:

- Redundancy  
- Poor query performance  
- Limited analytical capability  
- Lack of relational structure  

This platform redesigns the dataset into a **normalized relational model**, builds a **modular ETL pipeline**, and delivers **insight‑driven analytical outputs** for business intelligence.

---

## 🧱 What This Project Demonstrates

This project showcases my approach to real‑world data engineering:

### **1. Database Optimization & Relational Modelling**
- Applied **Second Normal Form (2NF)**  
- Created **dimension and fact tables** (Customers, Products, Regions, Sales)  
- Added **PRIMARY KEY**, **FOREIGN KEY**, and **INDEX** constraints  
- Built a clean, scalable schema (`schema_final.sql`)

### **2. ETL Pipeline Engineering**
- Designed modular Python scripts for:
  - Extracting raw CSV data  
  - Cleaning and validating datasets  
  - Transforming data into analytical structures  
  - Loading into PostgreSQL  
- Implemented structured logging and reproducible workflows  
- Automated schema creation using `load_raw_schema.py`

### **3. Data Cleaning & Feature Engineering**
- Standardized formats and handled missing values  
- Removed duplicates and enforced data quality  
- Engineered business‑friendly features:
  - Revenue calculations  
  - Time‑based attributes (month, quarter, season)  
  - Customer and sales segmentation  

### **4. Business Analytics & SQL Views**
Delivered insight‑ready SQL views for:

- Revenue by product  
- Revenue by region  
- Monthly sales trends  
- Customer segmentation buckets  

These views support dashboards, reporting, and strategic decision‑making.

---

## 🏗️ Data Model Overview

The final relational model includes:

- **Calendar** (Dimension)  
- **Customers** (Dimension)  
- **Products** (Dimension)  
- **Stores** (Dimension)  
- **Sales** (Fact Table)  

This structure improves performance, reduces redundancy, and enables flexible analytics.

---

## 🛠️ Tech Stack

- **Python** (Pandas, SQLAlchemy)  
- **PostgreSQL**  
- **ETL Pipelines**  
- **SQL Views & Analytics**  
- **Logging & Modular Architecture**  

---

## 📌 Why This Project Matters

This project reflects how I approach data engineering in real environments:

- Build systems that scale  
- Enforce data integrity  
- Design for analytics from day one  
- Keep pipelines modular, readable, and production‑ready  
- Deliver insights that drive business outcomes  

---

## 📈 Current Status

The raw layer, schema loader, and foundational ETL structure are complete.  
Next phases include advanced transformations, segmentation logic, and final analytics views.

---

## 🧠 My Philosophy

> “A data platform is only valuable when it is structured, reliable, and built for insight.”

---

If you find this project helpful or want to collaborate on data engineering work, feel free to connect.
