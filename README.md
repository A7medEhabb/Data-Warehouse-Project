# 📦 Data Warehouse Project

An **end-to-end Data Warehouse project** built using SQL Server, showcasing ETL processes, star schema modeling, data loading, and analytical queries. This project is designed for educational and professional practice in Business Intelligence and Data Warehousing.


## 📌 Table of Contents

1. [Overview](#-overview)  
2. [Requirements](#-requirements)  
3. [Folder Details](#-folder-details)  
6. [Usage & ETL](#-usage--etl)  
7. [Sample Reports & Queries](#-sample-reports--queries)  
8. [Resources](#-resources)  
9. [License](#-license)  
10. [Contact](#-contact)


## 🧠 Overview

This project simulates a full data warehousing process including:

- **Data extraction** from multiple sources (CSV files).
- **Data transformation and cleaning** using SQL scripts.
- **Data loading** into a star-schema warehouse.
- Analytical SQL queries and visual models (ERD).
- Documentation for each step of the process.

The aim is to demonstrate data modeling, warehousing, and analytics skills using a real-world approach.
---

## ⚙️ Requirements

- SQL Server 2017 or later (Express or Developer edition is fine)
- SQL Server Management Studio (SSMS)
- Understanding of the ETL process
- Git for cloning the repository
- Basic SQL knowledge


## 📂 Folder Details

| Folder        | Description                                                                 |
|---------------|-----------------------------------------------------------------------------|
| `Datasets/`   | Contains raw data files in CSV format (e.g., `crm_cust_info.csv`. |
| `Scripts/`    | SQL scripts for creating schema, performing ETL, and writing queries.       |
|               | - `ddl.sql`: Defines fact and dimension tables.                  |
|               | - `bronze_load_data.sql`: Loads CSV data into SQL Server.                     |
|               |              
| `docs/`       | Documentation folder with supporting visuals and explanations.              |
|               | - `Data Integration.png`: Entity-Relationship Diagram of the data model.                |
|               | - `Data flow.png`: Data flow diagram.                            |
|               |     |
| `README.md`   | This file.                                                                 |
| `LICENSE`     | MIT License for project usage.                                              |



