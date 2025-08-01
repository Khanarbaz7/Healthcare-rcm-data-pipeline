# 🏥 Healthcare Revenue Cycle Management (RCM) - Data Engineering Project

Welcome to the Healthcare Revenue Cycle Management (RCM) Data Engineering Challenge!  
This project simulates a real-world data engineering role in a healthcare analytics company. It focuses on building a robust ETL pipeline to integrate, clean, transform, and analyze healthcare financial data.

## 📌 Objective

Design and implement a complete data pipeline that:
- Integrates patient, transaction, and claim data from multiple hospitals
- Tracks historical changes using Slowly Changing Dimensions (SCD Type 2)
- Cleans and validates data for analytics
- Loads data into BigQuery for visualization and analysis

## 🧰 Tech Stack

- **Languages:** Python, SQL  
- **Databases:** MySQL (local), BigQuery (cloud)  
- **Libraries:** pandas, SQLAlchemy, google-cloud-bigquery, python-dotenv  
- **Tools:** Git, GitHub, Google Cloud Platform

## 📂 Project Structure
healthcare-rcm-data-pipeline/
│
├── data/ # Sample data and CSVs
├── scripts/ # Python scripts for each phase
├── sql/ # SQL scripts for schema and setup
├── config/ # Config files like database connections
├── .env.example # Environment variable template
├── requirements.txt
└── README.md

## ✅ Features in process

- [ ] MySQL and BigQuery environment setup  
- [ ] Data extraction from hospital databases and claims CSVs  
- [ ] Data cleaning and transformation  
- [ ] Star schema modeling (fact and dimension tables)  
- [ ] SCD Type 2 implementation for patient dimension  
- [ ] Data loading to BigQuery  
- [ ] Analytics queries and reports  

## 🔐 Secrets Management

All sensitive credentials (API keys, passwords, etc.) are stored in a `.env` file and **not committed** to the repository. Refer to `.env.example` for required variables.

## 📊 Key Metrics (to be implemented)

- Total Revenue, Denial Rate, Patient Volume, Collection Rate
- Patient Lifetime Value, Insurance Mix, Procedure Profitability

## 📁 Resources

- [Google Cloud BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [SQLAlchemy Docs](https://docs.sqlalchemy.org/)
- [python-dotenv Docs](https://pypi.org/project/python-dotenv/)

---




