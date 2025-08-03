## Healthcare Revenue Cycle Management (RCM) – Final Data Engineering Project

Welcome to the completed **Healthcare Revenue Cycle Management (RCM)** Data Engineering Project!  
This project replicates a real-world scenario in a healthcare analytics organization, where we built a robust and scalable data pipeline to transform raw healthcare data into actionable business insights.

---

## Objective

Design and implement a complete end-to-end data pipeline that:

- Consolidates patient, claims, and transaction data from multiple hospital sources (MySQL, CSV)
- Applies **SCD Type 2** for historical patient tracking
- Performs rigorous **data cleaning, validation, and enrichment**
- Loads structured data into **BigQuery** for analytics
- Delivers **key RCM metrics** via dashboards

---

## Tech Stack

- **Languages**: Python, SQL  
- **Data Sources**: MySQL (Hospital A & B), CSV (claims)  
- **Data Warehouse**: Google BigQuery  
- **Libraries**: `pandas`, `SQLAlchemy`, `google-cloud-bigquery`, `python-dotenv`  
- **Tools**: Git, GitHub, Google Cloud Platform, Looker Studio (for dashboards)  
- **Logging**: Custom ETL logging framework

---

  ## Project Structure
  healthcare-rcm-data-pipeline/
├── data/ # Raw and extracted data
├── cleaned/ # Cleaned CSVs ready for BigQuery
├── scripts/ # Python ETL scripts
├── sql/ # BigQuery schema and query files
├── dashboards/ # Screenshots and links to Looker dashboards
├── config/ # Database and env configuration
├── .env.example # Template for credentials
├── requirements.txt
└── README.md
 
---

##  Completed Features

- ✔️ Environment setup for MySQL and BigQuery  
- ✔️ Extraction from hospital databases & claim files  
- ✔️ Cleaning & deduplication of patient records  
- ✔️ SCD Type 2 implementation for `dim_patients`  
- ✔️ Surrogate key generation across fact/dim tables  
- ✔️ Data validation (foreign key, row count, data types)  
- ✔️ Automated ETL logging to BigQuery  
- ✔️ Business KPIs & analytics SQL queries  
- ✔️ Final dashboards in Looker Studio  

---

##  Key Metrics Delivered

- **Revenue Metrics**: Total revenue, monthly trends, by hospital  
- **Claims Metrics**: Approval/denial rates, average processing time  
- **Patient Metrics**: Volume, insurance mix, demographics  
- **Operational KPIs**: Days in A/R, collection rate, write-off amount  
- **Advanced Analytics**: Patient lifetime value, procedure profitability  

---

##  Secrets Management

All sensitive credentials (e.g., DB passwords, API keys) are stored in a `.env` file and excluded from Git.  
Refer to `.env.example` to set up your environment.

---

##  Dashboards

The final analytics dashboards were built using **Looker Studio** and include:

- RCM KPIs Overview  
- Data Quality Report  
- SCD Version Distribution  
- ETL Job Monitoring

---

##  Outcome & Success Criteria

-  Processed over **1 million** records with >99% data quality  
-  Implemented SCD2 with versioning & current flags  
-  ETL performance tracked & validated through logs  
-  Delivered insights matching real RCM business goals  
-  Project ready for production deployment  

---

Feel free to explore the scripts, outputs, and dashboards.  
For questions or deployment assistance, please contact.



