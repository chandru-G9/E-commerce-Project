🛒 End-to-End E-commerce Data Pipeline | Azure Cloud
This project demonstrates a real-world enterprise-scale data engineering solution for processing and analyzing e-commerce data (~25M records). The pipeline is designed using Azure Cloud services with a Medallion Architecture approach, enabling scalable, secure, and optimized data analytics.

🚀 Architecture Overview
Tech Stack:
Azure Data Factory (ADF): Orchestration & data ingestion
Azure Data Lake Storage Gen2 (ADLS): Centralized data lake (Raw → Processed → Curated)
Azure Databricks (PySpark): Data transformation using Medallion architecture (Bronze → Silver → Gold)
Azure Synapse Analytics: Data warehouse for modeling & analytical queries
Power BI: Visualization & business insights

Architecture Flow:
Ingestion Layer (ADF):
Extracts multiple tables from on-premise SQL Database
Loads into ADLS (Bronze layer) with validation
Transformation Layer (Databricks - PySpark):
Cleanses & enriches raw data (Bronze → Silver)
Aggregates & applies business logic (Silver → Gold)
Storage & Modeling Layer (Synapse):
Designed Star Schema for reporting
Implemented SCD Type 1 for dimension updates

Visualization Layer (Power BI):
Customer behavior analysis
Time-based sales trends
Category performance metrics

Medallion Architecture
Bronze (Raw): Stores ingested unprocessed data as-is
Silver (Validated): Cleaned, structured, and joined data
Gold (Curated): Aggregated data for business reporting and dashboards

📊 Power BI Dashboards
Key insights delivered:
Customer purchase patterns
Sales performance across time ranges

Key Features
✔️ Processed ~25M records end-to-end
✔️ Reusable & parameterized ADF pipelines for multiple sources
✔️ Scalable PySpark transformations on Databricks
✔️ Optimized data model (Star Schema + SCD Type 1) for analytics
✔️ Business-ready dashboards in Power BI

📈 Future Improvements
Implement SCD Type 2 for historical tracking
Automate CI/CD with Azure DevOps


📌 How to Use
Clone this repo:
git clone https://github.com/chandru-G9/E-commerce-Project.git
Import ADF pipelines into your Azure Data Factory instance
Upload transformation notebooks into Databricks workspace
Deploy schema scripts to Synapse Analytics
Connect Power BI to Synapse / ADLS for reporting



🌐 LinkedIn
 | GitHub
