# 🏎️ F1 Data Lakehouse | End-to-End Data Engineering Project

[![Python 3.11](https://img.shields.io/badge/Python-3.11-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Databricks](https://img.shields.io/badge/Databricks-Workflows-FF3621?style=flat&logo=databricks&logoColor=white)](https://databricks.com/)
[![Azure](https://img.shields.io/badge/Microsoft-Azure%20Data%20Lake-0089D6?style=flat&logo=microsoftazure&logoColor=white)](https://azure.microsoft.com/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-ACID-00ADD8?style=flat&logo=deltalake&logoColor=white)](https://delta.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

An end-to-end Data Engineering project simulating an enterprise-grade Retail/SaaS Lakehouse architecture on Microsoft Azure and Databricks, utilizing PySpark, Delta Lake, and Dimensional Modeling.

---
## 💼 Business Value & Product Vision

This project transcends simple sports analytics by mapping Formula 1 domain entities to commercial SaaS and Retail Key Performance Indicators (KPIs). It addresses core data challenges: scalability, schema enforcement, data quality, and high-performance Business Intelligence.

| F1 Domain Concept | Corporate Equivalent (SaaS / Retail) | Business Problem Solved |
| :--- | :--- | :--- |
| **Race Results** | Fact Sales / Transactions | Revenue monitoring and transaction volume tracking. |
| **Drivers & Constructors** | Customers & Brands | Lifetime Value (LTV) and Market Share analysis. |
| **DNFs (Did Not Finish)** | Customer Churn | Identifying churn rates and product reliability. |
| **Fastest Lap** | Peak Usage / Traffic Spikes | Performance analysis and maximum engagement tracking. |
---
## 🏗️ Solution Architecture (Medallion Architecture)

The pipeline implements the **Medallion Architecture** (Bronze, Silver, Gold) to guarantee data quality, ACID transactions, and governance at each stage:

1. **Ingestion Layer (Bronze):** 🥉
   * **Source:** External REST APIs (Jolpica/Ergast F1).
   * **Process:** Raw ingestion of JSON payloads using Python (`requests`) and PySpark.
   * **Storage:** Append-only raw storage on Azure Data Lake Gen2.

2. **Transformation Layer (Silver):** 🥈
   * **Data Cleaning:** Handling null values, strict typing (`Schema Enforcement`), and timestamp normalization.
   * **Deduplication & Partitioning:** Removing duplicate records and optimizing distributed scans using `.partitionBy('season')`.
   * **Format:** Delta Lake format ensuring ACID compliance and time travel capabilities.

3. **Business Modeling Layer (Gold):** 🥇
   * **Dimensional Modeling:** Designing a **Star Schema** optimized for BI and downstream analytics.
     * *Dimensions:* `dim_drivers`, `dim_constructors`, `dim_races`.
     * *Fact Table:* `fact_results` (containing surrogate keys and pre-aggregated metrics).
----
## ⚙️ Orchestration & CI/CD Pipeline

The pipeline is fully automated via **Databricks Workflows (Databricks Jobs)**, simulating a production-grade environment with dependency management and error handling:

![Pipeline Visual Databricks](src/docs/pipeline-workflow.png)
_(Execution Flow: Ingestion ➔ Transformation ➔ Modeling)_

- **Job Name:** `f1_analytics_orchestrator`
- **Features:** Automated API retry logic, dynamic parameters (season/year), and failure alerting notifications.
---
## 📊 Analytics & Proof of Value

To demonstrate commercial value, an interactive dashboard was built in Databricks SQL:

![Dashboard Analytics](src/docs/Dashboard_analytics.png)
* **Market Dominance:** Team-level point distribution and Market Share analysis.
* **Individual Performance:** Driver standings and seasonal evolution rankings.
* **Reliability KPIs:** Churn rate equivalents (DNF analysis) measuring technical health.
---
## 🛠️ Technology Stack

* **Cloud Provider:** Microsoft Azure (Data Lake Gen2)
* **Distributed Compute:** Apache Spark (Databricks)
* **Storage & Protocol:** Delta Lake (ACID Transactions)
* **Languages:** Python (PySpark) for ETL, SQL for Analytics
* **Orchestration:** Databricks Workflows / Jobs
---
## 🚀 How to Execute

### 1. Clone the Repository
    ```bash
    git clone [https://github.com/ana-lapas/f1-lakehouse.git](https://github.com/ana-lapas/f1-lakehouse.git)
    ```
2.  **Setup no Databricks:**
    - Import the notebooks from the `/notebooks` directory into your Databricks workspace.
    - Configure your Azure credentials in `1_Ingestao_Bronze`.
3.  **Run the Pipeline**
    - Create a Databricks Job pointing to the three notebooks in sequential order (Bronze ➔ Silver ➔ Gold).
    - Trigger the job execution and inspect the resulting tables on the Data Lake.
---
## 📫 Author
**Ana Paula Leão**

- [LinkedIn](https://www.linkedin.com/in/ana-paula-leao/)
- [Portfólio](https://github.com/ana-lapas)
