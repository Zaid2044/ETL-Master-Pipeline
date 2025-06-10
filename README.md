# ETL-Master: An Automated Data Pipeline

A robust and modular Python script that demonstrates a complete **Extract, Transform, Load (ETL)** pipeline. This project simulates a real-world data engineering scenario where data from disparate sources (a local CSV file and a live web API) is consolidated, cleaned, and loaded into a structured SQLite database for analysis.

---

## 🏗️ The Business Problem

In any data-driven organization, raw data is generated across multiple systems and in various formats. Sales data might exist in daily CSV exports from an e-commerce platform, while product inventory details are accessible via an internal API. This fragmented data is unusable for business intelligence, reporting, or machine learning until it is centralized and standardized. The core challenge is to build an automated and reliable pipeline to perform this consolidation.

## 💡 The Solution

ETL-Master is a Python-based solution that automates this entire process. It is designed with clear separation of concerns, with dedicated functions for each stage of the ETL workflow.

### The Pipeline's Workflow:

1.  **Extract:**
    *   Reads batch sales data from a local `online_sales.csv` file.
    *   Fetches live product data from the public `FakeStoreAPI`, simulating a secondary data source.

2.  **Transform:**
    *   Standardizes column names and data types from both sources to create a unified schema.
    *   Enriches the data by calculating new, valuable fields (e.g., `total_sale_value`).
    *   Merges the two datasets into a single, clean master pandas DataFrame, adding a `source` column to maintain data lineage.

3.  **Load:**
    *   Establishes a connection to a local SQLite database (`sales_data.db`).
    *   Loads the final, transformed DataFrame into a `master_sales` table, replacing any old data to ensure freshness. This structured database is now ready for querying by analysts or for use as a feature source for machine learning models.

---

## 🛠️ Technology Stack

*   **Core Language:** Python
*   **Data Manipulation:** Pandas, NumPy
*   **API Interaction:** Requests
*   **Database:** SQLite (via Python's built-in `sqlite3` library)

---

## 🏁 Getting Started

### 1. Clone the Repository
```bash
git clone https://github.com/Zaid2044/ETL-Master-Pipeline.git
cd ETL-Master-Pipeline


2. Set Up the Environment
This project requires a online_sales.csv file in the root directory. A sample is included in the repository.
# Create a virtual environment
python -m venv venv

# Activate it (Windows)
.\venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt


3. Run the ETL Pipeline
python etl_pipeline.py