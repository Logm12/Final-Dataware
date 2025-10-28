# End-to-End Data Warehouse for Sales Analytics

This project implements a complete, end-to-end ELT (Extract, Load, Transform) pipeline. It ingests data from a transactional (OLTP) database, processes it, and loads it into a dimensional (OLAP) data warehouse. The final data is then served to a Business Intelligence tool for analysis.

**Data Source:** [DataCo Smart Supply Chain for Big Data Analysis (Kaggle)](https://www.kaggle.com/datasets/shashwatwork/dataco-smart-supply-chain-for-big-data-analysis)
**Project Goal:** To build an automated and reliable data pipeline that transforms raw transactional data into an analysis-ready Star Schema, visualized through an interactive dashboard.

## 🚀 Tech Stack

* **PostgreSQL** 🐘: Hosts both the source `sales_oltp` and the destination `dwh` databases.
* **Docker & Docker Compose** 🐳: Containerizes and orchestrates the entire stack (DB, Airflow, BI).
* **Apache Airflow** ✈️: Orchestrates the ELT pipeline, manages task dependencies, and schedules runs.
* **SQL** ✍️: The core logic for all Transform operations (from staging to dim/fact tables).
* **Python (Pandas)** 🐍: Used in Airflow for Extract & Load (ingestion) scripts.
* **Tableau** 📊: The BI tool used to connect to the DWH and build interactive dashboards.

## 🏗️ System Architecture

The project follows a modern data stack architecture.

* **OLTP Database**: A PostgreSQL database (`sales_oltp`) simulates a live production system for order management.
* **Airflow Pipeline**:
    * **Ingest DAG**: An Airflow DAG (`ingest_dag`) extracts data from the OLTP tables and loads it 1:1 into a `staging` schema within the Data Warehouse.
    * **Transform DAG**: A second DAG (`transform_dag`) runs after ingestion. It executes a series of SQL scripts to clean, model, and transform the staging data into the final Star Schema (dimension and fact tables).
* **Data Warehouse (DWH)**: A separate PostgreSQL database (`dwh`) containing the modeled data. It includes:
    * `staging` schema: Raw, untouched copies of the source data.
    * `production` schema: The final, analysis-ready `dim_` and `fact_` tables.
* **Business Intelligence (BI)**: Metabase connects directly to the `dwh` to create visualizations, build dashboards, and enable data exploration.

## 🗂️ Database Schema Design

### 1. OLTP Schema (sales\_oltp)

The source schema is normalized (3NF) to optimize for write operations (INSERT, UPDATE, DELETE) and reduce data redundancy.

### 2. DWH Star Schema (dwh)

The destination schema is a Star Schema, which is de-normalized to optimize for fast read and aggregation queries. This is the standard for analytical workloads.

* **Fact Table**:
    * `fact_sales`: One row per order item, containing all key metrics (sales, profit, quantity) and foreign keys to the dimension tables.
* **Dimension Tables**:
    * `dim_customer`: Describes the "who."
    * `dim_product`: Describes the "what."
    * `dim_date`: Describes the "when."

## ✨ Key Features

* **Fully Dockerized**: The entire environment is defined in `docker-compose.yml` for a one-command setup.
* **Automated ELT Pipeline**: Airflow DAGs manage the full data flow from source to destination.
* **Idempotent Transformations**: The SQL transformation scripts are idempotent, using `INSERT ... ON CONFLICT DO UPDATE` logic. This ensures that running the pipeline multiple times does not create duplicate data.
* **Data Modeling**: Implements a clean separation between raw `staging` data and the final production-grade star schema.
* **Interactive Dashboards**: Metabase provides rich, interactive dashboards with filters (dropdown, checkbox, text) and a responsive layout.
* **Row-Level Security (RLS)**: Demonstrates two methods for implementing RLS to restrict data access for different user groups (both via Postgres RLS policies and Metabase sandboxing).

## ⚙️ How to Run the Project

### Prerequisites

* Docker
* Docker Compose

### Installation

1.  Clone the repository:
    ```bash
    git clone [https://github.com/your-username/your-repo-name.git](https://github.com/your-username/your-repo-name.git)
    cd your-repo-name
    ```
2. Write .env file:
    ```bash
AIRFLOW_IMAGE_NAME=apache/airflow:3.1.0
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=.
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
_PIP_ADDITIONAL_REQUIREMENTS=apache-airflow-providers-postgres
    ```

4.  Start all services:
    ```bash
    docker-compose up -d --build
    ```
    This command will:
    * Build the custom Airflow image.
    * Start the Postgres, Airflow (webserver, scheduler), and Metabase containers.
    * Run the `init.sql` script inside the `init-scripts` folder to create the `sales_oltp` and `dwh` databases, their schemas, and load initial sample data.

    **Note**: It may take a few minutes for all services to be healthy, especially the Airflow webserver.

### Accessing Services

* **Apache Airflow (Orchestrator):**
    * **URL:** `http://localhost:8080`
    * **Username:** `airflow`
    * **Password:** `airflow`
    * **Action:** Once logged in, un-pause the `ingest_dag` and `transform_dag` and trigger a run.

* **Metabase (BI Tool):**
    * **URL:** `http://localhost:3000`
    * **Action:** On first load, create your admin account. Then, connect to the DWH.
    * **Connection Settings:**
        * Database type: `PostgreSQL`
        * Host: `postgres` (This is the service name from `docker-compose.yml`)
        * Port: `5432`
        * Database Name: `dwh`
        * Username: `admin`
        * Password: `admin`

* **PostgreSQL (Database):**
    * **Host:** `localhost`
    * **Port:** `5432`
    * **Username:** `admin`
    * **Password:** `admin`
    * **Action:** You can connect with a client like DBeaver or pgAdmin to inspect the `sales_oltp` and `dwh` databases.

## 📁 Project Structure

```text
.
├── docker-compose.yml      # Main Docker orchestration file
├── .env                    # Environment variables (DB passwords, etc.)
│
├── dags/                   # Airflow DAG definitions
│   ├── ingest_dag.py       # DAG for (E)xtract & (L)oad (OLTP -> Staging)
│   └── transform_dag.py    # DAG for (T)ransform (Staging -> Dim/Fact)
│
├── init-scripts/           # SQL scripts run by Postgres on initial startup
│   └── init.sql            # Creates schemas, tables, and inserts sample data
│
├── sql/                    # SQL transformation logic called by transform_dag
│   ├── transform_dim_customer.sql
│   ├── transform_dim_product.sql
│   ├── transform_dim_date.sql
│   └── transform_fact_sales.sql
│
└── README.md               # You are here!
