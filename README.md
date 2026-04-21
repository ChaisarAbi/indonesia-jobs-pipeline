# 🇮🇩 Indonesia Data Jobs Intelligence Pipeline

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/release/python-3120/)
[![Airflow 2.10.0](https://img.shields.io/badge/Airflow-2.10.0-red.svg)](https://airflow.apache.org/)
[![OpenSearch 3.6.0](https://img.shields.io/badge/OpenSearch-3.6.0-green.svg)](https://opensearch.org/)

A production-grade ETL pipeline that collects, processes, and visualizes the Indonesian Data & Analytics job market using **Apache Airflow**, **OpenSearch**, and **Docker**.

![Dashboard]
<img width="1919" height="949" alt="image" src="https://github.com/user-attachments/assets/437ed75f-a7e3-48cf-8617-830555bb4c3a" />


## 🏗️ Architecture

```mermaid
graph TD
    A[Kaggle: JobStreet Jobs] -->|load_data_jobs| D{Transform & Merge}
    B[Kaggle: JobStreet Salary] -->|load_salary_data| D
    C[Remotive API] -->|fetch_remotive| D
    
    D -->|Deduplication| E[Load to OpenSearch]
    D -->|Salary Parsing| E
    D -->|Skills Extraction| E
    
    E --> F[OpenSearch Dashboards]
````

## 📊 Key Insights (2024-2025)

From our analysis of **563+ job postings** in the Indonesian market:

  - **Top Demanded Skills**: 🛠️ **Excel** (212), **SQL** (159), **Power BI** (97).
  - **Highest Paying Region**: 💰 **Kalimantan Selatan** (\~Rp 11jt/month avg).
  - **Market Trend**: Mid-Level positions dominate the current landscape.
  - **Geographic Hotspot**: **Jakarta** remains the primary hub for data roles.

## 🛠️ Tech Stack

| Component | Technology |
| :--- | :--- |
| **Orchestration** | Apache Airflow 2.10.0 |
| **Storage & Search** | OpenSearch 3.6.0 |
| **Visualization** | OpenSearch Dashboards |
| **Containerization** | Docker + Docker Compose |
| **Data Processing** | Python 3.12 (Pandas) |

## 📁 Project Structure

```text
indonesia-jobs-pipeline/
├── airflow/
│   ├── dags/
│   │   └── indonesia_jobs_pipeline.py   # Main ETL DAG
│   └── data/                            # Source CSV files
├── docker-compose-airflow.yml            # Airflow services
├── docker-compose-opensearch.yml         # OpenSearch services
├── Dockerfile                            # Custom Airflow image
└── README.md
```

## 🚀 Getting Started

### Prerequisites

  - Docker & Docker Compose
  - 4GB RAM minimum allocated to Docker
  - Kaggle Datasets (see Data Sources)

### 1\. Installation

```bash
# Clone the repository
git clone [https://github.com/chaisarabi/indonesia-jobs-pipeline.git](https://github.com/chaisarabi/indonesia-jobs-pipeline.git)
cd indonesia-jobs-pipeline

# Create shared Docker network
docker network create aventra-network
```

### 2\. Start Services

```bash
# Spin up OpenSearch & Dashboards
docker compose -f docker-compose-opensearch.yml up -d

# Spin up Airflow
docker compose -f docker-compose-airflow.yml up -d
```

### 3\. Data Setup

Place the following CSV files in `airflow/data/`:

  - `jobstreet_data_jobs.csv` - [jobstreet_data_jobs.csv](https://github.com/user-attachments/files/26927505/jobstreet_data_jobs.csv)

  - `jobstreet_salary.csv` [jobstreet_salary.csv](https://github.com/user-attachments/files/26927512/jobstreet_salary.csv)


### 4\. Trigger Pipeline

Access Airflow at `http://localhost:8080` or trigger via CLI:

```bash
docker exec airflow-scheduler airflow dags trigger indonesia_jobs_pipeline
```

## 📈 Pipeline Design

  - **Parallel Extraction**: Tasks run concurrently to optimize performance.
  - **Idempotency**: Uses `job_id` as document ID to prevent duplicate data on retries.
  - **Bulk Loading**: Efficiently indexes data in chunks of 500 documents.
  - **Validation**: Integrated quality gates to ensure clean data flow.

## 📦 Data Sources

| Source | Description |
| :--- | :--- |
| **Kaggle JobStreet** | Primary dataset for Indonesia Data Jobs (Aug-Sep 2025). |
| **Remotive API** | Live tech job feed for remote positions. |

## 👤 Author

**Chaisar Abi Prasetyo**

  - **Portfolio**: [portfolio.aventra.my.id](https://www.google.com/search?q=https://portfolio.aventra.my.id)
  - **GitHub**: [@chaisarabi](https://github.com/chaisarabi)

## 📝 License

This project is licensed under the [MIT License](https://www.google.com/search?q=LICENSE).

```
```
