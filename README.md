# 🇮🇩 Indonesia Data Jobs Intelligence Pipeline

A production-grade ETL pipeline that collects, processes, and visualizes
the Indonesian Data & Analytics job market using Apache Airflow, OpenSearch,
and Docker.

![Dashboard](docs/dashboard.png)

## 🏗️ Architecture

\`\`\`
[Kaggle: JobStreet Data Jobs]     [Kaggle: JobStreet Salary]     [Remotive API]
     555 job postings                  32,976 salary records        Remote jobs
           ↓                                  ↓                         ↓
      load_data_jobs               load_salary_data              fetch_remotive
           ↓                                  ↓                         ↓
           └──────────────────────────────────┴─────────────────────────┘
                                              ↓
                                    transform_and_merge
                                    - Deduplication
                                    - Salary parsing
                                    - Skills extraction
                                              ↓
                                     load_to_opensearch
                                     (Bulk API, idempotent)
                                              ↓
                                    OpenSearch Dashboards
                                    - Top 10 Skills
                                    - Avg Salary by Province
                                    - Jobs by Level
                                    - Jobs by City
\`\`\`

## 📊 Key Insights

From 563 job postings in the Indonesian Data & Analytics market (2024-2025):

- **#1 Most demanded skill**: Excel (212 postings)
- **#2**: SQL (159 postings)
- **#3**: Power BI (97 postings)
- **Highest paying province**: Kalimantan Selatan (~Rp 11 juta/month avg)
- **Dominant level**: Mid Level positions dominate the market
- **Job hotspot**: Jakarta accounts for majority of postings

## 🛠️ Tech Stack

| Component | Technology |
|---|---|
| Orchestration | Apache Airflow 2.10.0 |
| Storage & Search | OpenSearch 3.6.0 |
| Visualization | OpenSearch Dashboards |
| Containerization | Docker + Docker Compose |
| Language | Python 3.12 |
| Data Processing | Pandas |

## 📁 Project Structure

\`\`\`
indonesia-jobs-pipeline/
├── airflow/
│   └── dags/
│       └── indonesia_jobs_pipeline.py   # Main ETL DAG
├── docker-compose-airflow.yml           # Airflow services
├── docker-compose-opensearch.yml        # OpenSearch services
├── Dockerfile                           # Custom Airflow image
├── .gitignore
└── README.md
\`\`\`

## 🚀 How to Run

### Prerequisites
- Docker & Docker Compose
- 4GB RAM minimum
- Data files from Kaggle (see Data Sources)

### 1. Clone the repository
\`\`\`bash
git clone https://github.com/chaisarabi/indonesia-jobs-pipeline.git
cd indonesia-jobs-pipeline
\`\`\`

### 2. Create shared Docker network
\`\`\`bash
docker network create aventra-network
\`\`\`

### 3. Start OpenSearch
\`\`\`bash
docker compose -f docker-compose-opensearch.yml up -d
\`\`\`

### 4. Download data files
Place the following CSV files in \`airflow/data/\`:
- \`jobstreet_data_jobs.csv\` — [Indonesia Data Jobs Dataset](https://www.kaggle.com/datasets)
- \`jobstreet_salary.csv\` — [JobStreet Salary Dataset](https://www.kaggle.com/datasets)

### 5. Start Airflow
\`\`\`bash
docker compose -f docker-compose-airflow.yml up -d
\`\`\`

### 6. Trigger the pipeline
Open Airflow at \`http://localhost:8080\` and trigger \`indonesia_jobs_pipeline\`

Or via CLI:
\`\`\`bash
docker exec airflow-scheduler airflow dags trigger indonesia_jobs_pipeline
\`\`\`

### 7. View Dashboard
Open OpenSearch Dashboards at \`http://localhost:5601\`

## 📈 Pipeline Details

### DAG Structure
\`\`\`
load_data_jobs ──┐
                  ├──► transform_and_merge ──► load_to_opensearch
load_salary_data─┤
                  │
fetch_remotive ───┘
\`\`\`

### Key Design Decisions
- **Parallel extraction**: All 3 source tasks run simultaneously
- **Idempotent loading**: Uses \`job_id\` as document ID — safe to re-run
- **Bulk indexing**: Chunks of 500 documents for efficiency
- **Data quality gate**: Pipeline fails loudly if zero records produced

## 📦 Data Sources

| Source | Records | Description |
|---|---|---|
| Kaggle JobStreet Data Jobs | 555 | Data & Analytics jobs Aug-Sep 2025 |
| Kaggle JobStreet Salary | 32,976 | Salary data across all job categories |
| Remotive API | ~100 | Live remote tech jobs |

## 👤 Author

**Chaisar Abi Prasetyo**
- GitHub: [@chaisarabi](https://github.com/chaisarabi)

## 📝 License

MIT License
