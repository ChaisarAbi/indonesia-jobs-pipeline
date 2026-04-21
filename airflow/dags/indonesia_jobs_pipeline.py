from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import hashlib
import re
import os
from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk

# ── Config ──────────────────────────────────────────────────────
OPENSEARCH_HOST       = "aventra-opensearch"
OPENSEARCH_PORT       = 9200
INDEX_NAME            = "indonesia-jobs"
PATH_DATA_JOBS        = "/opt/airflow/data/jobstreet_data_jobs.csv"
PATH_SALARY           = "/opt/airflow/data/jobstreet_salary.csv"
REMOTIVE_URL          = "https://remotive.com/api/remote-jobs?category=data&limit=100"

default_args = {
    "owner": "aventra",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

# ── Helpers ──────────────────────────────────────────────────────
def make_id(*args) -> str:
    """Create a deterministic, stable ID from any fields."""
    raw = "_".join(str(a) for a in args).lower().strip()
    return hashlib.md5(raw.encode()).hexdigest()

def parse_salary(salary_str) -> tuple:
    if pd.isna(salary_str) or not str(salary_str).strip():
        return None, None

    # Kalau sudah numeric langsung (float/int dari pandas)
    try:
        val = float(salary_str)
        if val > 0:
            # Kalau nilainya kecil banget (< 1000), kemungkinan dalam satuan juta
            if val < 1000:
                val = val * 1_000_000
            return int(val), int(val)
    except (ValueError, TypeError):
        pass

    # Handle string format seperti "5.000.000 - 8.000.000" atau "Rp 5jt"
    cleaned = str(salary_str)
    cleaned = re.sub(r'[Rp\s]', '', cleaned)        # hapus Rp dan spasi
    cleaned = re.sub(r'(\d)\.(\d{3})', r'\1\2', cleaned)  # hapus titik ribuan: 5.000.000 → 5000000
    cleaned = re.sub(r'(\d+)jt', lambda m: str(int(m.group(1)) * 1_000_000), cleaned)  # jt → juta

    numbers = re.findall(r'\d+', cleaned)

    if not numbers:
        return None, None
    elif len(numbers) == 1:
        return int(numbers[0]), int(numbers[0])
    else:
        return int(numbers[0]), int(numbers[1])

def parse_experience(exp_str) -> int:
    """
    Parse experience string to integer years.
    Handles: '2 tahun', '1-3 tahun', '3', None
    Returns minimum years, or None if unparseable.
    """
    if pd.isna(exp_str) or not str(exp_str).strip():
        return None
    numbers = re.findall(r'\d+', str(exp_str))
    return int(numbers[0]) if numbers else None

def parse_tools(tools_str) -> list:
    """
    Parse tools/skills string into a clean list.
    Handles: 'SQL, Python, Tableau' or 'SQL Python Tableau'
    """
    if pd.isna(tools_str) or not str(tools_str).strip():
        return []

    # Split by common delimiters
    raw = re.split(r'[,;|/\n]+', str(tools_str))
    tools = [t.strip().lower() for t in raw if t.strip()]
    return tools

def get_opensearch_client():
    return OpenSearch(
        hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
        use_ssl=False,
        verify_certs=False,
        timeout=30,
    )

def ensure_index(client):
    if not client.indices.exists(INDEX_NAME):
        client.indices.create(index=INDEX_NAME, body={
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
            },
            "mappings": {
                "properties": {
                    "job_id":           {"type": "keyword"},
                    "posisi":           {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                    "perusahaan":       {"type": "keyword"},
                    "kota":             {"type": "keyword"},
                    "provinsi":         {"type": "keyword"},
                    "gaji_min":         {"type": "long"},
                    "gaji_max":         {"type": "long"},
                    "gaji_rata2":       {"type": "long"},
                    "tools":            {"type": "keyword"},
                    "pendidikan":       {"type": "keyword"},
                    "pengalaman_tahun": {"type": "integer"},
                    "level":            {"type": "keyword"},
                    "deskripsi":        {"type": "text"},
                    "sumber":           {"type": "keyword"},
                    "scraped_at":       {"type": "date"},
                }
            }
        })
        print(f"✅ Index created: {INDEX_NAME}")

# ════════════════════════════════════════════════════════════════
# TASK 1 — Load Dataset 2: Data & Analytics Jobs (555 records)
# This is our PRIMARY source — clean, focused, Indonesia-specific
# ════════════════════════════════════════════════════════════════
def load_data_jobs(**context):
    if not os.path.exists(PATH_DATA_JOBS):
        print(f"⚠️ File not found: {PATH_DATA_JOBS}")
        context["ti"].xcom_push(key="data_jobs", value=[])
        return

    df = pd.read_csv(PATH_DATA_JOBS)
    print(f"📂 Loaded {len(df)} rows from data jobs CSV")
    print(f"   Columns: {df.columns.tolist()}")

    jobs = []
    for _, row in df.iterrows():
        posisi    = str(row.get("posisi", "")).strip()
        perusahaan = str(row.get("perusahaan", "")).strip()

        if not posisi or posisi == "nan":
            continue

        gaji_min, gaji_max = parse_salary(row.get("gaji"))
        gaji_rata2 = (gaji_min + gaji_max) // 2 if gaji_min and gaji_max else None

        jobs.append({
            "job_id":           make_id("data_jobs", posisi, perusahaan),
            "posisi":           posisi,
            "perusahaan":       perusahaan,
            "kota":             str(row.get("kota", "")).strip() or None,
            "provinsi":         str(row.get("provinsi", "")).strip() or None,
            "gaji_min":         gaji_min,
            "gaji_max":         gaji_max,
            "gaji_rata2":       gaji_rata2,
            "tools":            parse_tools(row.get("tools")),
            "pendidikan":       str(row.get("pendidikan", "")).strip() or None,
            "pengalaman_tahun": parse_experience(row.get("pengalaman")),
            "level":            str(row.get("level", "")).strip() or None,
            "deskripsi":        str(row.get("deskripsi_lengkap", ""))[:3000],
            "sumber":           "kaggle_data_jobs",
            "scraped_at":       datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
        })

    print(f"✅ Prepared {len(jobs)} data analytics jobs")
    context["ti"].xcom_push(key="data_jobs", value=jobs)


# ════════════════════════════════════════════════════════════════
# TASK 2 — Load Dataset 1: Salary Dataset (32,976 records)
# Used for salary enrichment — adds market salary context
# ════════════════════════════════════════════════════════════════
def load_salary_data(**context):
    if not os.path.exists(PATH_SALARY):
        print(f"⚠️ File not found: {PATH_SALARY}")
        context["ti"].xcom_push(key="salary_jobs", value=[])
        return

    df = pd.read_csv(PATH_SALARY)
    print(f"📂 Loaded {len(df)} rows from salary CSV")
    print(f"   Columns: {df.columns.tolist()}")

    # Detect column names — Kaggle datasets vary
    # Try to find the right columns by inspecting
    col_posisi    = next((c for c in df.columns if "posisi" in c.lower() or "title" in c.lower() or "jabatan" in c.lower()), None)
    col_company   = next((c for c in df.columns if "perusahaan" in c.lower() or "company" in c.lower()), None)
    col_lokasi    = next((c for c in df.columns if "lokasi" in c.lower() or "location" in c.lower() or "kota" in c.lower()), None)
    col_gaji      = next((c for c in df.columns if "gaji" in c.lower() or "salary" in c.lower()), None)

    print(f"   Detected columns → posisi:{col_posisi}, company:{col_company}, lokasi:{col_lokasi}, gaji:{col_gaji}")

    if not col_posisi:
        print("⚠️ Could not detect posisi column, skipping salary data")
        context["ti"].xcom_push(key="salary_jobs", value=[])
        return

    jobs = []
    for _, row in df.iterrows():
        posisi     = str(row.get(col_posisi, "")).strip()
        perusahaan = str(row.get(col_company, "")).strip() if col_company else ""
        lokasi     = str(row.get(col_lokasi, "")).strip() if col_lokasi else ""

        if not posisi or posisi == "nan":
            continue

        # Gaji_Rata2 in this dataset is already averaged
        gaji_raw  = row.get(col_gaji)
        gaji_min, gaji_max = parse_salary(gaji_raw)
        gaji_rata2 = int(gaji_raw) if not pd.isna(gaji_raw) and str(gaji_raw).isdigit() else gaji_min

        jobs.append({
            "job_id":           make_id("salary", posisi, perusahaan, lokasi),
            "posisi":           posisi,
            "perusahaan":       perusahaan or None,
            "kota":             lokasi or None,
            "provinsi":         None,
            "gaji_min":         gaji_min,
            "gaji_max":         gaji_max,
            "gaji_rata2":       gaji_rata2,
            "tools":            [],
            "pendidikan":       None,
            "pengalaman_tahun": None,
            "level":            None,
            "deskripsi":        None,
            "sumber":           "kaggle_salary",
            "scraped_at":       datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
        })

    print(f"✅ Prepared {len(jobs)} salary records")
    context["ti"].xcom_push(key="salary_jobs", value=jobs)


# ════════════════════════════════════════════════════════════════
# TASK 3 — Fetch live remote jobs from Remotive API
# Free, no auth, official public API — real live data
# ════════════════════════════════════════════════════════════════
def fetch_remotive(**context):
    try:
        response = requests.get(REMOTIVE_URL, timeout=15)
        response.raise_for_status()
        raw_jobs = response.json().get("jobs", [])
        print(f"📡 Fetched {len(raw_jobs)} jobs from Remotive API")
    except Exception as e:
        print(f"⚠️ Remotive API failed: {e}")
        context["ti"].xcom_push(key="remotive_jobs", value=[])
        return

    jobs = []
    for job in raw_jobs:
        title    = str(job.get("title", "")).strip()
        company  = str(job.get("company_name", "")).strip()

        # Extract skills from tags + description
        tags        = [t.strip().lower() for t in job.get("tags", [])]
        description = job.get("description", "") or ""

        # Parse salary from candidate_required_location or salary field
        salary_str  = job.get("salary", "")
        gaji_min, gaji_max = parse_salary(salary_str)

        jobs.append({
            "job_id":           make_id("remotive", job.get("id", title + company)),
            "posisi":           title,
            "perusahaan":       company,
            "kota":             "Remote",
            "provinsi":         "Remote",
            "gaji_min":         gaji_min,
            "gaji_max":         gaji_max,
            "gaji_rata2":       (gaji_min + gaji_max) // 2 if gaji_min and gaji_max else None,
            "tools":            tags,
            "pendidikan":       None,
            "pengalaman_tahun": None,
            "level":            None,
            "deskripsi":        description[:3000],
            "sumber":           "remotive",
            "scraped_at":       datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
        })

    print(f"✅ Prepared {len(jobs)} Remotive jobs")
    context["ti"].xcom_push(key="remotive_jobs", value=jobs)


# ════════════════════════════════════════════════════════════════
# TASK 4 — Merge, deduplicate, validate
# ════════════════════════════════════════════════════════════════
def transform_and_merge(**context):
    data_jobs    = context["ti"].xcom_pull(key="data_jobs",    task_ids="load_data_jobs")    or []
    salary_jobs  = context["ti"].xcom_pull(key="salary_jobs",  task_ids="load_salary_data")  or []
    remotive_jobs = context["ti"].xcom_pull(key="remotive_jobs", task_ids="fetch_remotive")  or []

    all_jobs = data_jobs + salary_jobs + remotive_jobs
    print(f"\n📊 Source breakdown:")
    print(f"   data_jobs:     {len(data_jobs)}")
    print(f"   salary_jobs:   {len(salary_jobs)}")
    print(f"   remotive_jobs: {len(remotive_jobs)}")
    print(f"   total:         {len(all_jobs)}")

    # Deduplicate by job_id
    seen     = set()
    unique   = []
    for job in all_jobs:
        if job["job_id"] not in seen:
            seen.add(job["job_id"])
            unique.append(job)

    print(f"\n✅ After dedup: {len(unique)} unique jobs")

    # Data quality gate — fail loudly if something is very wrong
    if len(unique) == 0:
        raise ValueError("❌ Zero jobs after merge! Check all three sources.")

    # Stats for visibility in Airflow logs
    with_tools   = sum(1 for j in unique if j["tools"])
    with_salary  = sum(1 for j in unique if j["gaji_rata2"])
    with_level   = sum(1 for j in unique if j["level"])

    print(f"\n📈 Data quality stats:")
    print(f"   With tools:  {with_tools}/{len(unique)} ({with_tools*100//len(unique)}%)")
    print(f"   With salary: {with_salary}/{len(unique)} ({with_salary*100//len(unique)}%)")
    print(f"   With level:  {with_level}/{len(unique)} ({with_level*100//len(unique)}%)")

    context["ti"].xcom_push(key="final_jobs", value=unique)


# ════════════════════════════════════════════════════════════════
# TASK 5 — Bulk load to OpenSearch
# ════════════════════════════════════════════════════════════════
def load_to_opensearch(**context):
    final_jobs = context["ti"].xcom_pull(key="final_jobs", task_ids="transform_and_merge") or []

    if not final_jobs:
        print("⚠️ No jobs to load")
        return

    client = get_opensearch_client()
    ensure_index(client)

    # Build bulk actions
    actions = [
        {
            "_index":  INDEX_NAME,
            "_id":     job["job_id"],
            "_source": job,
        }
        for job in final_jobs
    ]

    # Bulk in chunks of 500 to avoid memory issues
    chunk_size = 500
    total_success = 0
    total_errors  = 0

    for i in range(0, len(actions), chunk_size):
        chunk = actions[i:i + chunk_size]
        success, errors = bulk(client, chunk, raise_on_error=False)
        total_success += success
        total_errors  += len(errors)
        print(f"   Chunk {i//chunk_size + 1}: {success} ok, {len(errors)} errors")

    print(f"\n✅ Final: {total_success} indexed, {total_errors} errors")

    # Confirm total docs in index
    count = client.count(index=INDEX_NAME)["count"]
    print(f"📊 Total documents in OpenSearch: {count}")


# ════════════════════════════════════════════════════════════════
# DAG Definition
# ════════════════════════════════════════════════════════════════
with DAG(
    dag_id="indonesia_jobs_pipeline",
    default_args=default_args,
    description="Indonesia job market intelligence pipeline",
    schedule_interval="0 19 * * *",  # 02:00 WIB daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["jobs", "kaggle", "opensearch", "indonesia", "data-engineering"],
) as dag:

    t_data_jobs   = PythonOperator(task_id="load_data_jobs",       python_callable=load_data_jobs)
    t_salary      = PythonOperator(task_id="load_salary_data",     python_callable=load_salary_data)
    t_remotive    = PythonOperator(task_id="fetch_remotive",       python_callable=fetch_remotive)
    t_merge       = PythonOperator(task_id="transform_and_merge",  python_callable=transform_and_merge)
    t_load        = PythonOperator(task_id="load_to_opensearch",   python_callable=load_to_opensearch)

    # All 3 sources run in parallel, merge waits for all
    [t_data_jobs, t_salary, t_remotive] >> t_merge >> t_load
