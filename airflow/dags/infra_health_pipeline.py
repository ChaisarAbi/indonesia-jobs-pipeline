from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import ssl
import socket
import time

# ── Config ──────────────────────────────────────────────────────
OPENSEARCH_HOST = "aventra-opensearch"
OPENSEARCH_PORT = 9200
INDEX_NAME      = "infra-health"

WAHA_URL        = "http://waha-infra:3000"
WAHA_API_KEY    = "aventra-infra-123"
WAHA_SESSION    = "default"
ALERT_TO        = "628978279036@c.us"

DOMAINS = [
    "air.aventra.my.id",
    "grafana.aventra.my.id",
    "search.aventra.my.id",
    "n8n.aventra.my.id",
    "waha.aventra.my.id",
    "waha3.aventra.my.id",
    "portfolio.aventra.my.id",
]

UPTIME_OK_CODES = [200, 204, 301, 302, 401, 403]
SSL_WARN_DAYS   = 30
ERROR_THRESHOLD = 10

default_args = {
    "owner": "aventra",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


# ── Helpers ──────────────────────────────────────────────────────
def get_opensearch_client():
    from opensearchpy import OpenSearch
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
                    "check_type":    {"type": "keyword"},
                    "domain":        {"type": "keyword"},
                    "status":        {"type": "keyword"},
                    "status_code":   {"type": "integer"},
                    "response_ms":   {"type": "integer"},
                    "ssl_days_left": {"type": "integer"},
                    "ssl_expiry":    {"type": "date"},
                    "error_count":   {"type": "integer"},
                    "error_message": {"type": "text"},
                    "checked_at":    {"type": "date"},
                }
            }
        })
        print(f"✅ Created index: {INDEX_NAME}")


def send_whatsapp_alert(message: str):
    try:
        response = requests.post(
            f"{WAHA_URL}/api/sendText",
            headers={
                "X-Api-Key": WAHA_API_KEY,
                "Content-Type": "application/json",
            },
            json={
                "session": WAHA_SESSION,
                "chatId": ALERT_TO,
                "text": message,
            },
            timeout=10,
        )
        if response.status_code in [200, 201]:
            print("✅ WhatsApp alert sent")
        else:
            print(f"⚠️ WhatsApp alert failed: {response.status_code} {response.text}")
    except Exception as e:
        print(f"⚠️ WhatsApp error: {e}")


# ════════════════════════════════════════════════════════════════
# TASK 1 — Uptime Check
# ════════════════════════════════════════════════════════════════
def check_uptime(**context):
    results = []
    issues  = []

    for domain in DOMAINS:
        url = f"https://{domain}"
        try:
            start    = time.time()
            response = requests.get(
                url,
                timeout=10,
                allow_redirects=True,
                verify=True,
                headers={"User-Agent": "AventraHealthBot/1.0"},
            )
            elapsed = int((time.time() - start) * 1000)
            status  = "UP" if response.status_code in UPTIME_OK_CODES else "DOWN"

            if status == "DOWN":
                issues.append(f"❌ {domain} returned {response.status_code}")

            results.append({
                "check_type":    "uptime",
                "domain":        domain,
                "status":        status,
                "status_code":   response.status_code,
                "response_ms":   elapsed,
                "ssl_days_left": None,
                "ssl_expiry":    None,
                "error_count":   None,
                "error_message": None,
                "checked_at":    datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
            })
            print(f"{'✅' if status == 'UP' else '❌'} {domain}: {status} "
                  f"({response.status_code}) {elapsed}ms")

        except requests.exceptions.SSLError as e:
            issues.append(f"🔒 {domain} SSL Error")
            results.append({
                "check_type":    "uptime",
                "domain":        domain,
                "status":        "SSL_ERROR",
                "status_code":   None,
                "response_ms":   None,
                "ssl_days_left": None,
                "ssl_expiry":    None,
                "error_count":   None,
                "error_message": str(e)[:500],
                "checked_at":    datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
            })

        except requests.exceptions.ConnectionError:
            issues.append(f"❌ {domain} is DOWN (connection refused)")
            results.append({
                "check_type":    "uptime",
                "domain":        domain,
                "status":        "DOWN",
                "status_code":   None,
                "response_ms":   None,
                "ssl_days_left": None,
                "ssl_expiry":    None,
                "error_count":   None,
                "error_message": "Connection refused",
                "checked_at":    datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
            })

        except Exception as e:
            results.append({
                "check_type":    "uptime",
                "domain":        domain,
                "status":        "ERROR",
                "status_code":   None,
                "response_ms":   None,
                "ssl_days_left": None,
                "ssl_expiry":    None,
                "error_count":   None,
                "error_message": str(e)[:500],
                "checked_at":    datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
            })

    context["ti"].xcom_push(key="uptime_results", value=results)
    context["ti"].xcom_push(key="uptime_issues",  value=issues)
    print(f"\n📊 Uptime: {len(results)} checked, {len(issues)} issues")


# ════════════════════════════════════════════════════════════════
# TASK 2 — SSL Certificate Check
# ════════════════════════════════════════════════════════════════
def check_ssl(**context):
    results = []
    issues  = []

    for domain in DOMAINS:
        try:
            ctx = ssl.create_default_context()
            with socket.create_connection((domain, 443), timeout=10) as sock:
                with ctx.wrap_socket(sock, server_hostname=domain) as ssock:
                    cert      = ssock.getpeercert()
                    expiry_str = cert["notAfter"]
                    expiry_dt  = datetime.strptime(expiry_str, "%b %d %H:%M:%S %Y %Z")
                    days_left  = (expiry_dt - datetime.utcnow()).days

                    status = "OK"
                    if days_left <= 7:
                        status = "CRITICAL"
                        issues.append(f"🚨 {domain} SSL expires in {days_left} days!")
                    elif days_left <= SSL_WARN_DAYS:
                        status = "WARNING"
                        issues.append(f"⚠️ {domain} SSL expires in {days_left} days")

                    results.append({
                        "check_type":    "ssl",
                        "domain":        domain,
                        "status":        status,
                        "status_code":   None,
                        "response_ms":   None,
                        "ssl_days_left": days_left,
                        "ssl_expiry":    expiry_dt.strftime("%Y-%m-%dT%H:%M:%S"),
                        "error_count":   None,
                        "error_message": None,
                        "checked_at":    datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
                    })
                    print(f"🔒 {domain}: {days_left} days left ({status})")

        except Exception as e:
            issues.append(f"❌ {domain} SSL check failed")
            results.append({
                "check_type":    "ssl",
                "domain":        domain,
                "status":        "ERROR",
                "status_code":   None,
                "response_ms":   None,
                "ssl_days_left": None,
                "ssl_expiry":    None,
                "error_count":   None,
                "error_message": str(e)[:500],
                "checked_at":    datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
            })

    context["ti"].xcom_push(key="ssl_results", value=results)
    context["ti"].xcom_push(key="ssl_issues",  value=issues)
    print(f"\n📊 SSL: {len(results)} checked, {len(issues)} issues")


# ════════════════════════════════════════════════════════════════
# TASK 3 — Log Anomaly Detection
# ════════════════════════════════════════════════════════════════
def check_log_anomaly(**context):
    results = []
    issues  = []

    try:
        client = get_opensearch_client()

        response = client.search(
            index="*",
            body={
                "size": 0,
                "query": {
                    "bool": {
                        "must": [
                            {
                                "range": {
                                    "@timestamp": {
                                        "gte": "now-30m",
                                        "lte": "now"
                                    }
                                }
                            }
                        ],
                        "should": [
                            {"match": {"log":     "ERROR"}},
                            {"match": {"log":     "CRITICAL"}},
                            {"match": {"log":     "Exception"}},
                            {"match": {"message": "ERROR"}},
                            {"match": {"message": "CRITICAL"}},
                        ],
                        "minimum_should_match": 1,
                        "must_not": [
                            {"match": {"log": "sshd"}},
                            {"match": {"log": "kex_exchange"}},
                            {"match": {"log": "Protocol major versions"}},
                            {"match": {"log": "Disconnected from"}},
                            {"match": {"log": "Invalid user"}},
                            {"match": {"log": "Failed password"}},
                        ]
                    }
                },
            },
            ignore_unavailable=True,
        )

        error_count = response.get("hits", {}).get("total", {}).get("value", 0)

        status = "OK"
        if error_count >= ERROR_THRESHOLD:
            status = "ANOMALY"
            issues.append(f"🚨 {error_count} errors detected in logs (last 30min)")
        elif error_count > 0:
            status = "WARNING"
            issues.append(f"⚠️ {error_count} errors found in logs (last 30min)")

        results.append({
            "check_type":    "log_anomaly",
            "domain":        "system_logs",
            "status":        status,
            "status_code":   None,
            "response_ms":   None,
            "ssl_days_left": None,
            "ssl_expiry":    None,
            "error_count":   error_count,
            "error_message": f"Found {error_count} errors in last 30 minutes",
            "checked_at":    datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
        })
        print(f"📋 Log anomaly: {error_count} errors ({status})")

    except Exception as e:
        print(f"⚠️ Log anomaly check failed: {e}")
        results.append({
            "check_type":    "log_anomaly",
            "domain":        "system_logs",
            "status":        "ERROR",
            "status_code":   None,
            "response_ms":   None,
            "ssl_days_left": None,
            "ssl_expiry":    None,
            "error_count":   None,
            "error_message": str(e)[:500],
            "checked_at":    datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
        })

    context["ti"].xcom_push(key="log_results", value=results)
    context["ti"].xcom_push(key="log_issues",  value=issues)


# ════════════════════════════════════════════════════════════════
# TASK 4 — Aggregate & Index to OpenSearch
# ════════════════════════════════════════════════════════════════
def aggregate_and_index(**context):
    from opensearchpy.helpers import bulk

    uptime_results = context["ti"].xcom_pull(key="uptime_results", task_ids="check_uptime") or []
    ssl_results    = context["ti"].xcom_pull(key="ssl_results",    task_ids="check_ssl") or []
    log_results    = context["ti"].xcom_pull(key="log_results",    task_ids="check_log_anomaly") or []

    all_results = uptime_results + ssl_results + log_results
    print(f"\n📊 Total results: {len(all_results)}")
    print(f"   Uptime:  {len(uptime_results)}")
    print(f"   SSL:     {len(ssl_results)}")
    print(f"   Logs:    {len(log_results)}")

    client = get_opensearch_client()
    ensure_index(client)

    actions = [{"_index": INDEX_NAME, "_source": r} for r in all_results]
    success, errors = bulk(client, actions, raise_on_error=False)
    print(f"✅ Indexed: {success} docs, {len(errors)} errors")

    # Summary untuk task berikutnya
    total_up     = sum(1 for r in uptime_results if r["status"] == "UP")
    ssl_critical = sum(1 for r in ssl_results if r["status"] in ["CRITICAL", "WARNING"])
    log_issues   = sum(1 for r in log_results if r["status"] in ["ANOMALY", "WARNING"])

    context["ti"].xcom_push(key="summary", value={
        "uptime_up":    total_up,
        "uptime_total": len(uptime_results),
        "ssl_critical": ssl_critical,
        "log_issues":   log_issues,
    })


# ════════════════════════════════════════════════════════════════
# TASK 5 — Send WhatsApp Alert
# ════════════════════════════════════════════════════════════════
def send_alerts(**context):
    uptime_issues = context["ti"].xcom_pull(key="uptime_issues", task_ids="check_uptime") or []
    ssl_issues    = context["ti"].xcom_pull(key="ssl_issues",    task_ids="check_ssl") or []
    log_issues    = context["ti"].xcom_pull(key="log_issues",    task_ids="check_log_anomaly") or []
    summary       = context["ti"].xcom_pull(key="summary",       task_ids="aggregate_and_index") or {}

    all_issues = uptime_issues + ssl_issues + log_issues

    # Kirim alert kalau ada issues
    if all_issues:
        message = (
            f"🚨 *Aventra Infrastructure Alert*\n"
            f"_{datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC_\n\n"
            f"*Issues Detected:*\n"
            f"{chr(10).join(all_issues)}\n\n"
            f"*Summary:*\n"
            f"✅ Uptime: {summary.get('uptime_up', 0)}/{summary.get('uptime_total', 0)} domains up\n"
            f"🔒 SSL: {summary.get('ssl_critical', 0)} certificates need attention\n"
            f"📋 Logs: {summary.get('log_issues', 0)} anomalies detected"
        )
        print(f"🚨 Sending alert for {len(all_issues)} issues...")
        send_whatsapp_alert(message)

    else:
        # Kirim periodic report setiap 6 jam (jam 0, 6, 12, 18 UTC)
        now = datetime.utcnow()
        if now.minute < 30 and now.hour % 6 == 0:
            message = (
                f"✅ *Aventra Infrastructure Report*\n"
                f"_{now.strftime('%Y-%m-%d %H:%M')} UTC_\n\n"
                f"All systems operational!\n"
                f"✅ {summary.get('uptime_up', 0)}/{summary.get('uptime_total', 0)} domains up\n"
                f"🔒 All SSL certificates valid\n"
                f"📋 No log anomalies detected"
            )
            print("📊 Sending periodic health report...")
            send_whatsapp_alert(message)
        else:
            print("✅ No issues found, skipping alert")


# ════════════════════════════════════════════════════════════════
# DAG Definition
# ════════════════════════════════════════════════════════════════
with DAG(
    dag_id="infra_health_pipeline",
    default_args=default_args,
    description="Infrastructure health monitoring — uptime, SSL, log anomaly + WhatsApp alerts",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["infrastructure", "monitoring", "devops", "opensearch", "whatsapp"],
) as dag:

    t_uptime = PythonOperator(
        task_id="check_uptime",
        python_callable=check_uptime,
    )

    t_ssl = PythonOperator(
        task_id="check_ssl",
        python_callable=check_ssl,
    )

    t_logs = PythonOperator(
        task_id="check_log_anomaly",
        python_callable=check_log_anomaly,
    )

    t_index = PythonOperator(
        task_id="aggregate_and_index",
        python_callable=aggregate_and_index,
    )

    t_alert = PythonOperator(
        task_id="send_alerts",
        python_callable=send_alerts,
    )

    # 3 checks parallel → aggregate → alert
    [t_uptime, t_ssl, t_logs] >> t_index >> t_alert