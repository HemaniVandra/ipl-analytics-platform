# IPL Analytics Platform

A sophisticated ETL/ELT platform leveraging Databricks (Spark) and AWS to transform raw IPL match data into actionable insights. This project demonstrates advanced Medallion Architecture, CI/CD integration, and robust data orchestration, bridging the gap between Python-based automation and professional Data Engineering.

---

## Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Data Sources│────▶│    Bronze    │────▶│    Silver    │────▶│     Gold     │
│  (APIs, Web) │     │  (Raw Data)  │     │ (Cleaned)    │     │ (Analytics)  │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
       │                    │                    │                    │
       └────────────────────┴────────────────────┴────────────────────┘
                          Orchestrated by Apache Airflow
                          Processed on Databricks (Spark)
                          Stored in AWS S3
```

## Tech Stack

| Component        | Technology                        |
|------------------|-----------------------------------|
| Orchestration    | Apache Airflow (Dockerized)       |
| Processing       | Databricks / Apache Spark         |
| Storage          | AWS S3                            |
| Transformations  | dbt (Data Build Tool)             |
| Data Quality     | Great Expectations                |
| Containerization | Docker & Docker Compose           |

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- Databricks workspace with a PAT (Personal Access Token)
- AWS credentials (Access Key + Secret Key) with S3 access
- CricAPI key (for live match data)

---

## Project Structure

```
ipl-analytics-platform/
├── dags/                    # Airflow DAG definitions
│   ├── nightly_pipeline.py  # Nightly batch pipeline (scrape → bronze → silver → gold)
│   └── live_match_dag.py    # Live match pipeline (runs during IPL season)
├── config/                  # App configuration
│   ├── config.py
│   └── config.yaml
├── ingestion/               # Data ingestion modules
│   ├── scrapers/            # Web scrapers (Cricinfo)
│   └── loaders/             # Data loaders (Cricsheet → Databricks)
├── dbt/                     # dbt project (Gold layer transforms)
├── expectations/            # Great Expectations checkpoints
├── notebooks/               # Databricks notebooks
├── plugins/                 # Airflow custom plugins
├── logs/                    # Airflow logs (auto-generated, gitignored)
├── scripts/
│   └── entrypoint.sh        # Docker entrypoint (auto-creates connections)
├── Dockerfile               # Custom Airflow image
├── docker-compose.yaml      # Docker Compose orchestration
├── requirements.txt         # Python dependencies
├── .env                     # Secrets & config (gitignored)
└── .env.example             # Template for .env
```

---

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/ipl-analytics-platform.git
cd ipl-analytics-platform
```

### 2. Set Up Environment Variables

Copy the example env file and fill in your secrets:

```bash
cp .env.example .env
```

Edit `.env` and fill in:

```env
AIRFLOW_UID=197609
DATABRICKS_HOST=your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your_dbt_token_here
AIRFLOW_DATABRICKS_TOKEN=your_airflow_pat_token_here
CRICAPI_KEY=your_cricapi_key

AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=ap-south-1
S3_BUCKET=ipl-analytics-raw-data

POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
```

### 3. Build & Start Airflow

```bash
docker compose up -d
```

This will:
1. Pull the Postgres image & build the custom Airflow image
2. Run database migrations & create an admin user
3. Start the **webserver** (UI) and **scheduler**

First-time build takes ~3-5 minutes. Subsequent starts are much faster.

### 4. Access Airflow UI

Open **http://localhost:8080** in your browser.

| Field    | Value      |
|----------|------------|
| Username | `airflow`  |
| Password | `airflow`  |

### 5. Verify Connections

Go to **Admin → Connections** in the Airflow UI and confirm:

- **`databricks_default`** — Type: `databricks`, Host: `https://your-workspace.cloud.databricks.com`
- **`aws_default`** — Type: `aws`, with your AWS credentials

> **Note:** Connections are automatically created on first startup via the entrypoint script. If they're missing, see [Manual Connection Setup](#manual-connection-setup) below.

---

## Docker Commands — Quick Reference

### Start Airflow

```bash
docker compose up -d
```
Starts all services (Postgres, Webserver, Scheduler) in the background.
Airflow UI will be available at **http://localhost:8080** within ~30 seconds.

### Stop Airflow

```bash
docker compose down
```
Stops all containers gracefully. **Your data (DAGs, connections, logs) is preserved.**

### Restart Airflow

```bash
docker compose restart
```
Restarts all running containers without removing them. Use this after changing environment variables or config.

If you've changed the `Dockerfile` or `requirements.txt`, rebuild first:

```bash
docker compose up -d --build
```

### View Logs

```bash
# All services
docker compose logs -f

# Webserver only
docker compose logs -f airflow-webserver

# Scheduler only
docker compose logs -f airflow-scheduler
```

### Check Status

```bash
docker compose ps
```

### Delete Everything (Fresh Start)

```bash
docker compose down -v
```
This stops all containers **and deletes the Postgres data volume** (connections, DAG run history, users — everything). Use this when you want a completely clean slate.

To also remove the built images:

```bash
docker compose down -v --rmi all
```

---

## After a System Restart

Docker Desktop restarts your containers automatically if it's set to launch on startup. If not:

1. **Open Docker Desktop** (or start the Docker daemon)
2. Run:
   ```bash
   docker compose up -d
   ```
3. Wait ~30 seconds, then open **http://localhost:8080**

That's it — your DAGs, connections, and run history are all persisted in the Postgres volume.

---

## Manual Connection Setup

If the automatic connection creation didn't run, you can create them manually:

### Via Docker CLI

```bash
# Databricks connection
docker compose exec airflow-webserver airflow connections add databricks_default \
    --conn-type databricks \
    --conn-host "https://your-workspace.cloud.databricks.com" \
    --conn-password "your_databricks_pat_token"

# AWS connection
docker compose exec airflow-webserver airflow connections add aws_default \
    --conn-type aws \
    --conn-login "your_aws_access_key" \
    --conn-password "your_aws_secret_key" \
    --conn-extra '{"region_name": "ap-south-1"}'
```

### Via Airflow UI

1. Go to **Admin → Connections**
2. Click **+** (Add a new record)
3. Fill in the connection details:
   - **Conn Id:** `databricks_default`
   - **Conn Type:** `Databricks`
   - **Host:** `https://your-workspace.cloud.databricks.com`
   - **Password:** Your Databricks PAT token

---

## DAGs Overview

### `ipl_nightly_pipeline`
Runs daily at **02:00 IST** (20:30 UTC). Full batch pipeline:

```
Scrape Cricinfo ──┐
                  ├──▶ Load Bronze ──▶ Silver Transforms ──▶ GE Checkpoint ──▶ dbt Gold ──▶ Optimize ──▶ Notify
Fetch Live API ───┘
```

### `ipl_live_match_pipeline`
Runs every **30 minutes** during IPL match hours (1:30 PM – 11:30 PM IST):

```
Check Live Match ──▶ Fetch Scores ──▶ Load Bronze ──▶ Silver Live ──▶ Refresh Dashboard
```

> **Note:** Both DAGs are paused by default on first deploy. Unpause them from the Airflow UI when ready.

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| Port 8080 already in use | Stop the other service, or change the port in `docker-compose.yaml` (e.g., `"8081:8080"`) |
| Containers won't start | Run `docker compose logs` to check errors |
| DAGs not showing up | Check `docker compose logs airflow-scheduler` for import errors |
| Connection test fails | Verify your PAT token is valid and the Databricks workspace URL is correct |
| Need a fresh start | Run `docker compose down -v` then `docker compose up -d` |
| `AIRFLOW_UID` warning | Set `AIRFLOW_UID` in `.env` to your host user ID (run `id -u` on Linux/Mac) |
