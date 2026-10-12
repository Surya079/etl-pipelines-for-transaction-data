# 🏦 DATA ENGINEERING — DATA ETL PIPELINE

A fully containerized, production-style local data engineering environment that generates synthetic banking transactions, ingests them through a medallion architecture (Raw → Core), orchestrates workflows with Apache Airflow, and stores/processes data across PostgreSQL, HDFS, Spark, and NiFi — all wired together with Docker Compose.

---

## 📖 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Synthetic Transaction Generator API](#-synthetic-transaction-generator-api)
- [Services Breakdown](#-services-breakdown)
- [Prerequisites](#-prerequisites)
- [Required .env Variables](#-required-env-variables)
- [Quick Start](#-quick-start)
- [Accessing the UIs](#-accessing-the-uis)
- [Running the ETL Pipeline](#-running-the-etl-pipeline)
- [Spark Jobs](#-spark-jobs)
- [Data Quality Checks (Soda)](#-data-quality-checks-soda)
- [NiFi Flow Setup](#-nifi-flow-setup)
- [Basic Docker Commands](#-basic-docker-commands)
- [Data Volumes](#-data-volumes)
- [Troubleshooting](#-troubleshooting)
- [Git SSH Permission Fix](#-git-ssh-permission-fix)
- [Shutdown & Cleanup](#-shutdown--cleanup)

---

## 🧭 Overview

The stack is a local data engineering environment that combines:

- A **PostgreSQL** database (transaction data + Airflow metadata)
- **Redis** as the Celery broker for Airflow
- A **FastAPI** service that generates synthetic transaction data
- **Apache Airflow** (webserver, scheduler, worker, init) for orchestration
- **Apache Hadoop HDFS** (namenode + datanode) for distributed storage
- **Apache Spark** (master + worker) for distributed processing
- **Jupyter** (PySpark notebook) for interactive development
- **Apache NiFi** for visual data flow management
- **Soda** for data quality checks

All services share a single Docker bridge network (`hadoopnet`) so they can reach each other by service name.

---

## 🏗 Architecture

```
┌─────────────────┐      ┌──────────────┐      ┌──────────────────┐
│  FastAPI        │─────▶│  PostgreSQL  │◀────▶│  Airflow         │
│  (Synthetic Tx) │      │  banking_etl │      │  (Orchestrator)  │
└─────────────────┘      └──────┬───────┘      └────────┬─────────┘
                                │                       │
                                ▼                       ▼
                         ┌─────────────┐         ┌─────────────┐
                         │  HDFS       │◀───────▶│  Spark      │
                         │  (Storage)  │         │  (Compute)  │
                         └──────┬──────┘         └──────┬──────┘
                                │                       │
                                ▼                       ▼
                         ┌─────────────┐         ┌─────────────┐
                         │  NiFi       │         │  Jupyter    │
                         │  (Flow)     │         │  (Dev)      │
                         └─────────────┘         └─────────────┘
```

---

## 🧬 Synthetic Transaction Generator API

A **FastAPI** application that generates realistic synthetic financial transaction data for testing, simulation, and development purposes. The API returns random transactions with many fields covering card details, merchant information, amounts, timestamps, and fraud indicators.

### Features

- Generate single or multiple random transactions.
- Rich set of fields modeled after real payment processing data.
- Uses [Faker](https://faker.readthedocs.io/) for realistic names, addresses, and other personal data.
- Pydantic schema for validation and automatic OpenAPI documentation.
- No database required — all data is generated on the fly.

### Requirements

- Python 3.9+
- Dependencies listed in `requirements.txt` (install with `pip install -r requirements.txt`).

### Installation and Local Run

1. **Clone the repository** (or download the code).

2. **Create and activate a virtual environment** (recommended):

   ```bash
   python -m venv venv
   source venv/bin/activate        # Linux/macOS
   venv\Scripts\activate           # Windows
   ```

3. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

4. **Run the API:**

   ```bash
   uvicorn main:app --reload --port 8000
   ```

5. **Open the interactive docs:** http://localhost:8000/docs

---

## 🧩 Services Breakdown

| Service             | Image                                  | Purpose                                                   | Ports (host)                       | Depends on                  |
| ------------------- | -------------------------------------- | --------------------------------------------------------- | ---------------------------------- | --------------------------- |
| `postgres`          | `postgres:15`                          | Main DB (`banking_etl`) + Airflow metadata DB (`airflow`) | `${POSTGRES_PORT}` → 5432          | —                           |
| `redis`             | `redis:7`                              | Celery broker / result backend for Airflow                | —                                  | —                           |
| `api`               | `Dockerfile.api`                       | FastAPI synthetic transaction generator                   | `${API_PORT}` → 8000               | postgres                    |
| `airflow-init`      | `Dockerfile.airflow`                   | Runs once: DB init + creates admin user                   | —                                  | postgres, redis             |
| `airflow-webserver` | `Dockerfile.airflow`                   | Airflow UI                                                | `${AIRFLOW_WEBSERVER_PORT}` → 8080 | airflow-init                |
| `airflow-scheduler` | `Dockerfile.airflow`                   | Airflow scheduler                                         | —                                  | airflow-init                |
| `airflow-worker`    | `Dockerfile.airflow`                   | Celery worker executing tasks                             | —                                  | airflow-init                |
| `hdfs-namenode`     | `bde2020/hadoop-namenode`              | HDFS NameNode                                             | 9870 (web UI)                      | —                           |
| `hdfs-datanode`     | `bde2020/hadoop-datanode`              | HDFS DataNode                                             | —                                  | hdfs-namenode               |
| `spark-master`      | `bitnami/spark:3.2.1`                  | Spark master                                              | 7077, 8081 (UI)                    | —                           |
| `spark-worker`      | `bitnami/spark:3.2.1`                  | Spark worker                                              | —                                  | spark-master                |
| `jupyter`           | `jupyter/pyspark-notebook:spark-3.2.1` | Notebook for PySpark dev                                  | 8888, 4040                         | spark-master, hdfs-namenode |
| `nifi`              | `apache/nifi:latest`                   | Visual data flow / ETL tool                               | 8443 (HTTPS UI), 10000             | —                           |

> **Note:** Spark master UI uses host port **8081** to avoid conflict with Airflow webserver on **8080**.

---

## ✅ Prerequisites

- Docker Desktop (or Docker Engine) with **Compose v2**
- A `.env` file in the project root with the required variables (see below)
- Directories for volume mounts:

```
postgres-init/                 # contains init.sql
dags/, scripts/, config/, datawarehouse/, data_quality/
spark-apps/, hadoop-config/, spark-events/
nifi/conf, nifi/database_repository, nifi/flowfile_repository,
nifi/content_repository, nifi/provenance_repository, nifi/state, nifi/logs
```

Create the NiFi persistence directories:

```bash
mkdir -p nifi/conf nifi/database_repository nifi/flowfile_repository \
         nifi/content_repository nifi/provenance_repository nifi/state nifi/logs
```

---

## 🔐 Required .env Variables

Create a `.env` file in the project root:

```env
# PostgreSQL
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=banking_etl
POSTGRES_PORT=5432

# Airflow metadata DB (separate database in the same Postgres instance)
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=airflow
AIRFLOW_DB_NAME=airflow

# Airflow secrets (must be identical across all Airflow services)
AIRFLOW_FERNET_KEY=46BKJoQYlPPOexq0OhDZnIlNepKFf87WFwLbfzqDDho=
AIRFLOW__WEBSERVER__SECRET_KEY=your-super-secret-key-change-me

# Airflow admin user
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_EMAIL=admin@example.com

# Host ports
API_PORT=8000
AIRFLOW_WEBSERVER_PORT=8080
```

> ⚠️ **Security:** Change `AIRFLOW__WEBSERVER__SECRET_KEY` and `AIRFLOW_FERNET_KEY` in any non-local environment. Never commit `.env` to Git.

---

## 🚀 Quick Start

### First-time Start

```bash
# 1. Create NiFi persistence directories
mkdir -p nifi/conf nifi/database_repository nifi/flowfile_repository \
         nifi/content_repository nifi/provenance_repository nifi/state nifi/logs

# 2. Copy and edit the env file
cp .env.example .env

# 3. Validate the compose file
docker compose config

# 4. Build custom images (Dockerfile.api and Dockerfile.airflow)
docker compose build

# 5. Start the stack
docker compose up -d

# 6. Check status
docker compose ps
```

Wait for all services to be healthy. `airflow-init` will run once and exit.

---

## 🌐 Accessing the UIs

| Service          | URL                         | Credentials                                           |
| ---------------- | --------------------------- | ----------------------------------------------------- |
| FastAPI docs     | http://localhost:8000/docs  | —                                                     |
| Airflow          | http://localhost:8080       | `${AIRFLOW_ADMIN_USER}` / `${AIRFLOW_ADMIN_PASSWORD}` |
| HDFS NameNode UI | http://localhost:9870       | —                                                     |
| Spark Master UI  | http://localhost:8081       | —                                                     |
| Jupyter          | http://localhost:8888       | Token from `docker compose logs jupyter`              |
| NiFi             | https://localhost:8443/nifi | `admin` / `adminadminadmin`                           |

Get the Jupyter token:

```bash
docker compose logs jupyter | grep "token"
```

---

## 🔄 Running the ETL Pipeline

Trigger the DAG in the Airflow UI, or test tasks manually:

```bash
docker compose exec airflow-scheduler airflow tasks test transaction_etl_dag ingest_transactions 2026-01-01
docker compose exec airflow-scheduler airflow tasks test transaction_etl_dag transform_to_core 2026-01-01
docker compose exec airflow-scheduler airflow tasks test transaction_etl_dag soda_quality_checks 2026-01-01
```

### Connect to PostgreSQL

**From inside the Postgres container:**

```bash
docker compose exec postgres psql -U postgres -d banking_etl
```

**From your host (e.g., DBeaver):**

- Host: `localhost`
- Port: `5432`
- User: `postgres`
- Password: from `.env`
- Database: `banking_etl`

---

## ⚡ Spark Jobs

Copy your script into `spark-apps/` (mounted at `/opt/spark-apps`), then:

```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/your_job.py
```

### Full Spark Job Commands

**Ingestion with schema:**

```bash
HADOOP_USER_NAME=root /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/spark-apps/schema/clearing_schema.py \
  /opt/spark-apps/ingestion_script_with_schema_v1.py
```

**Profiling data:**

```bash
HADOOP_USER_NAME=root /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/spark-apps/schema/clearing_schema.py \
  /opt/spark-apps/profiling_data.py
```

**Build bronze clean data:**

```bash
HADOOP_USER_NAME=root /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/spark-apps/schema/clearing_schema.py \
  /opt/spark-apps/build_bronze_clean_data.py
```

Or use Jupyter at http://localhost:8888 and run PySpark interactively.

---

## 🧪 Data Quality Checks (Soda)

Soda is used to validate both the **Raw** and **Core** tables.

**Check the Raw table:**

```bash
./.venv/Scripts/soda.exe scan -c data_quality/configuration.yml -d postgres data_quality/checks/raw_transactions.yml
```

**Check the Core table:**

```bash
./.venv/Scripts/soda.exe scan -c data_quality/configuration.yml -d postgres data_quality/checks/core_transactions.yml
```

> On Linux/macOS, replace `./.venv/Scripts/soda.exe` with `./.venv/bin/soda`.

---

## 🌊 NiFi Flow Setup

1. Open https://localhost:8443/nifi (accept the self-signed cert).
2. Log in with `admin` / `adminadminadmin`.
3. Drag processors onto the canvas (e.g., `GetFile` → `PutHDFS`).
4. Configure `PutHDFS` with:
   - **Hadoop Configuration Resources:** `/opt/nifi/nifi-current/conf/core-site.xml`
     (you may need to copy `hadoop-config/core-site.xml` into the NiFi conf mount)
   - **Directory:** `/user/nifi/data`
5. Start the flow.

---

## 🐳 Basic Docker Commands

All commands assume you are in the project root (where `docker-compose.yml` lives).

### Validate the Compose File

```bash
docker compose config
```

Prints the resolved file with env vars substituted. Use this to catch syntax or variable errors before starting.

### Build Custom Images

Rebuilds `Dockerfile.api` and `Dockerfile.airflow`:

```bash
docker compose build
```

Force a clean rebuild:

```bash
docker compose build --no-cache
```

### Start the Stack

Start everything in the background:

```bash
docker compose up -d
```

Start specific services (e.g., DB + API):

```bash
docker compose up -d postgres api
```

### Check Status

```bash
docker compose ps
```

Shows each container, its state, and port mappings.

### View Logs

All services:

```bash
docker compose logs -f
```

A specific service:

```bash
docker compose logs -f airflow-scheduler
```

Last 200 lines only:

```bash
docker compose logs --tail 200 airflow-worker
```

### Execute a Command Inside a Container

```bash
docker compose exec airflow-scheduler bash
```

> On Windows Git Bash, prefix with `MSYS_NO_PATHCONV=1` to avoid path mangling.

### Stop the Stack

Stop and remove containers (keeps volumes and images):

```bash
docker compose down
```

### Full Cleanup

Stop, remove containers, and delete volumes (Postgres/HDFS data will be lost):

```bash
docker compose down -v
```

### Restart a Single Service

```bash
docker compose restart airflow-scheduler
```

Force recreate (picks up new env vars):

```bash
docker compose up -d --force-recreate airflow-worker
```

---

## 💾 Data Volumes

**Named volumes:**

| Volume         | Purpose                     |
| -------------- | --------------------------- |
| `pgdata`       | PostgreSQL data persistence |
| `hdfs-nn-data` | HDFS NameNode metadata      |
| `hdfs-dn-data` | HDFS DataNode blocks        |

**Bind mounts (host directories):**

| Host Path                       | Purpose                          |
| ------------------------------- | -------------------------------- |
| `./dags`                        | Airflow DAGs                     |
| `./scripts`                     | Ingestion / creation scripts     |
| `./config`                      | `settings.py`, `.env`            |
| `./datawarehouse`               | Transformation code              |
| `./data_quality`                | Soda config and checks           |
| `./spark-apps`                  | Spark jobs and Jupyter notebooks |
| `./hadoop-config/core-site.xml` | Hadoop client config             |
| `./nifi/*`                      | NiFi persistence                 |

---

## 🛠 Troubleshooting

### Airflow log view shows "Could not read served logs"

Ensure all Airflow services share the same `AIRFLOW__WEBSERVER__SECRET_KEY`, `AIRFLOW__CORE__HOSTNAME_CALLABLE`, and base URLs.

### `raw_transactions` does not exist

Run the DAG's `create_tables` task, or manually:

```bash
docker compose exec airflow-scheduler python /opt/airflow/scripts/create_raw_table.py
docker compose exec airflow-scheduler python /opt/airflow/scripts/create_core_table.py
```

### Postgres password authentication failed

The `.env` password differs from the one baked into the existing volume. Either fix `.env` or reset:

```bash
docker compose down -v
docker compose up -d
```

### Port already in use

Change the host port in `.env` (e.g., `POSTGRES_PORT=5433`). The container port stays the same.

### NiFi permission errors

Ensure the host `nifi/` subdirectories exist and are writable:

```bash
mkdir -p nifi/{conf,database_repository,flowfile_repository,content_repository,provenance_repository,state,logs}
```

### Git Bash path mangling

Prefix commands with `MSYS_NO_PATHCONV=1`:

```bash
MSYS_NO_PATHCONV=1 docker compose exec airflow-scheduler ls /opt/airflow/dags
```

---

## 🔑 Git SSH Permission Fix

If you see this error when pushing:

```
ERROR: Permission to <owner>/<repo>.git denied to <authenticated-user>.
```

GitHub is telling you that your SSH key belongs to `<authenticated-user>`, but that account doesn't have write access to the repository. Follow these steps:

### STEP 1 — Understand the error message

```
ERROR: Permission to <owner>/<repo>.git denied to <authenticated-user>.
```

GitHub is telling you that your SSH key belongs to `<authenticated-user>`, but that account doesn't have write access to the repository.

### STEP 2 — Check which GitHub account SSH is currently using

```bash
ssh -T git@github.com
```

Expected output:

```
Hi <authenticated-user>! You've successfully authenticated...
```

This confirms the account your current SSH key is tied to. If it's the wrong one, continue.

### STEP 3 — List all SSH keys on your machine

```bash
ls -al ~/.ssh
```

You'll see files like `id_rsa`, `id_ed25519`, or custom names. Each public key (`*.pub`) corresponds to a private key that can be added to a GitHub account.

> **Explanation:** You may have multiple keys for different accounts. The system may be offering a key that belongs to the wrong account.

### STEP 4 — Find out which exact key is being offered to GitHub

```bash
ssh -vT git@github.com
```

Look for lines containing **Offering public key**. The path shown is the key file currently being used.

> **Explanation:** This verbose output tells you which private key SSH is trying. If it's not the one you want, you need to force SSH to use the correct key.

### STEP 5 — Obtain or create the correct SSH key

**Option A — You already have a key for the correct account**
If a key exists (e.g., `id_ed25519_surya079`) but isn't being used, skip to Step 6.

**Option B — Generate a new key for the correct GitHub account**

```bash
ssh-keygen -t ed25519 -C "your_email@example.com" -f ~/.ssh/id_ed25519_correct_account
```

- `-C` adds a comment (usually your email) to identify the key.
- `-f` sets a custom filename so you don't overwrite existing keys.

Press Enter to accept the default location (the custom name) and choose a passphrase (optional).

> **Explanation:** This creates a new private/public key pair specifically for the account you want to use.

### STEP 6 — Add the public key to the correct GitHub account

Copy the public key:

```bash
cat ~/.ssh/id_ed25519_correct_account.pub
```

Go to **GitHub → Settings → SSH and GPG keys → New SSH key**. Paste the key and save.

> **Explanation:** GitHub needs to know that this key is allowed to authenticate as the desired account.

### STEP 7 — Force SSH to use the correct key for GitHub

Create or edit `~/.ssh/config`:

```bash
nano ~/.ssh/config
```

Add (or modify) these lines:

```
Host github.com
  HostName github.com
  User git
  IdentityFile ~/.ssh/id_ed25519_correct_account
  IdentitiesOnly yes
```

- `IdentityFile` points to the private key you want to use.
- `IdentitiesOnly yes` tells SSH to only try that key and ignore others.

Save and exit (`Ctrl+O`, `Enter`, `Ctrl+X` in nano).

> **Explanation:** By default, SSH may try several keys and pick the first one that GitHub accepts. This forces it to use exactly the key you specify.

### STEP 8 — Test the SSH authentication again

```bash
ssh -T git@github.com
```

Now it should say:

```
Hi <correct-account>! You've successfully authenticated...
```

If it still shows the wrong account, double-check that:

- The key file path in `~/.ssh/config` is correct.
- The public key was added to the correct GitHub account.
- `IdentitiesOnly yes` is present.

### STEP 9 — Push again

```bash
git push -u origin main
```

---

## 🧹 Shutdown and Cleanup

**Stop containers, keep data:**

```bash
docker compose down
```

**Stop and delete all data volumes:**

```bash
docker compose down -v
```

**Remove unused images/networks after cleanup:**

```bash
docker system prune -f
```

---

## 📌 Notes

- The **FastAPI synthetic transaction generator** returns realistic random transactions with card details, merchant info, amounts, timestamps, and fraud indicators — modeled after real payment processing data, using [Faker](https://faker.readthedocs.io/) for realistic personal data.
- No database is required for the API itself — all data is generated on the fly.
- Pydantic schemas provide validation and automatic OpenAPI documentation.
- Python 3.9+ is required for local (non-Docker) development of the API.
- Dependencies for the API are listed in `requirements.txt` (install with `pip install -r requirements.txt`).
- **Security:** Always change `AIRFLOW__WEBSERVER__SECRET_KEY` and `AIRFLOW_FERNET_KEY` in any non-local environment. Never commit `.env` to Git.

---

**Happy data engineering! 🚀**
