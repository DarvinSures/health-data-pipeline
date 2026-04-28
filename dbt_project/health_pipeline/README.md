# 🏥 Health Data Pipeline

A production-grade ELT pipeline that ingests patient data from Google Sheets, transforms it into a standardised **FHIR Patient** format, and loads it into a PostgreSQL database — with full data quality checks, PII protection, orchestration, and monitoring.

---

## 📐 Architecture

```
Google Sheets
     │
     ▼
┌─────────────┐
│   Landing   │  Raw JSONB rows — exact copy of source, schema-change resilient
└─────────────┘
     │
     ▼
┌─────────────┐
│     Raw     │  Typed & cleaned — proper dates, nulls handled
└─────────────┘
     │
     ▼
┌─────────────┐
│   Staging   │  dbt — PII hashed, columns renamed, light cleaning
└─────────────┘
     │
     ▼
┌─────────────┐
│ Consumption │  dbt — Final FHIR Patient table, ready for downstream use
└─────────────┘
     │
     ▼
┌─────────────────────────────┐
│  Prefect Cloud              │  Orchestration, scheduling, monitoring
│  dbt Tests                  │  FHIR-aligned data quality checks        
└─────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Source | Google Sheets + GCP Service Account | Data source |
| Ingestion | Python + gspread | Extract and load |
| Storage | PostgreSQL (Docker) | Local database |
| Transformation | dbt Core | Layered SQL models |
| Data Quality | dbt Tests | FHIR-aligned pre and post transform checks |
| PII Protection | MD5 Hashing + Column Masking | HIPAA-aligned data protection |
| Orchestration | Prefect Cloud | Scheduling and monitoring |
| Version Control | GitHub | Source code management |

---

## 📁 Project Structure

```
health-data-pipeline/
├── ingestion/
│   └── load_data.py                  # Google Sheet → landing → raw
├── dbt_project/
│   └── health_pipeline/
│       ├── models/
│       │   ├── staging/
│       │   │   ├── sources.yml       # Source definitions + FHIR tests
│       │   │   └── stg_patient.sql   # PII hashing + cleaning
│       │   └── consumption/
│       │       └── fhir_patient.sql  # Final FHIR Patient table
│       ├── macros/
│       │   ├── create_schemas.sql    # Auto-creates all schemas
│       │   └── generate_schema_name.sql
│       ├── tests/
│       │   ├── fhir_telecom_valid.sql
│       │   ├── fhir_id_max_length.sql
│       │   ├── fhir_no_duplicate_patients.sql
│       │   ├── fhir_insurance_not_null.sql
│       │   └── fhir_birth_date_not_in_future.sql
│       ├── packages.yml
│       ├── dbt_project.yml
│       └── profiles.yml
├── orchestration/
│   └── pipeline_flow.py              # Prefect flow
├── credentials/                      # GCP service account (git ignored)
├── .env                              # Environment variables (git ignored)
├── .gitignore
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## ✅ Prerequisites

Before you begin, make sure the following are installed on your machine:

### 1. Git
Download and install from [git-scm.com](https://git-scm.com/downloads).

Verify installation:
```bash
git --version
```

### 2. Miniconda
Download and install from [docs.conda.io/en/latest/miniconda.html](https://docs.conda.io/en/latest/miniconda.html).

Verify installation:
```bash
conda --version
```

### 3. Docker Desktop
Download and install from [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop).

After installation, open Docker Desktop and make sure it is running (whale icon in taskbar).

Verify installation:
```bash
docker --version
```

### 4. Visual Studio Code (recommended)
Download from [code.visualstudio.com](https://code.visualstudio.com/).

Recommended extensions:
- Python
- dbt Power User
- SQLFluff

---

## ⚙️ Setup Instructions

Follow these steps exactly in order.

### Step 1 — Clone the repository

```bash
git clone https://github.com/DarvinSures/health-data-pipeline.git
cd health-data-pipeline
```

---

### Step 2 — Create and activate the conda environment

```bash
conda create -n health-pipeline python=3.11 -y
conda activate health-pipeline
```

You should see `(health-pipeline)` at the start of your terminal prompt.

---

### Step 3 — Install Python dependencies

```bash
pip install -r requirements.txt
```

---

### Step 4 — Set up Google Cloud credentials

This pipeline reads patient data from a Google Sheet using a GCP Service Account.

**4a. Create a GCP Project**
1. Go to [console.cloud.google.com](https://console.cloud.google.com)
2. Click the project dropdown → **New Project**
3. Name it `health-data-pipeline` → **Create**

**4b. Enable APIs**
1. Go to **APIs & Services → Library**
2. Search for and enable **Google Sheets API**
3. Search for and enable **Google Drive API**

**4c. Create a Service Account**
1. Go to **APIs & Services → Credentials**
2. Click **Create Credentials → Service Account**
3. Name it `health-pipeline-sa` → **Create and Continue**
4. Skip optional steps → **Done**

**4d. Download the JSON Key**
1. Click on your service account
2. Go to **Keys** tab → **Add Key → Create New Key**
3. Select **JSON** → **Create**
4. Move the downloaded file to the `credentials/` folder
5. Rename it to `service_account.json`

**4e. Share the Google Sheet with the Service Account**
1. Open `credentials/service_account.json`
2. Copy the `client_email` value
3. Open your Google Sheet → **Share**
4. Paste the email → set permission to **Viewer** → **Send**

---

### Step 5 — Configure environment variables

Create a `.env` file in the project root:

```bash
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=health_db
DB_USER=admin
DB_PASSWORD=admin123
DB_SCHEMA=raw

# GCP
GCP_SERVICE_ACCOUNT_PATH=./credentials/service_account.json
GOOGLE_SHEET_NAME=patient_data
```

> ⚠️ Never commit the `.env` file or `credentials/` folder to GitHub. Both are already in `.gitignore`.

---

### Step 6 — Start PostgreSQL with Docker

Make sure Docker Desktop is running, then:

```bash
docker-compose up -d
```

Verify PostgreSQL is running:
```bash
docker ps
```

You should see `health_postgres` with status `Up`.

---

### Step 7 — Set up dbt

Navigate to the dbt project:
```bash
cd dbt_project/health_pipeline
```

Create `profiles.yml` in this folder:
```yaml
health_pipeline:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: admin
      password: admin123
      dbname: health_db
      schema: public
      threads: 4
```

Install dbt packages:
```bash
dbt deps
```

Test the database connection:
```bash
dbt debug
```

You should see `All checks passed`.

Go back to the project root:
```bash
cd ../..
```

---

### Step 8 — Set up Prefect Cloud (optional for scheduling)

1. Create a free account at [app.prefect.cloud](https://app.prefect.cloud)
2. Log in from your terminal:
```bash
prefect cloud login
```

---

## ▶️ Running the Pipeline

### Run the full pipeline

From the project root:
```bash
python orchestration/pipeline_flow.py
```

This runs all steps in order:
1. Ingest from Google Sheet → landing → raw
2. dbt run → staging → consumption (FHIR)
3. dbt tests → FHIR data quality checks

---

### Run individual steps

**Ingestion only:**
```bash
python ingestion/load_data.py
```

**dbt models only:**
```bash
cd dbt_project/health_pipeline
dbt run
```

**dbt tests only:**
```bash
cd dbt_project/health_pipeline
dbt test
```

---

### View data lineage and documentation

```bash
cd dbt_project/health_pipeline
dbt docs generate
dbt docs serve
```

This opens a browser with full lineage diagram and model documentation.

---

## ✅ Verify the Pipeline Ran Successfully

Connect to PostgreSQL:
```bash
docker exec -it health_postgres psql -U admin -d health_db
```

Check FHIR output:
```sql
SELECT id, full_name, gender, telecom, nationality
FROM consumption.fhir_patient
LIMIT 5;
```

---

## 🔐 Data Protection

PII fields are protected at the **staging layer** before any downstream model sees raw values:

| Field | Treatment |
|---|---|
| `first_name`, `last_name` | MD5 hashed |
| `email`, `phone_number` | MD5 hashed |
| `insurance_number` | MD5 hashed |
| `birth_date` | Generalised to year only |
| `address`, `full_name` | Masked (`****`) |

---

## 🧪 Data Quality Tests

FHIR-aligned tests run automatically on every pipeline execution:

| Test | Layer | Description |
|---|---|---|
| `unique` | Source | Patient ID must be unique |
| `not_null` | Source | Required FHIR fields must exist |
| `accepted_values` | Source | Gender must be male/female/other/unknown |
| `accepted_values` | Source | Marital status must follow FHIR value set |
| `fhir_telecom_valid` | Consumption | Telecom must have phone and email |
| `fhir_id_max_length` | Consumption | ID must be max 64 chars (FHIR spec) |
| `fhir_no_duplicate_patients` | Consumption | No duplicate patient IDs |
| `fhir_insurance_not_null` | Consumption | Insurance number must exist |
| `fhir_birth_date_not_in_future` | Source | Birth date cannot be in the future |

Tests configured with `severity: warn` flag data issues without stopping the pipeline.

---

## 📅 Scheduled Runs

The pipeline is deployed to **Prefect Cloud** and runs every **Monday at 06:00 UTC**.

To trigger a manual run:
```bash
prefect deployment run 'health-data-pipeline/health-pipeline-deployment'
```

Monitor runs at: [app.prefect.cloud](https://app.prefect.cloud)

---

## 🔄 Stopping and Restarting

**Stop PostgreSQL:**
```bash
docker stop health_postgres
```

**Start PostgreSQL:**
```bash
docker start health_postgres
```

**Restart everything from scratch:**
```bash
docker stop health_postgres
docker rm health_postgres
docker-compose up -d
cd dbt_project/health_pipeline && dbt run && cd ../..
python ingestion/load_data.py
```

---
