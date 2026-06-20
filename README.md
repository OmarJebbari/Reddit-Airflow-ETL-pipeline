# Reddit-Airflow-ETL-Pipeline

A scalable, production-ready **Extract-Transform-Load (ETL) pipeline** that ingests Reddit data from multiple subreddits via web scraping, orchestrates it with Apache Airflow, processes it with pandas, and writes Bronze parquet to a MinIO lakehouse while also exporting CSV snapshots.

---

## 🎯 Project Overview

This project demonstrates enterprise-grade data engineering practices by:

- **Extracting** real-time data directly from Reddit via web scraping (.json endpoint) with pagination
- **Transforming** raw JSON data into structured, analysis-ready formats
- **Loading** Bronze parquet to MinIO and CSV snapshots for local inspection
- **Orchestrating** the entire workflow using Apache Airflow with daily execution schedules
- **Containerizing** the application for local development with a lakehouse stack

**Current Status:** Local Docker-based pipeline with MinIO lakehouse, Great Expectations validation, dbt Gold models, Trino query layer, and Metabase BI.

---

## 🏗️ Architecture

```mermaid
%%{init: {
  "theme": "base",
  "look": "handDrawn",
  "themeVariables": {
    "fontFamily": "Fira Code, Segoe UI, monospace",
    "fontSize": "14px",
    "darkMode": true,
    "background": "#0D1117",
    "primaryColor": "#1B263B",
    "primaryTextColor": "#E6EDF3",
    "primaryBorderColor": "#58A6FF",
    "lineColor": "#79C0FF",
    "secondaryColor": "#102A43",
    "tertiaryColor": "#1A1F2E"
  },
  "flowchart": {
    "curve": "catmullRom",
    "htmlLabels": true
  }
}}%%
flowchart LR
    subgraph SRC[Signal Sources]
        R1["Reddit JSON Endpoint"]
        R2["Subreddits<br/>dataengineering, datascience, aws, azure, python"]
        R1 --> R2
    end

    subgraph ORCH[Orchestration Mesh - Airflow]
        SCH["Daily Scheduler"]
        DAG["DAG: elt_reddit_pipeline"]
        T1["reddit_extraction"]
        T2["validate_bronze_parquet"]
        T3["dbt_run_gold"]
        T4["load_gold_to_postgres"]
        SCH --> DAG --> T1 --> T2 --> T3 --> T4
    end

    subgraph PROC[Processing and Governance]
        P1["Pagination + Normalization<br/>requests + pandas"]
        P2["Data Quality Contract<br/>Great Expectations"]
        P3["Analytical Modeling<br/>dbt Silver and Gold"]
        P1 --> P2 --> P3
    end

    subgraph STORE[Lakehouse and Serving]
        C1["CSV Snapshots"]
        B1["MinIO Bronze"]
        B2["MinIO Silver"]
        B3["MinIO Gold"]
        PG1["PostgreSQL reddit_gold"]
        PG2["PostgreSQL Airflow Metadata"]
        C1 --- B1 --> B2 --> B3
        PG1 --- PG2
    end

    subgraph CONS[Consumption Layer]
        TR["Trino Federated SQL"]
        MB["Metabase Dashboards"]
        TR --> MB
    end

    R2 -. "JSON Pull" .-> T1
    T1 --> P1
    P1 --> C1
    P1 --> B1
    T2 --> P2
    T3 --> P3
    P3 --> B2
    P3 --> B3
    T4 --> PG1
    DAG --> PG2
    B3 --> TR
    PG1 --> MB

    classDef src fill:#2B1A16,stroke:#FF9E64,stroke-width:2px,color:#FFD9C0;
    classDef orch fill:#132236,stroke:#58A6FF,stroke-width:2px,color:#D7ECFF;
    classDef proc fill:#13291E,stroke:#7EE787,stroke-width:2px,color:#D9FFE0;
    classDef store fill:#231A34,stroke:#D2A8FF,stroke-width:2px,color:#F1E4FF;
    classDef cons fill:#2D260F,stroke:#E3B341,stroke-width:2px,color:#FFF0C2;

    class R1,R2 src;
    class SCH,DAG,T1,T2,T3,T4 orch;
    class P1,P2,P3 proc;
    class C1,B1,B2,B3,PG1,PG2 store;
    class TR,MB cons;
```

This architecture shows the full lakehouse path from Reddit scraping to Gold serving in PostgreSQL/Metabase.

Visual analysis:
- Layered design: source ingestion, orchestration, quality/modeling, storage, and BI consumption.
- Clear control points: validation (`validate_bronze_parquet`) and model build (`dbt_run_gold`) are explicit gates.
- Serving strategy: PostgreSQL `reddit_gold` supports dashboard consumption with stable downstream semantics.

---

## 🔄 Data Flow

```mermaid
%%{init: {
  "theme": "base",
  "look": "handDrawn",
  "themeVariables": {
    "fontFamily": "Fira Code, Segoe UI, monospace",
    "fontSize": "13px",
    "darkMode": true,
    "background": "#0D1117",
    "lineColor": "#79C0FF",
    "signalColor": "#79C0FF",
    "signalTextColor": "#E6EDF3",
    "actorBorder": "#58A6FF",
    "actorBkg": "#132236",
    "actorTextColor": "#E6EDF3",
    "labelBoxBkgColor": "#1A2638",
    "labelBoxBorderColor": "#58A6FF",
    "loopTextColor": "#E3B341"
  },
  "sequence": {
    "mirrorActors": false,
    "showSequenceNumbers": true
  }
}}%%
sequenceDiagram
    autonumber
    participant S as Airflow Scheduler
    participant D as DAG Controller
    participant X as Extraction Worker
    participant R as Reddit Endpoint
    participant B as Bronze Publisher
    participant Q as Quality Gate
    participant T as dbt Runtime
    participant P as Postgres Sink
    participant O as Observability

    S->>D: Trigger daily production run
    D->>X: Launch reddit_extraction
    loop Each subreddit and page cursor
        X->>R: GET top.json with t=year and after token
        R-->>X: Return JSON batch
    end
    X->>B: Emit normalized dataframe
    B->>B: Persist CSV and Bronze parquet
    D->>Q: Launch validate_bronze_parquet
    Q-->>D: Contract checks passed
    D->>T: Launch dbt_run_gold
    T->>T: Build silver and gold marts
    D->>P: Launch load_gold_to_postgres
    P->>P: Refresh reddit_gold table
    D->>O: Publish logs, SLA markers, metrics
    O-->>S: Run complete and auditable
```

The runtime sequence highlights scheduling, extraction, quality checks, dbt modeling, load, and observability.

Visual analysis:
- End-to-end run ordering is explicit and production-friendly.
- Pagination loop reflects realistic Reddit extraction behavior.
- Observability step captures operational traceability at run completion.

---

## 🧭 Operating Model

```mermaid
%%{init: {
  "theme": "base",
  "look": "handDrawn",
  "themeVariables": {
    "fontFamily": "Fira Code, Segoe UI, monospace",
    "fontSize": "14px",
    "darkMode": true,
    "background": "#0D1117",
    "primaryColor": "#132236",
    "primaryTextColor": "#E6EDF3",
    "primaryBorderColor": "#58A6FF",
    "lineColor": "#79C0FF"
  },
  "flowchart": {
    "curve": "natural"
  }
}}%%
flowchart TB
    subgraph ENG[Engineering Controls]
      C1["Containerized Runtime<br/>Docker Compose"]
      C2["Workflow Orchestration<br/>Airflow Celery Executor"]
      C3["Quality and Transform Contracts<br/>Great Expectations and dbt"]
      C1 --> C2 --> C3
    end

    subgraph PROD[Data Product]
      D1["Raw Ingestion<br/>Scraped Reddit Signals"]
      D2["Bronze to Gold Progression<br/>MinIO Lakehouse"]
      D3["Serving Model<br/>Postgres reddit_gold"]
      D1 --> D2 --> D3
    end

    subgraph CONS[Business Consumption]
      B1["Self Service SQL<br/>Trino"]
      B2["Executive Insight Layer<br/>Metabase"]
      B1 --> B2
    end

    subgraph KPI[Outcome Metrics]
      K1["Freshness: Daily SLA"]
      K2["Quality: Validation Gate"]
      K3["Trust: Reproducible Runs"]
      K4["Value: Decision Ready Data"]
    end

    C3 --> D1
    D3 --> B1
    C2 --> K1
    C3 --> K2
    C1 --> K3
    B2 --> K4

    classDef controls fill:#132236,stroke:#58A6FF,stroke-width:2px,color:#D7ECFF;
    classDef product fill:#13291E,stroke:#7EE787,stroke-width:2px,color:#D9FFE0;
    classDef consume fill:#2D260F,stroke:#E3B341,stroke-width:2px,color:#FFF0C2;
    classDef kpi fill:#231A34,stroke:#D2A8FF,stroke-width:2px,color:#F1E4FF;

    class C1,C2,C3 controls;
    class D1,D2,D3 product;
    class B1,B2 consume;
    class K1,K2,K3,K4 kpi;
```

This operating view maps engineering controls to business outcomes (freshness, trust, and decision readiness).

Visual analysis:
- Connects platform controls (Airflow, quality contracts, container runtime) to business KPI outcomes.
- Frames the pipeline as a data product, not just an ETL script.

---

## 🖼️ Runtime Evidence (From Exports)

![Airflow DAG Success Graph](assets/linkedin/exports/1.png)

This screenshot confirms the production DAG path and successful run states across all four tasks:
`reddit_extraction -> validate_bronze_parquet -> dbt_run_gold -> load_gold_to_postgres`.

![MinIO Bronze Parquet Object](assets/linkedin/exports/2.png)

This screenshot confirms Bronze lakehouse persistence in MinIO (`bronze/reddit/reddit_*.parquet`), matching the documented storage architecture.

### Metadata Layer Evidence

![Metadata Layer](assets/linkedin/exports/metadata.png)

This screenshot highlights metadata visibility for the platform layer and complements the DAG and storage runtime evidence.

---

## 🧱 Enterprise Features (Implemented)

- **Data Quality:** Great Expectations validation on Bronze parquet before promotion
- **Transformations:** dbt-core Gold models running via Airflow
- **Serving Layer:** Final Airflow task refreshes `reddit_gold` in PostgreSQL for BI tools
- **Query Layer:** Trino for SQL over MinIO parquet
- **BI:** Metabase dashboards on top of Trino
- **CI/CD:** GitHub Actions for linting and tests

---

## 📋 Prerequisites

Before you begin, ensure you have:

- **Docker & Docker Compose** (v20.10+) - [Install](https://docs.docker.com/get-docker/)
- **Python 3.9+** (for local development)
- **Git** - [Install](https://git-scm.com/)
- **MinIO credentials** (see Configuration section)

---

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/OmarJebbari/Reddit-Airflow-ETL-pipeline.git
cd Reddit-Airflow-ETL-pipeline
```

### 2. Environment Configuration

Create or verify the `airflow.env` file with your credentials:

```bash
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:postgres@postgres:5432/airflow_reddit
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
```

### 3. Build and Start Services

```bash
# Build the custom Airflow image
docker-compose build

# Start all services (Airflow, PostgreSQL, Redis)
docker-compose up -d

# Verify services are running
docker-compose ps
```

### 4. Access Airflow UI

Open your browser and navigate to:

```
http://localhost:8080
```

- **Default username:** admin
- **Default password:** admin

### 5. Trigger the Pipeline

In the Airflow UI:

1. Navigate to **DAGs** → **elt_reddit_pipeline**
2. Click **Trigger DAG** to execute immediately
3. Monitor task execution in real-time

### 6. View Results

Once the task completes successfully, check the output:

```bash
ls -lh data/output/
# Output: reddit_YYYYMMDD.csv
```

---

## 📁 Project Structure

```
Reddit-Airflow-ETL-Pipeline/
├── dags/
│   ├── reddit_dag.py              # Airflow DAG definition
│   └── __pycache__/
├── etls/
│   ├── reddit_etl.py              # Extract, Transform, Load functions
│   └── __pycache__/
├── pipelines/
│   ├── reddit_pipeline.py          # Main pipeline orchestration
│   └── __pycache__/
├── utils/
│   ├── constants.py               # Global constants & paths
│   └── __pycache__/
├── config/
│   └── config.conf                # Configuration file
├── data/
│   ├── input/                     # Input data directory
│   └── output/                    # Output CSV files
├── logs/
│   ├── dag_id=elt_reddit_pipeline/  # DAG execution logs
│   ├── dag_processor_manager/     # Processor logs
│   └── scheduler/                 # Scheduler logs
├── plugins/                       # Airflow custom plugins (expandable)
├── tests/                         # Unit & integration tests
├── assets/
│   └── linkedin/
│       └── exports/               # LinkedIn-ready diagrams and runtime screenshots
├── Dockerfile                     # Custom Airflow image
├── docker-compose.yml             # Service orchestration
├── airflow.env                    # Environment variables
├── requirements.txt               # Python dependencies
└── README.md                      # This file
```

---

## ⚙️ Configuration

### `config/config.conf`

Key configuration parameters:

| Section | Parameter | Value | Purpose |
|---------|-----------|-------|---------|
| `database` | host | postgres | PostgreSQL host for Airflow metadata |
| `database` | port | 5432 | PostgreSQL port |
| `file_paths` | output_path | `/opt/airflow/data/output` | CSV output directory |
| `api_keys` | reddit_* | N/A | Placeholder (using web scraping, not OAuth) |
| `minio` | MINIO_* | N/A | MinIO credentials and bucket settings |
| `etl_settings` | batch_size | 100 | Posts per extraction |
| `etl_settings` | log_level | info | Logging verbosity |

### Environment Variables

```bash
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:postgres@postgres:5432/airflow_reddit
AIRFLOW__CORE__LOAD_EXAMPLES=False
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=YOUR_MINIO_ACCESS_KEY
MINIO_SECRET_KEY=YOUR_MINIO_SECRET_KEY
MINIO_BRONZE_BUCKET=bronze
MINIO_BRONZE_PREFIX=reddit
```

---

## 📊 Data Schema

The extracted and transformed data includes:

```python
{
    'id': str,                    # Unique post identifier
    'title': str,                 # Post title
    'text': str,                  # Post body content
    'score': int,                 # Upvote score
    'num_comments': int,          # Comment count
    'author': str,                # Post author username
    'created_utc': float,         # Unix timestamp
    'url': str,                   # Direct link
    'upvote_ratio': float,        # 0.0-1.0 ratio
    'over_18': bool,              # NSFW flag
    'edited': bool,               # Edit flag
    'spoiler': bool,              # Spoiler flag
    'stickied': bool,             # Stickied flag
    'subreddit': str              # Source subreddit
}
```

---

## 🔧 Development

### Local Development Setup

```bash
# Create virtual environment
python -m venv venv

# Activate (Windows)
venv\Scripts\activate
# Activate (Linux/Mac)
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Running Tests

```bash
# Execute test suite
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=etls --cov=pipelines
```

### Debugging

View logs from a failed DAG run:

```bash
# Stream logs from Airflow
docker-compose logs -f webserver

# View scheduler logs
docker-compose logs -f scheduler
```

---

## 📦 Technologies Used

| Technology | Version | Purpose |
|-----------|---------|---------|
| **Apache Airflow** | 2.7.1 | Workflow orchestration & scheduling |
| **Python** | 3.9 | Core programming language |
| **Pandas** | Latest | Data transformation & cleaning |
| **PostgreSQL** | 12 | Airflow metadata database |
| **Redis** | Latest | Message broker & caching |
| **MinIO** | Latest | Object storage for lakehouse |
| **Great Expectations** | Latest | Data quality validation |
| **dbt-core** | Latest | Transformations (Silver to Gold) |
| **Trino** | Latest | SQL query engine on MinIO |
| **Metabase** | Latest | BI dashboards |
| **Docker** | 20.10+ | Containerization |
| **Requests** | Latest | HTTP library for API calls |

---

## 🎯 DAG Specifications

**DAG ID:** `elt_reddit_pipeline`

| Parameter | Value |
|-----------|-------|
| Owner | Omar Jebbari |
| Schedule | Daily (`@daily`) |
| Start Date | 2026-04-05 |
| Catchup | Disabled |
| Tags | reddit, elt, pipeline |
| Tasks | 4 (reddit_extraction, validate_bronze_parquet, dbt_run_gold, load_gold_to_postgres) |
| Timeout | Default (no limit) |

### Task Parameters

**reddit_extraction:**
- **Operator:** PythonOperator
- **Subreddit:** dataengineering, datascience, aws, azure, python
- **Time Filter:** year (top posts from last year)
- **Limit:** 100 per request
- **Max Posts:** 1000 per subreddit
- **File Format:** `reddit_YYYYMMDD.csv` + MinIO Bronze parquet + Postgres `reddit_gold`

**validate_bronze_parquet:**
- **Operator:** PythonOperator
- **Purpose:** Apply data quality checks before downstream models

**dbt_run_gold:**
- **Operator:** BashOperator
- **Purpose:** Build Silver and Gold dbt models

**load_gold_to_postgres:**
- **Operator:** PythonOperator
- **Purpose:** Load latest Bronze parquet into PostgreSQL table `reddit_gold` with replace strategy

---

## 🐳 Docker Compose Services

```yaml
Services:
  ├── postgres          # Airflow metadata database
  ├── redis            # Message broker
    ├── minio            # Lakehouse storage (ports 9000/9001)
    ├── airflow-init     # DB init and admin user
    ├── airflow-webserver # Airflow UI (port 8080)
    ├── airflow-scheduler # DAG scheduler
    ├── airflow-worker   # Task executor
    ├── metabase         # BI UI (port 3000)
    └── trino            # SQL query engine (port 8081)
```

Start specific service:

```bash
docker-compose up -d postgres redis minio airflow-webserver airflow-scheduler airflow-worker metabase trino
```

---

## 🚨 Troubleshooting

### Service Won't Start

```bash
# Check logs
docker-compose logs -f

# Rebuild images
docker-compose build --no-cache

# Reset everything
docker-compose down -v
docker-compose up -d
```

### DAG Not Appearing

- Verify DAG file syntax: `python dags/reddit_dag.py`
- Check DAG folder mounted in compose: `./dags:/opt/airflow/dags`
- Restart scheduler: `docker-compose restart scheduler`

### Connection Errors

- Verify services running: `docker-compose ps`
- Check network: `docker network ls`
- Reset containers: `docker-compose restart`

---

## 📝 Logging

All logs are stored in the `logs/` directory with structure:

```
logs/
├── dag_id=elt_reddit_pipeline/
│   ├── run_id=manual__<timestamp>/
│   │   └── task_id=reddit_extraction/
│   │       ├── attempt=1.log
│   │       └── attempt=2.log
├── dag_processor_manager/
└── scheduler/
```

View logs programmatically:

```python
from airflow.utils.log.logging_mixin import LoggingMixin
logger = LoggingMixin().log
logger.info("Pipeline started")
```

---

## 🔐 Security Best Practices

1. **Never commit secrets** - Use `airflow.env` and `.gitignore`
2. **Rotate credentials** - Update PostgreSQL password in production
3. **Use secrets backend** - Airflow Variables or environment variables
4. **Network isolation** - Run containers on isolated networks
5. **Monitor access** - Review Airflow logs regularly

---

## 📄 License

This project is open source and available under the **MIT License**.

---

## 👤 Author

**Omar Jebbari**  
Data Engineer | ETL Specialist  

- GitHub: [@OmarJebbari](https://github.com/OmarJebbari)
- LinkedIn: [OMAR JEBBARI](https://www.linkedin.com/in/omar-jebbari-00b30b269/)

---


---

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📞 Support

For issues, questions, or suggestions:

- **Open an Issue** OmarJebbari GitHub
- **Email:** jebbariomar1@gmail.com
- **Discussions:** GitHub Discussions tab

---

## 📚 References

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Reddit JSON API Endpoint](https://www.reddit.com/dev/api#GET_top) - Web scraping endpoint
- [PRAW Documentation](https://praw.readthedocs.io/) - Official Reddit API wrapper (future reference)
- [Docker Documentation](https://docs.docker.com/)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Requests Library Documentation](https://requests.readthedocs.io/)

---

**Last Updated:** April 6, 2026  
**Version:** 1.0.0  
**Status:** Active Development

