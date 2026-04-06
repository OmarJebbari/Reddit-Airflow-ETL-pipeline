# Reddit-Airflow-ETL-Pipeline

A scalable, production-ready **Extract-Transform-Load (ETL) pipeline** that ingests Reddit data from the r/dataengineering subreddit via web scraping, orchestrates it with Apache Airflow, processes it with pandas, and outputs clean CSV files for analysis.

---

## 🎯 Project Overview

This project demonstrates enterprise-grade data engineering practices by:

- **Extracting** real-time data directly from Reddit via web scraping (.json endpoint)
- **Transforming** raw JSON data into structured, analysis-ready formats
- **Loading** cleaned data into CSV files for downstream consumption
- **Orchestrating** the entire workflow using Apache Airflow with daily execution schedules
- **Containerizing** the application for local development and eventual cloud deployment

**Current Status:** Local Docker-based pipeline with MinIO lakehouse, Great Expectations validation, dbt Gold models, Trino query layer, and Metabase BI.

---

## 🏗️ Architecture

```mermaid
graph TB
    subgraph Reddit["Reddit API"]
        A["r/dataengineering<br/>Top Posts"]
    end
    
    subgraph Airflow["Apache Airflow Orchestration"]
        DAG["DAG: elt_reddit_pipeline"]
        TASK["Task: reddit_extraction<br/>(Daily @ UTC)"]
    end
    
    subgraph Processing["ETL Pipeline"]
        EXTRACT["Extract<br/>scraping"]
        TRANSFORM["Transform<br/>pandas"]
        LOAD["Load<br/>CSV"]
    end
    
    subgraph Storage["Data Storage"]
        CSV["CSV Output<br/>/data/output/"]
        DB["PostgreSQL<br/>Airflow Metadata"]
    end
    
    subgraph Future["Future: Azure"]
        S3["Azure Blob Storage<br/>Data Lake"]
        RDS["Azure SQL Database<br/>Database"]
    end
    
    Reddit --> A
    A -->|HTTP Requests| TASK
    Airflow --> DAG
    DAG --> TASK
    TASK -->|PythonOperator| EXTRACT
    EXTRACT -->|Raw Data| TRANSFORM
    TRANSFORM -->|Structured Data| LOAD
    LOAD -->|CSV Files| CSV
    LOAD -->|Metadata| DB
    
    CSV -.->|Future| S3
    DB -.->|Future| RDS
    
    style Reddit fill:#FF4500
    style Airflow fill:#017CEE
    style Processing fill:#4CAF50
    style Storage fill:#FFC107
    style Future fill:#9C27B0,opacity:0.6
```

---

## 🔄 Data Flow

```mermaid
sequenceDiagram
    participant Scheduler as Airflow Scheduler
    participant DAG as DAG Dispatcher
    participant Extractor as Reddit Extractor
    participant Reddit as Reddit API
    participant Transformer as Data Transformer
    participant Loader as CSV Loader
    participant FileSystem as File System
    
    Scheduler->>DAG: Trigger Daily (UTC)
    DAG->>Extractor: Execute Task
    Extractor->>Reddit: GET /r/dataengineering/top.json
    Reddit-->>Extractor: JSON Response (100 posts)
    Extractor->>Transformer: Pass Raw Data
    Transformer->>Transformer: Parse & Normalize
    Transformer->>Loader: Return DataFrame
    Loader->>FileSystem: Write CSV
    FileSystem-->>Loader: Confirm Write
    Loader-->>DAG: Task Complete
    DAG-->>Scheduler: Log Status
```

---

## 🧱 Enterprise Features (Implemented)

- **Data Quality:** Great Expectations validation on Bronze parquet before promotion
- **Transformations:** dbt-core Gold models running via Airflow
- **Query Layer:** Trino for SQL over MinIO parquet
- **BI:** Metabase dashboards on top of Trino
- **CI/CD:** GitHub Actions for linting and tests

---

## 📋 Prerequisites

Before you begin, ensure you have:

- **Docker & Docker Compose** (v20.10+) - [Install](https://docs.docker.com/get-docker/)
- **Python 3.9+** (for local development)
- **Git** - [Install](https://git-scm.com/)
- **Reddit API Access** (see Configuration section)

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
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql://postgres:postgres@postgres:5432/airflow_reddit
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False
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
| `azure` | azure_* | N/A | Azure credentials for future cloud deployment |
| `etl_settings` | batch_size | 100 | Posts per extraction |
| `etl_settings` | log_level | info | Logging verbosity |

### Environment Variables

```bash
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql://postgres:postgres@postgres:5432/airflow_reddit
AIRFLOW__CORE__LOAD_EXAMPLES=False
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
    'stickied': bool              # Stickied flag
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
| Tasks | 1 (reddit_extraction) |
| Timeout | Default (no limit) |

### Task Parameters

**reddit_extraction:**
- **Operator:** PythonOperator
- **Subreddit:** dataengineering
- **Time Filter:** day (top posts from last 24 hours)
- **Limit:** 100 posts
- **File Format:** `reddit_YYYYMMDD.csv`

---

## 🐳 Docker Compose Services

```yaml
Services:
  ├── postgres          # Airflow metadata database
  ├── redis            # Message broker
    ├── airflow-init      # DB init and admin user
  ├── webserver        # Airflow UI (port 8080)
  ├── scheduler        # DAG scheduler
    ├── worker           # Task executor
    ├── metabase         # BI UI (port 3000)
    └── trino            # SQL query engine (port 8081)
```

Start specific service:

```bash
docker-compose up -d postgres redis
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
[GitHub](https://github.com/OmarJebbari) | [LinkedIn](https://linkedin.com/in/your-profile)

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

- **Open an Issue** onjebbari GitHub
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

**Last Updated:** April 5, 2026  
**Version:** 1.0.0  
**Status:** Active Development

