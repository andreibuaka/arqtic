# Arqtic Weather Pipeline

A data pipeline that pulls weather data for Toronto, processes it with quality gates and enrichment, and serves an interactive dashboard — deployed to GCP via Terraform.

**Arctiq Take-Home Assessment** | February 2026

## Quick Start

```bash
# Prerequisites: Python 3.14+, uv, graphviz (brew install graphviz)

# 1. Setup
make setup

# 2. Run the pipeline
make run-pipeline

# 3. Launch the dashboard
make run-dashboard
# → http://localhost:8501
```

## Architecture

![Architecture](architecture.png)

| Component | Technology | Why |
|-----------|------------|-----|
| Data Source | Open-Meteo API | Free, no API key, WMO-backed |
| Data Tool | DuckDB + Parquet | Embedded SQL, zero-config, GCS-transparent |
| Quality Gate | Pandera | Schema validation stops bad data |
| Dashboard | Streamlit + Plotly | Interactive drill-down |
| Forecasting | Prophet | Time-series with uncertainty bands |
| IaC | Terraform | GCS, Cloud Run, Scheduler, IAM |
| CI/CD | GitHub Actions | Lint/test on PR, deploy on merge |

## Pipeline: Extract → Validate → Transform → Load

- **Extract**: 3 years historical daily + 14-day hourly forecast from Open-Meteo
- **Validate**: Pandera schema checks (temperature range, non-negative wind, etc.)
- **Transform**: WMO codes, thermal comfort labels, anomaly detection, historical comparison
- **Load**: Parquet files to `./data/` (local) or `gs://bucket` (cloud)

## Dashboard Tabs

| Tab | What it shows |
|-----|--------------|
| Right Now | Hero weather, comfort advice, rain/UV alerts, hourly timeline |
| Trends | Plotly time-series with range selectors and anomaly markers |
| Forecast | 30-day Prophet prediction with uncertainty bands |
| Data Quality | Freshness, validation status, anomaly log, statistics |

## Testing

```bash
make test    # 29 pytest tests
make lint    # ruff lint + format check
```

## Deployment

```bash
# Requires: gcloud CLI, Terraform, Docker

# 1. Build Docker image
make docker-build

# 2. Deploy to GCP
make deploy
```

## Project Structure

```
arqtic/
├── config.py                 # Central configuration
├── pipeline/
│   ├── extract.py           # Open-Meteo API extraction
│   ├── quality.py           # Pandera schema validation
│   ├── transform.py         # Enrichment and computed columns
│   ├── load.py              # Parquet output
│   └── run.py               # Pipeline orchestrator
├── dashboard/
│   ├── app.py               # Streamlit main app
│   └── components/          # Tab renderers
├── forecast/
│   └── predict.py           # Prophet wrapper
├── terraform/               # GCP infrastructure
├── tests/                   # pytest suite
├── notebook/
│   └── presentation.ipynb   # Mission presentation
├── Dockerfile               # Multi-stage build
├── Makefile                 # Developer commands
└── pyproject.toml           # Dependencies (uv)
```
