# Apache Airflow DAGs

This repository contains Apache Airflow DAGs used for orchestrating data workflows such as file processing, API ingestion, and ETL pipelines.

## 📁 Project Structure

```bash
airflow-dags/
├── dags/ # Airflow DAG definitions
├── plugins/ # Custom plugins (optional)
├── requirements.txt # Python dependencies
├── .gitignore
└── README.md
```

## 🚀 Getting Started

## Prerequisites
- Python 3.9+, pip, python3-venv
- Apache Airflow 2.x
- WSL / Linux / macOS

## Setup (Local WSL)

```bash
python3 -m venv airflow_env
source airflow_env/bin/activate
```

## Install Apache Airflow:

1. Set Airflow home
```bash
export AIRFLOW_HOME=~/airflow
```

2. Install Airflow (example: 2.8.x):
```bash
pip install apache-airflow[EXTRAS]==AIRFLOW_VERSION --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-AIRFLOW_VERSION/constraints-PYTHON_VERSION.txt"
```

3. Start Airflow:
```bash
airflow standalone
```

## Access UI:
```bash
http://localhost:8080
```
