# End-to-End Data Engineering Pipeline

Production-style data engineering pipeline designed to handle real-world data ingestion, data quality validation and analytics-ready modeling.

---

## 🎯 Project Purpose

This project was built to simulate how data is handled in a professional environment.

The objective is not to showcase isolated scripts, but to design a complete and reliable data pipeline, covering the full lifecycle of data: from ingestion to analytics, with a strong focus on data quality, structure and automation.

---

## 🧠 What This Project Demonstrates

- Automated ingestion from an external data source  
- Data cleaning and normalization  
- Explicit data quality checks  
- Structured storage for analytics use cases  
- End-to-end orchestration of the pipeline  
- Reproducible execution using Docker  

The project is intentionally oriented toward robustness and maintainability rather than one-off data processing.

---

## 🧱 Architecture Overview

The pipeline follows a layered architecture commonly used in production systems:

- **Raw layer**: unmodified data ingested from the source  
- **Staging layer**: cleaned and standardized data  
- **Analytics layer**: analytics-ready models  

Each step is isolated, traceable and orchestrated to ensure data reliability.

---

## 🛠️ Tech Stack

- **Python 3.11**
- **PostgreSQL 15**
- **Apache Airflow**
- **dbt**
- **Docker & Docker Compose**

---

## 🚀 How to Run the Project

### Prerequisites
- Docker
- Docker Compose

### Start the pipeline
```bash
docker-compose up --build


Once running:

Airflow UI is available for pipeline monitoring

The database contains raw, staging and analytics schemas

🔍 Data Quality

Data quality is treated as a first-class concern.

The pipeline includes checks for:

missing values

duplicates

invalid ranges

If a quality rule fails, the pipeline stops, preventing unreliable data from propagating further.

📌 Why This Project Exists

This project was created to demonstrate practical data engineering skills:

designing reliable pipelines

structuring data for analytics

automating workflows

thinking in terms of systems rather than scripts

It reflects how data engineering work is approached in real-world teams.

👤 Author

Ilyesse El Adaoui
Data Engineer

GitHub: https://github.com/Ilyesse-soc

LinkedIn: https://www.linkedin.com/in/ilyesse-eladaoui/
