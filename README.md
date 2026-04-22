# 🌤️ Odisha Weather Intelligence

<div align="center">

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat-square&logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Streaming-231F20?style=flat-square&logo=apachekafka&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-Orchestration-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-336791?style=flat-square&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?style=flat-square&logo=docker&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?style=flat-square&logo=powerbi&logoColor=black)

**A real-time data engineering pipeline that streams, transforms, and visualizes live weather data for 7 cities across Odisha, India.**

</div>

---

## 📌 Project Overview

This project builds an end-to-end **real-time weather data pipeline** using industry-standard data engineering tools. Live weather data is fetched from the OpenWeatherMap API, streamed through Apache Kafka, transformed with custom business logic, stored in PostgreSQL, orchestrated by Apache Airflow, and visualized in a Power BI dashboard — all running inside Docker containers.

### Cities Monitored
> Balasore · Bhubaneswar · Brahmapur · Cuttack · Puri · Rourkela · Sambalpur

---

## 🏗️ Architecture

```
OpenWeatherMap API
        │
        ▼
 weather_producer.py          ← Fetches live weather data
        │
        ▼  Kafka Topic: weather_data
  Apache Kafka                ← Streams data in real time (Docker)
        │
        ▼
 kafka_to_postgres.py         ← Consumes from Kafka
        │
        ├── extract.py        ← Parses raw API response
        ├── transform.py      ← Cleans data + severity scoring
        └── load.py           ← Writes to PostgreSQL
                │
                ▼
          PostgreSQL           ← Stores clean weather records
                │
                ▼
      Apache Airflow           ← Orchestrates and schedules pipeline
                │
                ▼
         Power BI              ← Live dashboard with auto-refresh
```

---

## 📂 Project Structure

```
odisha-weather-intelligence/
├── consumer/
│   └── kafka_to_postgres.py    # Kafka consumer → PostgreSQL
├── dags/
│   └── weather_etl_dag.py      # Airflow DAG definition
├── dashboard/
│   └── odisha_dashboard.pbix   # Power BI report
├── plugins/
│   ├── extract.py              # API response parser
│   ├── load.py                 # DB insert logic
│   └── transform.py            # Cleaning + severity score
├── producer/
│   └── weather_producer.py     # API → Kafka publisher
├── sql/
│   └── init.sql                # PostgreSQL schema
├── .env.example                # Environment variable template
├── docker-compose.yml          # All services in one file
├── requirements.txt
└── README.md
```

---

## ⚙️ Tech Stack

| Layer | Tool | Purpose |
|-------|------|---------|
| Data Source | OpenWeatherMap API | Live weather data |
| Streaming | Apache Kafka | Real-time message queue |
| Transformation | Python + Pandas | Clean, enrich, score |
| Orchestration | Apache Airflow | Schedule and monitor |
| Storage | PostgreSQL | Persistent records |
| Containerization | Docker + Compose | Local dev environment |
| Visualization | Microsoft Power BI | Live dashboard |

---

## ⚠️ Severity Score Logic

Custom scoring in `plugins/transform.py`:

```python
# Weather condition codes
if condition_code >= 500:    score += 40   # Rain
elif condition_code >= 300:  score += 20   # Drizzle
elif condition_code >= 200:  score += 40   # Thunderstorm

# Wind speed (kph)
if wind >= 50:   score += 30
elif wind >= 30: score += 20
elif wind >= 15: score += 10
else:            score += 5

# Humidity
if humidity >= 85:   score += 20
elif humidity >= 70: score += 10
elif humidity >= 50: score += 5
```

**Live DB output:**

| City | Severity Score |
|------|---------------|
| Puri | 40 |
| New Delhi | 35 |
| Balasore | 25 |
| Cuttack | 25 |
| Brahmapur | 20 |
| Rourkela | 15 |
| Sambalpur | 15 |

---

## 🗃️ Database Schema

```sql
CREATE TABLE public.weather_clean (
    id              SERIAL PRIMARY KEY,
    city_id         INTEGER,
    city_name       VARCHAR(100),
    temperature_c   FLOAT,
    feels_like_c    FLOAT,
    humidity_pct    FLOAT,
    pressure_hpa    FLOAT,
    wind_speed_kph  FLOAT,
    visibility_km   FLOAT,
    condition       VARCHAR(100),
    condition_code  INTEGER,
    severity_score  FLOAT,
    recorded_at     TIMESTAMP,
    loaded_at       TIMESTAMP
);
```

---

## 🚀 How to Run

### 1. Clone the repo
```bash
git clone https://github.com/raj98612002/odisha-weather-intelligence.git
cd odisha-weather-intelligence
```

### 2. Set up environment variables
```bash
cp .env.example .env
# Fill in your API key and DB credentials
```

### 3. Start all services
```bash
docker-compose up -d
```

### 4. Run the pipeline
```bash
python producer/weather_producer.py     # Terminal 1
python consumer/kafka_to_postgres.py    # Terminal 2
```

### 5. Open Power BI
Open `dashboard/odisha_dashboard.pbix` and click **Refresh**

---

## 📊 Dashboard Features

- KPI Cards — Avg Temp, Humidity, Max Wind, Heat Index, Cities Count
- Temperature by City (ranked bar chart)
- Severity Score per city
- Sky Conditions donut chart (Clouds / Clear / Haze)
- Heat Index and Wind Speed rankings
- City filter buttons for all 7 cities

---

## 🌱 What I Learned

- Real-time streaming with Apache Kafka
- Workflow orchestration with Apache Airflow DAGs
- Containerizing multi-service apps with Docker Compose
- Writing ETL pipelines in Python
- Connecting PostgreSQL to Power BI for live dashboards
- Safe secret management with .env files

---

<div align="center">
Built by <strong>Biswajit Panda</strong> · Odisha, India 🌊
</div>
