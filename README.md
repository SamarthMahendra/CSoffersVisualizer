# 🧠 JobStats.fyi — Real-Time Interview Analytics Dashboard

> **JobStats.fyi** is a full-stack data product providing real-time analytics on the competitive CS recruiting landscape for internships and new-grad roles.  
> Built by students, it transforms messy, user-submitted community data into actionable metrics such as conversion rates, time-to-stage averages, and live hiring trends.

This README covers the **project architecture**, **data pipeline**, and **technical challenges solved**.

---
<img width="3024" height="1662" alt="image" src="https://github.com/user-attachments/assets/bf118cde-e8da-4ec2-b430-334ac66b8dd8" />
<img width="3024" height="1654" alt="image" src="https://github.com/user-attachments/assets/5b7a4688-0b77-4986-9040-bd3d8e7f4014" />
<img width="3020" height="1650" alt="image" src="https://github.com/user-attachments/assets/da2ed7ea-4b5b-41f0-8964-d6180a4b1357" />




## ⚙️ Core Architecture & Tech Stack

| **Component** | **Technology** | **Primary Role** |
|----------------|----------------|------------------|
| **Data Pipeline** | Python, OpenAI API, Cron Jobs | Scrapes raw messages, classifies content, cleans data, and handles batch processing/normalization. |
| **Backend API** | Flask (Python) | Central hub that runs complex MongoDB aggregation pipelines to calculate real-time metrics (funnel, conversion, time averages) based on user filters. |
| **Database** | MongoDB (PyMongo) | Stores raw and processed data, including structured submissions, metadata, and active sessions. |
| **Frontend UI** | HTML/CSS, Chart.js, D3.js | Renders the dashboard; D3.js powers Heatmaps & Timelines, while Chart.js handles Hiring Trends. |
| **Observability** | Datadog RUM & APM | Monitors frontend performance, session analytics, and Python batch job health. |

---

## 💧 Data Reliability Pipeline

The dashboard’s accuracy relies on turning **unstructured, user-generated text** into reliable structured data.  
This is achieved through a multi-stage, **batch-scheduled pipeline**.

### 1. Ingestion & Classification (Auto-Parsing)

- **Raw Data Source:**  
  Scrapes messages from CS community platforms (Discord) using Python scripts scheduled via Cron.  
- **OpenAI Classification:**  
  Uses GPT-4o + Pydantic schemas for **structured extraction** (`company`, `stage`, `spam`) from unstructured text.  
  → Outperforms traditional regex/NLP approaches.

---

### 2. Data Cleaning & Integrity Checks

| **Process** | **Goal** | **Implementation** |
|--------------|----------|--------------------|
| **Canonicalization** | Unify inconsistent company names. | `update_company_names.py` maintains a canonical map (e.g. `"JPMC"`, `"JP Morgan"` → `"JPMorgan Chase"`). |
| **Duplicate Checking** | Prevent multiple identical submissions skewing results. | Before insertion, check for duplicates on `(author, company, stage)`. |
| **Batch Processing** | Handle large data volumes efficiently. | Process 25–50 messages per API call; use `insert_many` for high-throughput writes. |
| **Backfilling Missing Stages** | Complete partial candidate journeys. | If “Onsite” → “Offer” skips stages, pipeline auto-fills synthetic “OA” and “Phone/R1” data for accurate funnels. |

---

### 3. Real-Time Analytics & Aggregation

The **Flask API** computes all metrics dynamically using MongoDB aggregation pipelines.

- **Conversion Metrics:**  
  Groups by `(author, company)` and counts stage transitions (e.g., `OA → Onsite`).
- **Time-Series Reconstruction:**  
  Rebuilds candidate timelines to compute average days between stages.
- **Hiring Trends Smoothing:**  
  Applies a **7-day moving average** to smooth out noise in daily submissions.

---

## 🚀 Impact & Validation

Achieving **100–300 daily active sessions (DAS)** validates both **product-market fit** and **technical robustness**.

### 1. Product-Market Fit & User Stickiness

- **Validation of Value:**  
  Consistent DAU indicates users rely on JobStats for transparency into the job market.
- **Data Flywheel Effect:**  
  More users → more submissions → fresher data → higher retention.

### 2. Technical Architecture Stress Test

- **Aggregation Performance:**  
  Proves Flask + MongoDB can handle real-time user queries efficiently.  
- **Data Reliability:**  
  Sustained traffic confirms stability of automated scraping and classification jobs.  
- **Live Session Management:**  
  Validates low-latency WebSocket API powering the live “👁 X online” counter.

---

## 🎯 Key Learnings & Highlights

- **Datadog Integration:**  
  Real-time RUM/APM monitoring for batch jobs and frontend latency.
- **D3.js Visualizations:**  
  Enables fine-grained control for dense, multi-dimensional data like conversion heatmaps.
- **Scalable Aggregations:**  
  Efficient, on-the-fly MongoDB pipelines for user-dependent filters.


**Visit:** [https://www.jobstats.fyi](https://www.jobstats.fyi)  
**Status:** ~14K+ submissions • 3.9K+ candidates • 1.4K+ companies
"""


<img width="1750" height="1224" alt="image" src="https://github.com/user-attachments/assets/910acfb8-2f91-42cd-84e8-92c19d6cb820" />





