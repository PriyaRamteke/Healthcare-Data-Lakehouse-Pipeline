# Healthcare Data Lakehouse Pipeline

An end-to-end data engineering pipeline built on **Databricks** using the **Medallion Architecture** (Bronze → Silver → Gold) for healthcare analytics. The pipeline ingests raw patient, lab, diagnosis, and outcome data, applies cleaning and transformations, produces business-level aggregations, and exports results to **Google BigQuery**.

---

## Project Description

### Overview

This project implements a **cloud-native data lakehouse** for a healthcare organization that manages patient admissions, laboratory diagnostics, and clinical outcomes. The pipeline automates the full data lifecycle — from raw CSV ingestion through quality-checked transformations to business-ready analytics — using Databricks on Google Cloud Platform.

### Problem Statement

Healthcare organizations generate large volumes of fragmented data across patient records, lab systems, and clinical outcome tracking. Without a unified data platform, analysts face challenges such as:

- **Data silos** — Patient demographics, lab results, diagnoses, and outcomes stored in separate CSV files with inconsistent formats
- **Poor data quality** — Unvalidated ages, inconsistent date formats (`dd_MM_yyyy`), unordered lab reference ranges, and duplicate records
- **No analytics-ready layer** — Raw data requires significant manual preparation before it can answer business questions like revenue by disease, recovery rates, or high-risk patient identification

### Solution

This pipeline addresses these challenges through a **3-layer Medallion Architecture**:

1. **Bronze Layer** — Ingests 4 raw CSV files (patients, labs, diagnoses, outcomes) with explicit schema enforcement and persists them as Delta tables for reliable, versioned storage
2. **Silver Layer** — Applies data cleaning (deduplication, date parsing, type casting, range normalization), joins all entities into a unified `silver_table`, and runs automated data quality checks flagging invalid ages, costs, and null IDs
3. **Gold Layer** — Produces 8 business-level aggregation tables covering revenue analysis, recovery rates, patient risk profiling, admission trends, and lab test volume metrics

The Gold tables are then exported to **Google BigQuery** for integration with downstream BI tools and dashboards.

### Key Features

- **Medallion Architecture** — Clean separation of raw, cleaned, and business data layers using Delta Lake
- **Automated Data Quality** — Built-in DQ checks with status flags (`VALID`, `INVALID_AGE`, `INVALID_COST`, `NULL_PATIENT_ID`)
- **8 Business Analytics Tables** — Pre-computed aggregations for revenue, recovery rates, risk analysis, admission summaries, and lab trends
- **Orchestrated Workflow** — Databricks Job with 3 sequential tasks completing in ~91 seconds
- **Cloud-Native on GCP** — Serverless compute, Unity Catalog Volumes for storage, BigQuery for analytics export
- **Delta Lake Throughout** — ACID transactions, schema enforcement, and time travel across all layers

### Business Value

| Metric                        | Insight Delivered                                                    |
| ----------------------------- | -------------------------------------------------------------------- |
| Revenue by Disease             | Identifies highest-revenue diagnoses (e.g., Hypertension: $163K)    |
| Recovery Rate                  | Tracks recovery performance per diagnosis (e.g., Asthma: 47%)       |
| High-Risk Patient Count        | Flags patients aged >60 per diagnosis for care prioritization        |
| Top 5 Treatment Costs          | Surfaces costliest patient-diagnosis combinations for cost control   |
| Admission Summary              | Tracks admission date ranges and average stay duration per diagnosis |
| Daily Lab Test Volume          | Monitors operational lab workload trends over time                   |

### Scope

- **Data Period**: January 2025 – July 2025
- **Patient Records**: 200 unique patients across 10 diagnosis categories
- **Lab Records**: 200 lab test results across 6 test types
- **Outcomes**: 3 categories (Recovered, Complicated, Deceased)
- **Gold Tables**: 8 pre-aggregated analytics tables
- **Export Target**: Google BigQuery (`healthcare` dataset)

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Tech Stack](#tech-stack)
3. [Data Sources](#data-sources)
4. [Pipeline Stages](#pipeline-stages)
   - [Bronze Layer — Raw Data Ingestion](#bronze-layer--raw-data-ingestion)
   - [Silver Layer — Data Cleaning](#silver-layer--data-cleaning)
   - [Gold Layer — Business Analytics](#gold-layer--business-analytics)
5. [Gold Tables Reference](#gold-tables-reference)
6. [BigQuery Integration](#bigquery-integration)
7. [Job Orchestration](#job-orchestration)
8. [Visualizations & Dashboards](#visualizations--dashboards)
9. [Project Structure](#project-structure)
10. [Configuration](#configuration)
11. [How to Run](#how-to-run)

---

## Architecture Overview

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Raw CSVs   │────▶│   Bronze    │────▶│   Silver    │────▶│    Gold     │
│ (Volumes)   │     │  (Delta)    │     │  (Delta)    │     │  (Delta)    │
└─────────────┘     └─────────────┘     └─────────────┘     └──────┬──────┘
                                                                   │
                                                                   ▼
                                                            ┌─────────────┐
                                                            │  BigQuery   │
                                                            │  (Export)   │
                                                            └─────────────┘
```

**Storage**: Delta Lake on Unity Catalog Volumes  
**Path**: `/Volumes/workspace/default/healthcare_data_lake/`  
**Cloud**: Google Cloud Platform (GCP)  
**Compute**: Databricks Serverless  

---

## Tech Stack

| Component         | Technology                          |
| ----------------- | ----------------------------------- |
| Cloud Platform    | Google Cloud Platform (GCP)         |
| Data Platform     | Databricks (Serverless Compute)     |
| Storage Format    | Delta Lake                          |
| Processing Engine | Apache Spark (PySpark)              |
| Orchestration     | Databricks Workflows                |
| Export Target     | Google BigQuery                     |
| Temp Storage      | Google Cloud Storage (GCS)          |
| Secrets Manager   | Databricks Secret Scope             |

---

## Data Sources

Four raw CSV files stored in Unity Catalog Volumes:

| File             | Description                            | Key Columns                                                                 |
| ---------------- | -------------------------------------- | --------------------------------------------------------------------------- |
| `patients.csv`   | Patient demographics & hospital stays  | `patient_id`, `name`, `age`, `gender`, `diagnosis_id`, `admission_date`, `discharge_date`, `outcome_id`, `treatment_cost` |
| `labs.csv`       | Laboratory test results                | `lab_id`, `patient_id`, `test_name`, `result`, `normal_range`               |
| `diagnoses.csv`  | Diagnosis lookup table                 | `diagnosis_id`, `diagnosis_name`                                            |
| `outcomes.csv`   | Outcome lookup table                   | `outcome_id`, `outcome_name`                                                |

**Diagnoses**: Hypertension, Diabetes, Heart Disease, Asthma, Stroke, COPD, Cancer, Arthritis, Kidney Disease, Liver Disease  
**Outcomes**: Recovered (1), Complicated (2), Deceased (3)  
**Lab Tests**: Blood Pressure, Blood Sugar, Cholesterol, Creatinine, Hemoglobin, Vitamin D  

---

## Pipeline Stages

### Bronze Layer — Raw Data Ingestion

**Notebook**: `Bronze Layer -Raw Data Ingestion`

Ingests raw CSV files with explicit schemas and writes them as Delta tables to the Bronze zone.

| Step | Description |
| ---- | ----------- |
| 1    | Define explicit `StructType` schemas for all 4 tables |
| 2    | Read CSVs with headers using `spark.read.csv()` |
| 3    | Write to `bronze/` path as Delta with `overwriteSchema` |

**Bronze Tables**: `diagnosis`, `labs`, `outcomes`, `patients`

---

### Silver Layer — Data Cleaning

**Notebook**: `Silver Layer -Data Cleaning`

Applies data quality transformations and produces cleaned, joined datasets.

| Table              | Transformations Applied                                                       |
| ------------------ | ----------------------------------------------------------------------------- |
| `patients_clean`   | Deduplicate on `patient_id`, convert dates (`to_date`), cast `treatment_cost` to Double |
| `labs_clean`       | Normalize `normal_range` format (reorder high-low range values)               |
| `outcomes_clean`   | Deduplicate on `outcome_id`                                                   |
| `diagnosis_clean`  | Deduplicate on `diagnosis_id`                                                 |
| `silver_table`     | Left join of all 4 cleaned tables on `patient_id`, `diagnosis_id`, `outcome_id` |
| `patients_dq`      | Data quality flags: `INVALID_AGE` (age < 0 or > 120), `INVALID_COST` (cost ≤ 0), `NULL_PATIENT_ID`, or `VALID` |

---

### Gold Layer — Business Analytics

**Notebook**: `Gold Layer -Business Analytics`

Produces business-level aggregations from the Silver layer using SQL queries.

8 Gold tables are generated (see next section for details).

---

## Gold Tables Reference

| Gold Table                     | Description                                        | Key Metrics                                              |
| ------------------------------ | -------------------------------------------------- | -------------------------------------------------------- |
| `revenue_by_disease`           | Total treatment revenue per diagnosis              | `diagnosis_id`, `total_revenue`                          |
| `recovery_rate`                | Recovery rate per diagnosis                        | `diagnosis_id`, `total_patients`, `recovered_patients`, `recovery_rate` |
| `avg_age_diagnosed`            | Average patient age per diagnosis                  | `diagnosis_id`, `avg_age`                                |
| `high_risk_patients`           | Count of patients aged >60 per diagnosis           | `diagnosis_id`, `high_risk_patients`                     |
| `top_5_high_treament_cost`     | Top 5 costliest patient-diagnosis combinations     | `diagnosis_id`, `patient_id`, `total_treatment_cost`     |
| `diagnosis_admission_summary`  | Admission date ranges and stay duration            | `diagnosis_id`, `total_patients`, `first_admission_date`, `last_admission_date`, `last_discharge_date`, `latest_stay_days` |
| `lab_test_volume`              | Number of lab tests per patient                    | `patient_id`, `total_tests`                              |
| `daily_lab_tests`              | Daily test volume trends                           | `date`, `daily_tests`                                    |

---

## BigQuery Integration

**Notebook**: `Big Query Integration`

Exports all 8 Gold Delta tables to Google BigQuery for downstream analytics and BI tools.

| Config                | Value                              |
| --------------------- | ---------------------------------- |
| GCP Project           | `directed-truck-489818-t6`         |
| BigQuery Dataset      | `healthcare`                       |
| Temp GCS Bucket       | `healthcare-databricks-temp`       |
| Credentials           | Service account JSON (stored in Volumes) |
| Secret Scope          | `gcp-secret`                       |
| Secret Key            | `gcp-sa-key`                       |

The export loop reads each Gold table from Delta and writes to `{GCP_PROJECT}.healthcare.{table_name}` in BigQuery.

---

## Job Orchestration

The pipeline is orchestrated via **Databricks Workflows** as a sequential 3-task job:

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────────┐
│  Data_Ingestion  │────▶│  Data_Cleaning   │────▶│ Bussiness_Analytics  │
│  (Bronze Layer)  │     │  (Silver Layer)  │     │   (Gold Layer)       │
│  ~43 seconds     │     │  ~24 seconds     │     │   ~24 seconds        │
└──────────────────┘     └──────────────────┘     └──────────────────────┘
```

**Job Name**: Healthcare Data Pipeline Run  
**Total Runtime**: ~91 seconds  
**Status**: All tasks succeeded  

---

## Visualizations & Dashboards

The Gold layer outputs are visualized with the following charts:

| Chart Type          | Metric                                        | Description                                                        |
| ------------------- | --------------------------------------------- | ------------------------------------------------------------------ |
| Donut Chart         | Patient Distribution by Diagnosis             | Shows percentage split across 10 diagnosis categories (e.g., Hypertension 14%, Liver Disease 13%) |
| Donut Chart         | High-Risk Patient Distribution                | Breakdown of patients aged >60 by diagnosis                        |
| Bar Chart           | High-Risk Patients by Diagnosis               | Counts per diagnosis ID, colored by patient count                  |
| Bar + Line Combo    | Total Revenue by Diagnosis                    | Bar for revenue amount with trend line overlay                     |
| Bar + Line Combo    | Average Age by Diagnosis                      | Bar for avg age per diagnosis with trend line                      |
| Line Chart          | Treatment Cost by Diagnosis (Top 5)           | Total treatment cost for top diagnosis categories                  |
| Stacked Bar Chart   | Daily Admission Trends                        | 100% stacked bars showing admission date distribution over time    |

---

## Project Structure

```
/Volumes/workspace/default/healthcare_data_lake/
├── patients.csv                    # Raw source
├── labs.csv                        # Raw source
├── diagnoses.csv                   # Raw source
├── outcomes.csv                    # Raw source
├── bronze/                         # Bronze Delta tables
│   ├── diagnosis/
│   ├── labs/
│   ├── outcomes/
│   └── patients/
├── silver/                         # Silver Delta tables
│   ├── patients_clean/
│   ├── labs_clean/
│   ├── outcomes_clean/
│   ├── diagnosis_clean/
│   ├── silver_table/
│   └── patients_dq/
└── gold/                           # Gold Delta tables
    ├── revenue_by_disease/
    ├── recovery_rate/
    ├── avg_age_diagnosed/
    ├── high_risk_patients/
    ├── top_5_high_treament_cost/
    ├── diagnosis_admission_summary/
    ├── lab_test_volume/
    └── daily_lab_tests/
```

**Notebooks**:

| Notebook                              | Purpose                              |
| ------------------------------------- | ------------------------------------ |
| `Bronze Layer -Raw Data Ingestion`    | CSV ingestion → Bronze Delta         |
| `Silver Layer -Data Cleaning`         | Cleaning, joins, DQ checks → Silver  |
| `Gold Layer -Business Analytics`      | Aggregations → Gold Delta            |
| `Big Query Integration`               | Gold Delta → BigQuery export         |

---

## Configuration

All notebooks share the following path configuration (defined in Cell 1 of each notebook):

```python
RAW_PATH    = '/Volumes/workspace/default/healthcare_data_lake/'
BRONZE_PATH = '/Volumes/workspace/default/healthcare_data_lake/bronze/'
SILVER_PATH = '/Volumes/workspace/default/healthcare_data_lake/silver/'
GOLD_PATH   = '/Volumes/workspace/default/healthcare_data_lake/gold/'
```

**GCP Configuration**:
```python
GCP_PROJECT       = 'directed-truck-489818-t6'
BQ_QUERY          = 'healthcare'
TEMP_GCS_BUCKET   = 'healthcare-databricks-temp'
GCP_SECRET_SCOPE  = 'gcp-secret'
GCP_SECRET_KEY    = 'gcp-sa-key'
```

---

## How to Run

1. **Prerequisites**: Ensure raw CSV files are uploaded to `/Volumes/workspace/default/healthcare_data_lake/`
2. **Secrets**: Configure GCP service account credentials in Databricks Secret Scope (`gcp-secret`)
3. **Run via Workflow**: Trigger the **Healthcare Data Pipeline Run** job — it executes all 3 stages sequentially
4. **Run Manually**: Execute notebooks in order:
   - `Bronze Layer -Raw Data Ingestion`
   - `Silver Layer -Data Cleaning`
   - `Gold Layer -Business Analytics`
   - `Big Query Integration` (optional, for BigQuery export)
5. **Verify**: Check Gold tables in `/Volumes/workspace/default/healthcare_data_lake/gold/` or query from BigQuery

---

*Built on Databricks with PySpark and Delta Lake on GCP.*
