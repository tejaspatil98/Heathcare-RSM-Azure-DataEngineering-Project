# 🏥 Healthcare Revenue Cycle Management (RCM) Data Platform

An end-to-end, enterprise-grade Data Engineering platform designed to ingest, process, and analyze **Healthcare Revenue Cycle Management (RCM)** data across multiple hospital sources (EMR systems), claims providers, and external clinical APIs using the **Azure Data Engineering Stack**, **Medallion Architecture**, **Slowly Changing Dimensions (SCD Type 2)**, and **Unity Catalog**.

## 📌 Table of Contents

1. [Executive Summary & Business Domain](#-executive-summary--business-domain)

2. [End-to-End Architectural Flow](#-end-to-end-architectural-flow)

3. [Tech Stack & Azure Services](#-tech-stack--azure-services)

4. [Medallion Lakehouse Implementation](#-medallion-lakehouse-implementation)

5. [Data Modeling Strategy (Gold Layer)](#-data-modeling-strategy-gold-layer)

6. [Key Engineering Highlights](#-key-engineering-highlights)

7. [Deployment & Governance](#-deployment--governance)

8. [Repository Structure](#-repository-structure)

## 🩺 Executive Summary & Business Domain

**Revenue Cycle Management (RCM)** tracks healthcare finances from initial patient scheduling through treatment, billing, claim adjudication, and ultimate revenue collection.

### Business Goal & Key Indicators

The primary goal of this data platform is to aggregate multi-facility hospital data and external payor claims to help finance teams track **Accounts Receivable (AR)** performance, minimize collection cycles, and reduce write-offs.

```
flowchart LR
    A[📅 Patient Visit] --> B[🏥 Medical Service]
    B --> C[📄 Claim Generation]
    C --> D[📑 Payor Review]
    D -->|Approved| E[💰 Payor Payment]
    D -->|Partial / Denied| F[⚠️ Patient Responsibility]
    E & F --> G[📥 AR Settlement]

```

* **Core KPIs Tracked:**

  * **Days in AR:** Measure of average days taken to convert claims into cash.

  * **AR > 90 Days Ratio:** Percentage of total outstanding balance older than 90 days (high risk of bad debt).

## 🏗️ End-to-End Architectural Flow

The pipeline ingests heterogeneous sources using **Azure Data Factory**, stores data incrementally in **Azure Data Lake Storage Gen2** across a **Medallion Architecture**, and processes business rules with **Azure Databricks**.

```
[1. HETEROGENEOUS SOURCES]
  ├─ 🏥 EMR SQL DBs (Hospitals A & B) ────────┐
  ├─ 📂 Claims & CPT Files (Flat Files) ──────┼──> [📥 Landing Zone] ──┐
  └─ 🌐 External APIs (NPI & ICD Codes) ──────│                       │
                                              │                       │
[2. ORCHESTRATION]                            │                       │
  ├─ 🔑 Azure Key Vault (Credentials) ──(Auth)┘                       │
  └─ ⚡ Azure Data Factory (Metadata Pipelines) <─────────────────────┘
        │
        ▼
[3. MEDALLION STORAGE (ADLS Gen2)]  <───>  [4. COMPUTE & GOVERNANCE]
  ├─ 🟤 Bronze Layer (Parquet Source of Truth)
  │     │
  │     └─► (Processed by Azure Databricks PySpark) <─── (Governed by Unity Catalog)
  │               │
  ├─ ⚪ Silver Layer (CDM / Data Quality / SCD2 Delta)
  │     │
  │     └─► (Processed by Azure Databricks PySpark)
  │               │
  └─ 🟡 Gold Layer (Star Schema Delta Tables)

```

## 🛠️ Tech Stack & Azure Services

| Component | Technology | Role in Architecture | 
 | ----- | ----- | ----- | 
| **Orchestration** | **Azure Data Factory (ADF)** | Dynamic, metadata-driven pipelines, parallel loops, watermark logging. | 
| **Compute Engine** | **Azure Databricks** | PySpark execution for schema standardization, quality checks, and SCD2 merges. | 
| **Storage Engine** | **Delta Lake** | Provides ACID compliance, upserts, time travel, and schema enforcement. | 
| **Storage Tier** | **ADLS Gen2** | Multi-layer storage (`landing`, `bronze`, `silver`, `gold`). | 
| **Operational Source** | **Azure SQL Database** | Source Electronic Medical Record (EMR) databases for hospital facilities. | 
| **Security & Governance** | **Azure Key Vault & Unity Catalog** | Secret management, centralized governance, and workspace metastore sharing. | 

## 🏷️ Medallion Lakehouse Implementation

```
 📥 LANDING ZONE          🟤 BRONZE LAYER             ⚪ SILVER LAYER            🟡 GOLD LAYER
+----------------+      +-------------------+      +-------------------+      +-------------------+
|  Raw Flat CSV  | ---> | Raw Parquet Format| ---> | Common Data Model | ---> | Dimensional Model |
|  Files Dumped  |      | Source of Truth   |      | Quality Checks    |      | Star Schema       |
|                |      | (Immutable)       |      | SCD Type 2        |      | Facts & Dims      |
+----------------+      +-------------------+      +-------------------+      +-------------------+

```

1. **Landing Zone:** External claims and CPT code CSV files land directly here.

2. **Bronze Layer:** EMR databases, flat files, and external REST APIs (NPI/ICD codes) are ingested and converted into an immutable, raw **Parquet** state.

3. **Silver Layer:** Data is mapped into a **Common Data Model (CDM)**. PySpark performs data cleaning, quarantine handling (`is_quarantined`), and tracks history using **SCD Type 2** in **Delta Lake**.

4. **Gold Layer:** Business aggregation layer that models data into **Fact** and **Dimension** Delta tables for reporting and analytical consumption.

## 📐 Data Modeling Strategy (Gold Layer)

Data in the Gold Layer is modeled into a high-level **Star Schema** optimized for analytical queries:

```
                        ---------------------------+
                       |      dim_patient          |
                       +---------------------------+
                       | PK  patient_key (SK)      |
                       |     patient_id            |
                       |     first_name            |
                       |     last_name             |
                       |     gender                |
                       |     dob                   |
                       |     address               |
                       |     is_current            |
                       +-------------+-------------+
                                     │
                                     │ 1
                                     │
                                     │ N
+---------------------------+  +-----+---------------------+  +---------------------------+
|      dim_provider         |  |      fact_billing         |  |       dim_department      |
+---------------------------+  +---------------------------+  +---------------------------+
| PK  provider_key (SK)     |  | PK  billing_key (SK)      |  | PK  department_key (SK)   |
|     provider_id           |  | FK  patient_key           |  |     department_id         |
|     npi_code              |  | FK  provider_key          |  |     department_name       |
|     first_name            |  | FK  department_key        |  |     facility_name         |
|     last_name             |  | FK  claim_key             |  +-------------+-------------+
|     specialty             |  | FK  icd_key               |                │
+-------------+-------------+  | FK  cpt_key               |                │ 1
              │                |     encounter_id          |                │
              │ 1              |     transaction_date      |                │ N
              │                |     gross_charge_amount   |  +-------------+-------------+
              │ N              |     insurance_paid_amount |  |      fact_encounters      |
              +───────────────►|     patient_paid_amount   |  +---------------------------+
                               |     outstanding_balance   |  | PK  encounter_fact_key    |
                               |     days_in_ar            |  | FK  patient_key           |
                               +--------------+------------+  | FK  provider_key          |
                                              │               | FK  department_key        |
                                              │ N             |     admission_date        |
                                              │               |     discharge_date        |
                                              │ 1             |     encounter_type        |
                               +--------------+------------+  +---------------------------+
                               |        dim_claims         |
                               +---------------------------+
                               | PK  claim_key (SK)        |
                               |     claim_id              |
                               |     claim_status          |
                               |     payor_id              |
                               |     submitted_amount      |
                               +---------------------------+

```

## ⚡ Key Engineering Highlights

### 1. Metadata-Driven ADF Engine

Ingestion from source databases is completely decoupled from pipeline code. Pipeline parameters (database name, table name, watermark column, active flag, load type) are driven dynamically via a centralized CSV configuration file (`configs/emr/load_config.csv`).

* **Parallel ForEach Activity:** Processes entities concurrently.

* **Audit Logging:** Logs runtime execution metadata, row counts, and watermark dates to a dedicated `audit.load_logs` Delta table.

### 2. Data Quality & Quarantine Pattern

Data scrubbing identifies malformed or invalid records before writing to Silver tables. Instead of failing the entire job, bad records are tagged with an `is_quarantined = true` flag and isolated for revision.

### 3. Slowly Changing Dimensions (SCD Type 2)

Patient demographic changes (e.g., address or contact updates) are handled using Delta Lake `MERGE INTO` operations:

* Historical records are marked with `is_current = false` and end-dated.

* The latest record state is inserted with `is_current = true`.

## 🔒 Deployment & Governance

1. **Azure Key Vault:** Credentials, storage keys, and database passwords are pulled at runtime using linked services and secret scopes.

2. **Unity Catalog:** Manages governance and dataset access using three-level namespace organization (`catalog.schema.table`).

## 📁 Repository Structure

```
├── adf/
│   ├── pipeline/          # Dynamic metadata ingestion pipelines
│   ├── linkedService/     # Azure SQL, ADLS Gen2, Key Vault, Databricks
│   └── dataset/           # Generic parameterized datasets
├── databricks/
│   ├── 1_bronze/          # Raw API & flat file landing scripts
│   ├── 2_silver/          # Data cleansing, CDM mapping, SCD2 merge
│   └── 3_gold/            # Fact & Dimension aggregation builds
├── configs/               # Dynamic metadata control files
└── README.md

```
