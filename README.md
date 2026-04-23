# 🏥 Hospital ETL Pipeline (AWS Glue + PySpark)

## 📌 Overview

This project demonstrates a production-grade ETL pipeline built using AWS Glue and PySpark to transform raw hospital data into an analytics-ready format.

The pipeline processes CSV data stored in Amazon S3, applies transformations and data quality checks, and outputs a curated dataset in Parquet format for downstream analytics.

---

## 🧱 Architecture

S3 (Raw CSV)
→ AWS Glue (PySpark ETL)
→ S3 (Curated Parquet - Versioned)
→ Athena (Analytics)

---

## ⚙️ Tech Stack

* PySpark
* AWS Glue
* Amazon S3
* Parquet
* AWS Athena

---

## 🔄 Pipeline Flow

1. Load raw CSV files from S3
2. Clean and standardize dimension tables (patients, doctors, branches)
3. Build a core dataset using appointments as the fact table
4. Aggregate billing data to prevent duplication
5. Join fact and dimension tables into a star schema
6. Apply data quality checks:

   * Null validation on key fields
   * Schema validation
7. Write output to versioned S3 paths (`v1/`, `v2/`)

---

## 🚀 Key Features

### ✅ Star Schema Design

* Fact table: appointments
* Dimensions: patients, doctors, branches

### ✅ Data Quality Layer

* Prevents null key records
* Enforces schema consistency

### ✅ Optimized Transformations

* Broadcast joins to reduce shuffle
* Aggregation before joins to avoid duplication

### ✅ Versioned Data Storage

* Outputs stored as:

  * `v1/`
  * `v2/`
* Enables reproducibility and debugging

---

## ⚠️ Challenges & Solutions

### ❌ Duplicate columns in joins

✔ Resolved using column aliasing

### ❌ S3 overwrite failures

✔ Solved using versioned output paths

### ❌ Join duplication (fan-out issue)

✔ Fixed using aggregation before joining

### ❌ Schema inconsistencies

✔ Handled via explicit column selection and validation

---

## 📊 Output

Final dataset: **Patient Revenue Summary**

Includes:

* Appointment details
* Patient information
* Doctor specialization
* Branch details
* Total billing amount
* Billing frequency

Stored in Parquet format for efficient querying using Athena.

---

## 📷 Screenshots

(To be added)

* Glue job configuration
* S3 curated outputs
* Athena query results

---

## 🧠 Learnings

* Building scalable ETL pipelines in AWS
* Handling real-world data quality issues
* Designing star schema data models
* Optimizing Spark transformations

---

## 🔮 Future Improvements

* Incremental loading using watermarking
* Partitioned data storage
* Pipeline orchestration (Airflow)
* Advanced data validation frameworks

---
