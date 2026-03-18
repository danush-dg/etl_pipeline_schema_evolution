# 📊 MySQL ETL Pipeline with Schema Evolution, Full & Incremental Load

## 📌 Overview
This project implements a **robust ETL (Extract, Transform, Load) pipeline** using Python and MySQL. It supports:

- Full (historical) load
- Incremental load
- Schema validation & evolution
- Dynamic upsert (merge)
- Logging & alerting
- Control-driven execution

The pipeline ensures **data consistency across source → staging → operational layers**.

---

## 🏗️ Architecture

Source Table → Staging Table → Operational Table  
                     ↓  
               Logging Table  
                     ↓  
               Control Table  

---

## 🗂️ Database Tables

### 1. `source_sales`
- Raw source data
- Contains `updated_at` for incremental extraction

### 2. `stg_sales`
- Temporary staging layer
- Dynamically updated schema

### 3. `op_sales`
- Final operational table
- Stores deduplicated and updated records

### 4. `control_table`
Stores pipeline configuration:
- `source_table`, `staging_table`, `operational_table`
- `ingestion_type` (full / incremental)
- `last_loaded`
- `primary_key_col`

### 5. `logging_table`
Tracks pipeline execution:
- `row_loaded`
- `status`
- `schema_change`
- `alert`

---


## ✅ 3. Schema Evolution

Handles:

### ➤ New Columns
- Automatically added to staging & operational tables

### ➤ Datatype Changes
- Detects mismatch between source & staging
- Applies safe conversions only:
  - VARCHAR → VARCHAR
  - INT → INT
  - DECIMAL → DECIMAL
  - DATETIME → DATETIME

### ➤ Validation
Pipeline fails if:
- Column missing in source
- Unsafe datatype change detected

---

## ✅ 4. Dynamic Upsert

```sql
INSERT INTO op_sales (...)
SELECT ...
FROM stg_sales
ON DUPLICATE KEY UPDATE ...
```

- Inserts new records  
- Updates existing records  
- Prevents duplicates  

---

## ✅ 5. Logging & Alerting

Tracks:
- Row count  
- Schema changes  
- Execution status  

### 🚨 Alert Condition
Triggered when:
- Current Load > 110% of last 5-day average  

---

## ✅ 6. Control-Driven Execution

Pipeline reads from `control_table`:

- Makes solution reusable  
- Allows switching between full/incremental loads dynamically  

---

## 🧠 Pipeline Flow

1. Connect to MySQL  
2. Read control table configuration  
3. Validate & evolve schema  
4. Extract data (full / incremental)  
5. Load into staging (truncate + insert)  
6. Upsert into operational table  
7. Update control table (`last_loaded`)  
8. Log execution  
9. Disconnect  

---

## 🚀 Setup & Execution

### 1. Create Database & Tables

Run the provided SQL script:

```sql
CREATE DATABASE db;
USE db;
-- Execute full script
```

### 2. Install Dependency

```bash
pip install mysql-connector-python
```

### 3. Configure Connection

```python
etl = MySQLETLPipeline('localhost', 'root', '1234', 'db')
```

### 4. Run Pipeline

```bash
python your_script.py
```

---

## 🧪 Test Scenarios Covered

- ✔ Initial full load  
- ✔ Incremental load  
- ✔ Schema evolution (add column)  
- ✔ Datatype change handling  
- ✔ Alert triggering  
- ✔ Failure handling  

---

## 🔄 Switching Load Type

### Full Load (Historical)

```sql
UPDATE control_table 
SET ingestion_type = 'full',
    last_loaded = '1900-01-01 00:00:00';
```

### Incremental Load

```sql
UPDATE control_table 
SET ingestion_type = 'incremental',
    last_loaded = NOW();
```

---

## 📊 Example Schema Changes

```sql
-- Change datatype
ALTER TABLE source_sales MODIFY COLUMN amount INT;

-- Add new column
ALTER TABLE source_sales ADD COLUMN category VARCHAR(50);
```

Pipeline will:
- Detect change  
- Apply to staging & operational  
- Log schema change  

---

## ⚠️ Error Handling

Pipeline fails safely when:
- Missing columns in source  
- Unsafe datatype conversion  
- Query execution failure  

✔ Automatic rollback supported  

---

## 📈 Sample Logging Output

| alert | row_loaded | status  | schema_change |
|------|-----------|--------|--------------|
| No   | 5         | Success | Yes          |
| Yes  | 20        | Success | No           |
| No   | 0         | Failed  | No           |

## ✅ 2. Incremental Load
Loads only new/updated records

```sql
SELECT * 
FROM source_sales
WHERE updated_at >= last_loaded;
