# databricks-transaction-pipeline
📌 Project Title

Cloud-Aware Transaction Data Engineering Pipeline using PySpark & Databricks

⸻

📖 Problem Statement

Financial systems ingest transaction data in semi-structured JSON format from external sources.
This data must be validated, standardized, deduplicated, and transformed into analytics-ready datasets while ensuring data consistency and re-run safety.

⸻

🧱 Architecture (Medallion Pattern)

Source → Bronze → Silver → Gold
	•	Source: Finance API (CoinGecko)
	•	Compute: Databricks Serverless (PySpark)
	•	Storage: Unity Catalog Volumes (Delta Lake)

⸻

🟤 Bronze Layer (Raw)
	•	Ingests raw JSON from API
	•	Stores data as-is with ingestion metadata
	•	No transformations applied

Purpose: Traceability and replayability

⸻

⚪ Silver Layer (Clean & Standardized)
	•	Flattens nested JSON using explode
	•	Standardizes timestamps
	•	Applies data quality checks (nulls, invalid values)
	•	Implements idempotent incremental loads using Delta MERGE
	•	Handles schema evolution using Delta mergeSchema

Purpose: Clean, deduplicated, reliable data

⸻

🟡 Gold Layer (Analytics-Ready)
	•	Aggregates transactional data by business date
	•	Computes metrics (avg, min, max, count)
	•	Exposes data via SQL views for BI tools

Purpose: Consumption by analytics and reporting teams

⸻

🔐 Data Reliability & Governance
	•	Uses Delta Lake ACID transactions
	•	Maintains full transaction history
	•	Supports time travel and auditability
	•	Storage governed using Unity Catalog volumes

⸻

🛠️ Tech Stack
	•	Python
	•	PySpark
	•	Databricks (Serverless)
	•	Delta Lake
	•	Unity Catalog
	•	SQL

⸻

✅ Key Features
	•	End-to-end medallion architecture
	•	JSON flattening & schema handling
	•	Idempotent pipeline design
	•	Incremental processing
	•	Schema evolution support
