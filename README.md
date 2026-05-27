# Automated Data Onboarding & Integration Pipeline PoC

A lightweight, platform-agnostic data integration engine designed to demonstrate a resilient, defensive approach to system-to-system flat-file onboarding. Built using Python for perimeter file system verification and DuckDB for relational staging and strict schema transformation workflows.

## 🏗️ Architectural Strategy

When integrating data between external clients (e.g., enterprise portals or university management platforms) and a target SaaS core application, importing raw files directly into clean production tables introduces significant operational risks. Formatting anomalies, duplicate primary keys, or structural column modifications can break downstream application state logic.

This Proof of Concept (PoC) implements a three-tier defensive strategy:
1. Perimeter Layer (Python): Automatically scans the target inbound landing zones, verifies file presence, and executes byte-size integrity checks to isolate empty payloads before initializing database execution.
2. Staging Layer (DuckDB - Relational Staging): Ingests raw data completely into flexible string-based VARCHAR schemas. This ensures the ingestion pipeline never crashes on initial load, regardless of malformed client inputs.
3. Transformation & Load Layer (SQL Engine): Executes systematic data-fidelity queries to programmatically isolate orphan records, trap duplicate primary key collisions, map dynamic value conversions (e.g., bitwise flags to categorical strings), and cleanly load validated entries into a strict, type-safe production schema.

---

## 🛠️ Tech Stack & Requirements
* Runtime: Python 3.x
* Data Processing: Pandas (for DataFrame abstraction)
* Database Engine: DuckDB (In-process analytical RDBMS)
* Version Control: Git

---

## 🚀 Local Deployment & Verification

To run this data integration engine inside an isolated virtual environment:

1. Clone the repository and navigate to the project directory:
git clone https://github.com/thomaswooknam/edsights-integration-pipeline-poc.git
cd edsights-integration-pipeline-poc

2. Build and activate the private Python environment:
python3 -m venv .venv
source .venv/bin/activate

3. Install the engine dependencies:
pip install duckdb pandas

4. Execute the pipeline:
python pipeline_poc.py

---

## 📊 Sample Validation Engine Output

When running, the engine automatically catches deliberate formatting discrepancies (such as duplicate entries, unparsable dates, and missing primary IDs) and outputs a deterministic data-fidelity summary:

--- STEP 0: SFTP LANDING ZONE & FILE VERIFICATION ---
Created local mock SFTP landing directory: ./mock_sftp_inbound
Mock file 'university_roster.csv' successfully deposited via simulated SFTP transfer.
[SUCCESS] File verification passed: Found 'university_roster.csv' in landing zone.
[SUCCESS] File integrity passed: File size is 185 bytes. Proceeding to database layer.

Successfully connected to local database layer.
Ingest Layer Complete: Raw SFTP data loaded into staging_university_roster.

--- RUNNING SYSTEMATIC DATA FIDELITY CHECKS ---
[ALERT] Found 1 record(s) missing a student ID.
[ALERT] Found 1 duplicate key conflict(s).

Transformation Layer Complete: Production environment populated.

--- FINAL PRODUCTION TABLE RECORDS ---
   student_id  first_name enrollment_status registration_date
0         101       Tommy           Active        2026-05-01
1         102  Alexandria           Active        2026-05-02
