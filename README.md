# Automated REST API Inboarding & Defensive Integration Pipeline PoC

A lightweight, platform-agnostic data integration engine designed to demonstrate a resilient, defensive approach to web-connected system-to-system onboarding workflows. Built using **Python (Requests)** for secure network payload consumption and **DuckDB** for relational staging and strict schema transformation logic.

## 🏗️ Architectural Strategy

When integrating data between external client systems and a core SaaS application, accepting data streams directly into production environments introduces critical operational risks. Structural modifications, duplicate primary keys, or malformed values can easily break downstream application state logic.

This Proof of Concept (PoC) implements a four-tier defensive strategy:
1. **API Ingestion Layer (Python):** Establishes secure handshakes with target REST endpoints, parsing raw payloads into memory streams with comprehensive HTTP status handling.
2. **Schema Contract Validation:** Compares inbound streams against an immutable structural contract. Programmatically detects **Schema Drift** (unannounced client structural shifts or rogue columns) and safely sanitizes fields to prevent database layer failures.
3. **Loose Relational Staging (DuckDB):** Ingests raw data completely into flexible string-based VARCHAR tables, ensuring the execution engine never crashes on initial load regardless of row-level value defects.
4. **Transformation & Load Layer:** Executes rigorous analytical SQL queries to detect and report orphan records, trap duplicate primary key collisions, map categorical strings, and insert clean records into a strict production schema.

---

## 🛠️ Tech Stack & Requirements
* **Runtime:** Python 3.x
* **Core Libraries:** Pandas, Requests
* **Database Engine:** DuckDB (In-process analytical RDBMS)
* **Testing Framework:** PyTest, Responses (HTTP Network Mocking)

---

## 🚀 Local Deployment & Test Automation

To clone this integration engine and execute its automated test suite inside an isolated environment:

1. Clone the repository and enter the directory:
git clone https://github.com/thomaswooknam/edsights-integration-pipeline-poc.git
cd edsights-integration-pipeline-poc

2. Build and activate the private Python environment:
python3 -m venv .venv
source .venv/bin/activate

3. Install the engine and testing dependencies:
pip install duckdb pandas requests responses pytest

4. Run the automated test suite:
pytest -v

---

## 📊 Automated Test Suite Trace Log

When the testing suite runs, it simulates live web responses and schema anomalies, proving the resilience of the data infrastructure:

test_pipeline.py::test_successful_api_fetch PASSED
test_pipeline.py::test_api_server_error_throws_exception PASSED
test_pipeline.py::test_schema_drift_from_stream PASSED

============================================ 3 passed in 4.14s =============================================
