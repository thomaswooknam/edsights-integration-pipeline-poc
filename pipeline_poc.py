import os
import duckdb
import pandas as pd
import requests
import io

def fetch_data_from_api(endpoint_url):
    """
    Simulates fetching inbound payload records securely via a web API.
    Includes fundamental HTTP error and status handling.
    """
    try:
        response = requests.get(endpoint_url, timeout=10)
        # Automatically throw an exception if the web server returns an error (e.g., 404 or 500)
        response.raise_for_status() 
        return response.text
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"API Connection Failure: Unable to fetch inbound data. Error: {e}")


def verify_schema_contract(file_content, expected_columns):
    """
    Validates an incoming data stream against a strict structural contract.
    """
    # Use io.StringIO to let Pandas read the raw text stream exactly like an open file
    actual_headers = list(pd.read_csv(io.StringIO(file_content), nrows=0).columns)
    
    missing_columns = [col for col in expected_columns if col not in actual_headers]
    unexpected_columns = [col for col in actual_headers if col not in expected_columns]
    
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
        
    drift_detected = len(unexpected_columns) > 0
    return {"drift_detected": drift_detected, "unexpected_columns": unexpected_columns}


def run_pipeline(target_url="https://api.university.edu/v1/roster"):
    print("--- STEP 0: SECURE API ENDPOINT INGESTION LAYER ---")
    print(f"[INFO] Initializing handshake with target REST API: {target_url}")
    
    try:
        raw_payload = fetch_data_from_api(target_url)
        print("[SUCCESS] API Handshake established. Payload stream consumed successfully.\n")
    except RuntimeError as err:
        print(f"[CRITICAL ERROR] {err}")
        return

    print("--- STEP 1: SCHEMA STRUCTURE CONTRACT VERIFICATION ---")
    EXPECTED_COLUMNS = ["STU_ID", "FIRST_NAME", "ENROLLMENT_STATUS", "REGISTRATION_DATE"]
    
    result = verify_schema_contract(raw_payload, EXPECTED_COLUMNS)
    
    if result["drift_detected"]:
        print(f"[SCHEMA DRIFT DETECTED] Warning: Inbound API schema contains undocumented fields: {result['unexpected_columns']}")
        print(f"[ACTION] Defensive Isolation: Stripping unexpected fields to enforce application data integrity.")
        df_sanitized = pd.read_csv(io.StringIO(raw_payload), usecols=EXPECTED_COLUMNS).astype(str)
    else:
        print("[SUCCESS] Schema Contract Matches perfectly.")
        df_sanitized = pd.read_csv(io.StringIO(raw_payload)).astype(str)

    # Database Layer (Staging & Transformation)
    db_file = "edsights_demo.db"
    if os.path.exists(db_file):
        os.remove(db_file)

    conn = duckdb.connect(db_file)
    conn.register("raw_api_dataframe", df_sanitized)

    conn.execute("""
        CREATE TABLE staging_university_roster AS 
        SELECT 
            STU_ID::VARCHAR as raw_id,
            FIRST_NAME::VARCHAR as raw_name,
            ENROLLMENT_STATUS::VARCHAR as raw_status,
            REGISTRATION_DATE::VARCHAR as raw_date
        FROM raw_api_dataframe;
    """)
    print("\nStaging Layer Compiled: Sanitized data loaded safely into staging_university_roster.")

    print("\n--- RUNNING SYSTEMATIC DATA FIDELITY CHECKS ---")
    orphans = conn.execute("SELECT COUNT(*) FROM staging_university_roster WHERE raw_id IS NULL OR raw_id = 'nan' OR raw_id = ''").fetchone()[0]
    print(f"[ALERT] Found {orphans} record(s) missing a student ID.")

    duplicates = conn.execute("SELECT COUNT(*) FROM (SELECT raw_id FROM staging_university_roster GROUP BY raw_id HAVING COUNT(*) > 1) AS dupes").fetchone()[0]
    print(f"[ALERT] Found {duplicates} duplicate key conflict(s).")

    conn.execute("""
        CREATE TABLE prod_student_roster (
            student_id INT PRIMARY KEY,
            first_name VARCHAR,
            enrollment_status VARCHAR,
            registration_date DATE
        );
    """)

    conn.execute("""
        INSERT INTO prod_student_roster
        SELECT DISTINCT
            raw_id::INT as student_id,
            raw_name as first_name,
            CASE 
            WHEN raw_status = '1' THEN 'Active'
            WHEN raw_status = '2' THEN 'Inactive'
            ELSE 'Unknown'
            END as enrollment_status,
            TRY_CAST(raw_date AS DATE) as registration_date
        FROM staging_university_roster
        WHERE 
            raw_id IS NOT NULL AND raw_id != 'nan' AND raw_id != ''
            AND TRY_CAST(raw_date AS DATE) IS NOT NULL;
    """)
    print("\nTransformation Layer Complete: Production environment populated.")

    print("\n--- FINAL PRODUCTION TABLE RECORDS ---")
    print(conn.execute("SELECT * FROM prod_student_roster").fetchdf())
    conn.close()

if __name__ == "__main__":
    # If running normally, we require an integration test setup or live target. 
    # Let's verify via our automated test suite next!
    pass
