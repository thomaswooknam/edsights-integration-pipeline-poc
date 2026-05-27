import os
import duckdb
import pandas as pd

def verify_schema_contract(file_path, expected_columns):
    """
    Validates an incoming file against a strict structural contract.
    Returns a dictionary indicating if drift was detected and the list of unexpected columns.
    """
    actual_headers = list(pd.read_csv(file_path, nrows=0).columns)
    
    missing_columns = [col for col in expected_columns if col not in actual_headers]
    unexpected_columns = [col for col in actual_headers if col not in expected_columns]
    
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
        
    drift_detected = len(unexpected_columns) > 0
    return {"drift_detected": drift_detected, "unexpected_columns": unexpected_columns}


def run_pipeline():
    print("--- STEP 0: SFTP LANDING ZONE & FILE VERIFICATION ---")
    sftp_landing_zone = "./mock_sftp_inbound"
    target_file_name = "university_roster.csv"
    full_file_path = os.path.join(sftp_landing_zone, target_file_name)

    if not os.path.exists(sftp_landing_zone):
        os.makedirs(sftp_landing_zone)

    # Simulation payload with rogue column
    mock_csv_payload = """STU_ID,FIRST_NAME,MIDDLE_NAME,ENROLLMENT_STATUS,REGISTRATION_DATE
101,Tommy,Lee,1,2026-05-01
102,Alexandria,Alicia,1,2026-05-02
101,Tommy,,1,2026-05-01
,OrphanRecord,,1,2026-05-03
104,BadDateFormat,,2,05-04-2026"""

    with open(full_file_path, "w") as f:
        f.write(mock_csv_payload)
    print(f"Mock file '{target_file_name}' successfully deposited via simulated SFTP transfer.")

    if not os.path.exists(full_file_path) or os.path.getsize(full_file_path) == 0:
        print("[CRITICAL ERROR] Pipeline Halted: File verification failed.")
        return

    print("\n--- STEP 1: SCHEMA STRUCTURE CONTRACT VERIFICATION ---")
    EXPECTED_COLUMNS = ["STU_ID", "FIRST_NAME", "ENROLLMENT_STATUS", "REGISTRATION_DATE"]
    
    # Call our testable function
    result = verify_schema_contract(full_file_path, EXPECTED_COLUMNS)
    
    if result["drift_detected"]:
        print(f"[SCHEMA DRIFT DETECTED] Warning: Inbound file contains undocumented columns: {result['unexpected_columns']}")
        print(f"[ACTION] Defensive Isolation: Gracefully discarding rogue columns to prevent staging layer compilation failure.")
        df_sanitized = pd.read_csv(full_file_path, usecols=EXPECTED_COLUMNS).astype(str)
    else:
        print("[SUCCESS] Schema Contract Matches perfectly.")
        df_sanitized = pd.read_csv(full_file_path).astype(str)

    # Database Layer
    db_file = "edsights_demo.db"
    if os.path.exists(db_file):
        os.remove(db_file)

    conn = duckdb.connect(db_file)
    conn.register("raw_file_dataframe", df_sanitized)

    conn.execute("""
        CREATE TABLE staging_university_roster AS 
        SELECT 
            STU_ID::VARCHAR as raw_id,
            FIRST_NAME::VARCHAR as raw_name,
            ENROLLMENT_STATUS::VARCHAR as raw_status,
            REGISTRATION_DATE::VARCHAR as raw_date
        FROM raw_file_dataframe;
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
    run_pipeline()
