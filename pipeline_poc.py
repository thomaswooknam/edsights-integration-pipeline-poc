import os
import duckdb
import pandas as pd

print("--- STEP 0: SFTP LANDING ZONE & FILE VERIFICATION ---")

# 1. Define your local mock SFTP path
sftp_landing_zone = "./mock_sftp_inbound"
target_file_name = "university_roster.csv"
full_file_path = os.path.join(sftp_landing_zone, target_file_name)

# 2. Setup: Automatically create the folder if it doesn't exist
if not os.path.exists(sftp_landing_zone):
    os.makedirs(sftp_landing_zone)
    print(f"Created local mock SFTP landing directory: {sftp_landing_zone}")

# 3. Setup: Write fresh, raw mock data into the folder to simulate a new transfer
mock_csv_payload = """STU_ID,FIRST_NAME,ENROLLMENT_STATUS,REGISTRATION_DATE
101,Tommy,1,2026-05-01
102,Alexandria,1,2026-05-02
101,Tommy,1,2026-05-01
,OrphanRecord,1,2026-05-03
104,BadDateFormat,2,05-04-2026"""

with open(full_file_path, "w") as f:
    f.write(mock_csv_payload)
print(f"Mock file '{target_file_name}' successfully deposited via simulated SFTP transfer.")


# ==========================================
# THE CORE PYTHON VERIFICATION GUARDBARS
# ==========================================

# Check A: Does the file actually exist at the location?
if not os.path.exists(full_file_path):
    print(f"[CRITICAL ERROR] Pipeline Halted: Expected file not found at {full_file_path}.")
    exit()
else:
    print(f"[SUCCESS] File verification passed: Found '{target_file_name}' in landing zone.")

# Check B: Is the file completely empty (0 bytes)? 
file_size = os.path.getsize(full_file_path)
if file_size == 0:
    print(f"[CRITICAL ERROR] Pipeline Halted: '{target_file_name}' is present but contains 0 bytes of data.")
    exit()
else:
    print(f"[SUCCESS] File integrity passed: File size is {file_size} bytes. Proceeding to database layer.\n")


# ==========================================
# DATABASE LAYER (STAGING & TRANSFORMATION)
# ==========================================

# 4. Initialize Database File
db_file = "edsights_demo.db"
if os.path.exists(db_file):
    os.remove(db_file)

conn = duckdb.connect(db_file)
print("Successfully connected to local database layer.")

# 5. Ingest Layer: Load raw data from our specific verified path into a Pandas DataFrame
df = pd.read_csv(full_file_path).astype(str)
conn.register("raw_file_dataframe", df)

# Create a Staging Table where everything is a loose text VARCHAR to prevent crash failures
conn.execute(
    """
    CREATE TABLE staging_university_roster AS 
    SELECT 
        STU_ID::VARCHAR as raw_id,
        FIRST_NAME::VARCHAR as raw_name,
        ENROLLMENT_STATUS::VARCHAR as raw_status,
        REGISTRATION_DATE::VARCHAR as raw_date
    FROM raw_file_dataframe;
"""
)
print("Ingest Layer Complete: Raw SFTP data loaded into staging_university_roster.")

# 6. Systematic Automated Error Isolation (Your Data Fidelity Guardrail)
print("\n--- RUNNING SYSTEMATIC DATA FIDELITY CHECKS ---")

# Check A: Identify Missing Primary Keys (Orphans)
orphans = conn.execute(
    "SELECT COUNT(*) FROM staging_university_roster WHERE raw_id IS NULL OR raw_id = 'nan' OR raw_id = ''"
).fetchone()[0]
print(f"[ALERT] Found {orphans} record(s) missing a student ID.")

# Check B: Identify Duplicates
duplicates = conn.execute(
    "SELECT COUNT(*) FROM (SELECT raw_id FROM staging_university_roster GROUP BY raw_id HAVING COUNT(*) > 1) AS dupes"
).fetchone()[0]
print(f"[ALERT] Found {duplicates} duplicate key conflict(s).")

# 7. Production Target Layer: Define the strict, clean schema table
conn.execute(
    """
    CREATE TABLE prod_student_roster (
        student_id INT PRIMARY KEY,
        first_name VARCHAR,
        enrollment_status VARCHAR,
        registration_date DATE
    );
"""
)

# 8. Transformation & Production Load (The SQL-driven Technical Handshake)
conn.execute(
    """
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
        raw_id IS NOT NULL AND raw_id != 'nan' AND raw_id != '' -- Drop orphans
        AND TRY_CAST(raw_date AS DATE) IS NOT NULL; -- Drop unparsable dates
"""
)
print("\nTransformation Layer Complete: Production environment populated.")

# 9. Verify Final Production Output
print("\n--- FINAL PRODUCTION TABLE RECORDS ---")
print(conn.execute("SELECT * FROM prod_student_roster").fetchdf())

conn.close()
