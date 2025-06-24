import psycopg2
import csv
import logging
import os

# --- Configuration ---
DB_CONFIG = {
    'host': 'localhost',
    'dbname': 'New_Ract',
    'user': 'postgres',
    'password': 'Janan!1802',
    'port': 5432
}

TABLES_AND_CSVS = {
    '"Appendix C - P1"': 'Appendix C.csv',
    'appendix_b_p2_conversion': 'Appendix B.csv',
    '"HSHA-PARENTERAL-INFUSION"': 'HSHA-PARENTERAL-INFUSION.csv'
}

LOG_FILE = 'multi_insert_log.txt'

# --- Setup logging ---
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)

# --- Connect to PostgreSQL ---
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

# --- Insert each CSV file into its table ---
for table, csv_file in TABLES_AND_CSVS.items():
    if not os.path.exists(csv_file):
        logging.error(f"CSV file not found: {csv_file}")
        continue

    with open(csv_file, newline='') as file:
        reader = csv.reader(file)
        headers = next(reader)  # assumes first row is header
        placeholders = ', '.join(['%s'] * len(headers))
        query = f'INSERT INTO {table} ({", ".join(headers)}) VALUES ({placeholders})'

        logging.info(f"\n--- Inserting into {table} from {csv_file} ---")
        for row in reader:
            try:
                cur.execute(query, row)
                logging.info(f"Inserted: {row}")
            except Exception as e:
                logging.error(f"Failed: {row} | Error: {e}")

# --- Finalize ---
conn.commit()
cur.close()
conn.close()

print("All inserts complete. Logs saved to multi_insert_log.txt.")
