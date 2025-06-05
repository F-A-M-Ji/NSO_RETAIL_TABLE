import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DB_CONFIG = {
    "server": "192.168.0.235",
    "port": "1433",
    "database": "retail",
    "username": "sa",
    "password": "1qazxsw@",
    "driver": "{ODBC Driver 17 for SQL Server}",
    "table_name": "Report_test_table",
}

PARAMS_CONFIG = {"YR": "67", "QTR": "1", "TSIC_LENGTH": 4}

BIGN_CSV_FILE = os.path.join(BASE_DIR, "input", "Bign_4digit.csv")

INPUT_DIR = os.path.join(BASE_DIR, "input")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

for dir_path in [INPUT_DIR, OUTPUT_DIR]:
    if not os.path.exists(dir_path):
        try:
            os.makedirs(dir_path)
            if dir_path == INPUT_DIR:
                print(f"Please place {os.path.basename(BIGN_CSV_FILE)} in {INPUT_DIR}")
        except Exception as e:
            print(f"Error creating directory {dir_path}: {e}")
