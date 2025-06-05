from data_access.db_handler import get_sql_server_connection
from processing.retail_calculator_5digit import run_processing_pipeline
from config.app_config import (
    PARAMS_CONFIG,
    BIGN_CSV_FILE,
    OUTPUT_DIR,
    INPUT_DIR,
    DB_CONFIG,
)


def main():
    tsic_length_for_run = PARAMS_CONFIG["TSIC_LENGTH"]

    conn = get_sql_server_connection()

    if conn:
        try:
            run_processing_pipeline(conn)
        except Exception as e:
            import traceback

            traceback.print_exc()
        finally:
            conn.close()
    else:
        print("❌ CRITICAL: Could not connect to database. Exiting application.")


if __name__ == "__main__":
    from config.app_config import (
        INPUT_DIR,
        OUTPUT_DIR,
    )

    main()
