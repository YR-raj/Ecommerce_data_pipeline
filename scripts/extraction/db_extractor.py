import os
import csv
from pathlib import Path
import logging
from datetime import datetime
import psycopg2
import yaml

# If running inside Docker, use the standard Airflow home path directly
if os.environ.get("AIRFLOW_HOME"):
    MAIN_PROJECT_DIR = Path("/opt/airflow")
else:
    MAIN_PROJECT_DIR = Path(__file__).resolve().parent.parent.parent

CONFIG_DIR = MAIN_PROJECT_DIR / 'config'
config_path = CONFIG_DIR / 'pipeline_config.yaml'


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def load_pipeline_config(config_path=config_path):
    """
    Loads configurations parameters using PyYAML and adapts paths/ports dynamically for Docker.
    """
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
        
    # Check if the script is executing inside the Airflow Docker container
    if os.environ.get("AIRFLOW_HOME"):
        # Update Source OLTP configurations for Container-to-Container communication
        config["database"]["source_oltp"]["host"] = "source_oltp_db"
        config["database"]["source_oltp"]["port"] = 5432
        
        # Update Target OLAP configurations for Container-to-Container communication
        config["database"]["target_olap"]["host"] = "target_olap_db"
        config["database"]["target_olap"]["port"] = 5432
        
    return config


def get_connection(creds):
    """Returns a standard PostgreSQL connection."""
    return psycopg2.connect(
        host=creds.get("host", "localhost"),
        port=int(creds.get("port", 5432)),
        database=creds.get("database"),
        user=creds.get("user"),
        password=creds.get("password")
    )


def get_last_watermark(target_conn, pipeline_name, table_name):
    """Fetches the last successful watermark timestamp from the tracking table."""
    query = """
        SELECT last_success_watermark
        FROM etl_metadata
        WHERE pipeline_name = %s AND source_table = %s;
    """
    with target_conn.cursor() as cur:
        cur.execute(query, (pipeline_name, table_name))
        result = cur.fetchone()
        return result[0] if result else datetime(1970, 1, 1)


def update_metadata(target_conn, pipeline_name, table_name, new_watermark, status):
    """Updates the tracking table with the new high watermark after a successful run."""
    
    query = """
        INSERT INTO etl_metadata (pipeline_name, source_table, last_success_watermark, last_run_status)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (pipeline_name, source_table)
        DO UPDATE SET 
            last_success_watermark = EXCLUDED.last_success_watermark,
            last_run_status = EXCLUDED.last_run_status;
    """

    with target_conn.cursor() as cur:
        cur.execute(query, (pipeline_name, table_name, new_watermark, status))
    target_conn.commit()



def save_to_bronze(table_name, columns, rows, batch_index, run_id):
    """Save extracted rows to Bronze layer as CSV."""

    now = datetime.now()

    dir_path = (
        MAIN_PROJECT_DIR /
        "data" /
        "bronze" /
        table_name /
        f"run_{run_id}"
    )

    print(dir_path)
    os.makedirs(dir_path, exist_ok=True)

    file_path = dir_path / f"batch_{batch_index}.csv"

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)



def extract_table(config, source_conn, target_conn, table_name, timestamp_col, run_id):

    pipeline_name = config["pipeline"]["name"]
    batch_size = config["pipeline"].get("batch_size", 10000)

    low_watermark = get_last_watermark(
        target_conn,
        pipeline_name,
        table_name
    )

    logging.info(f"Table '{table_name}': Current low watermark is {low_watermark}")

    with source_conn.cursor() as cur:

        cur.execute(f"SELECT MAX({timestamp_col}) FROM {table_name}")
        high_watermark = cur.fetchone()[0]

    if high_watermark is None:
        logging.info(f"Table '{table_name}': Source table is empty.")
        return

    if high_watermark <= low_watermark:
        logging.info(f"Table '{table_name}': No new records.")
        return

    query = f"""
        SELECT *
        FROM {table_name}
        WHERE {timestamp_col} > %s
            AND {timestamp_col} <= %s
        ORDER BY {timestamp_col};
    """

    cursor_name = f"chunk_cursor_{table_name}"

    try:
        # Get schema
        with source_conn.cursor() as meta_cur:
            meta_cur.execute(
                f"SELECT * FROM {table_name} LIMIT 0"
            )

            columns = [desc[0] for desc in meta_cur.description]

        total_rows = 0
        batch_idx = 1

        # Stream rows
        with source_conn.cursor(name=cursor_name) as server_cur:
            server_cur.execute(query, (low_watermark, high_watermark))

            while True:
                rows = server_cur.fetchmany(batch_size)

                if not rows:
                    break

                save_to_bronze(table_name, columns, rows, batch_idx, run_id)
                total_rows += len(rows)
                logging.info(f"Table '{table_name}': Exported batch {batch_idx} ({len(rows)} rows).")                
                batch_idx += 1

        update_metadata(
            target_conn,
            pipeline_name,
            table_name,
            high_watermark,
            "SUCCESS"
        )

        logging.info(f"Table '{table_name} : Successfully exported {total_rows} rows.")
        logging.info(f"Table '{table_name}': Updated watermark to '{high_watermark}'")

    except Exception as e:

        update_metadata(
            target_conn,
            pipeline_name,
            table_name,
            low_watermark,
            f"FAILED: {str(e)}"
        )

        logging.exception(f"Extraction failed for {table_name}")
        raise


if __name__ == "__main__":
    logging.info("--- Beginning Bronze Extraction Job ---")
    cfg = load_pipeline_config()
    run_id = os.environ["PIPELINE_RUN_ID"]
    logging.info(f"Bronze run_id = {run_id}")
    
    s_conn = get_connection(cfg["database"]["source_oltp"])
    t_conn = get_connection(cfg["database"]["target_olap"])
    
    try:
        # Pull each operational entity independently based on their respective log markers
        extract_table(cfg, s_conn, t_conn, "source_products", "updated_at", run_id)
        extract_table(cfg, s_conn, t_conn, "source_customers", "created_at", run_id)
        extract_table(cfg, s_conn, t_conn, "source_orders", "updated_at", run_id)
        logging.info("--- Bronze Extraction Job Completed Successfully ---")
    finally:
        s_conn.close()
        t_conn.close()
