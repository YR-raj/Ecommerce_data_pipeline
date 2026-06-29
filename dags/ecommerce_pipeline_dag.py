from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "ecommerce_medallion_pipeline",
    default_args=default_args,
    description="Orchestrates E-commerce Seeding, Bronze, Silver, and Gold Layers",
    schedule_interval=None,  
    catchup=False,
) as dag:

    # # 1. Seeding Phase (Database initialization)
    # run_seeding = BashOperator(
    #     task_id="run_seeding",
    #     bash_command="python /opt/airflow/scripts/initialize_systems.py", 
    # )

    # 1. Bronze Extraction Layer
    run_bronze = BashOperator(
        task_id="extract_to_bronze",
        bash_command="""
        export PIPELINE_RUN_ID={{ ts_nodash }}
        python /opt/airflow/scripts/extraction/db_extractor.py
        """,
    )

    # 2. Silver Processing Layer
    run_silver = BashOperator(
        task_id="process_to_silver",
        bash_command="""
        export PIPELINE_RUN_ID={{ ts_nodash }}
        python /opt/airflow/scripts/transformation/silver_processor.py
        """,
    )

    # 3. Gold Transformation Layer (PySpark Engine)
    run_gold = BashOperator(
        task_id="transform_to_gold",
        bash_command="""
        export PIPELINE_RUN_ID={{ ts_nodash }}
        python /opt/airflow/scripts/transformation/gold_transformer.py
        """,
    )

    # Define the strict sequential execution order
    run_bronze >> run_silver >> run_gold