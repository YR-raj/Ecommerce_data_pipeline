import os
import sys
import logging
import json
from pathlib import Path
from datetime import datetime
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, array, array_remove, expr, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, TimestampType

######################################################################################################
# If running inside Docker, use the standard Airflow home path directly
if os.environ.get("AIRFLOW_HOME"):
    MAIN_PROJECT_DIR = Path("/opt/airflow")
else:
    MAIN_PROJECT_DIR = Path(__file__).resolve().parent.parent.parent

CONFIG_DIR = MAIN_PROJECT_DIR / 'config'
config_path = CONFIG_DIR / 'pipeline_config.yaml'

#######################################################################################################
# configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

########################################################################################################

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


def get_spark_session(pipeline_name):
    """
    Initializes a local Spark session optimized for memory performance.
    """
    return SparkSession.builder \
        .appName(f"{pipeline_name}_SilverProcessor") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()


def check_bronze_schema(df, expected_cols, table_name):
    """
    BRONZE LAYER: Schema Validation
    Verifies that all mandatory structural columns exist.
    If a column is entirely missing, it immediately terminates the pipeline execution.
    """
    missing_cols = [col for col in expected_cols if col not in df.columns]
    
    if missing_cols:
        logging.critical(f"CRITICAL SCHEMA FAILURE: TABLE '{table_name}' is missing columns: {missing_cols}")
        sys.exit(f"Pipeline execution halted due to schema mismatch in raw Bronze data for {table_name}.")
    
    logging.info(f"Table '{table_name}': Bronze schema structural check passed.")


def process_silver_table(spark, config, table_name, pk_col, timestamp_col, schema, expected_cols, rule_expressions):
    """
    SILVER LAYER: Executes data casting, multi-point business validations,
    quarantine routing, deduplication, and writes Parquet snapshots.
    """
    logging.info(f"=== Starting Silver Processing for Table: {table_name} ===")
    run_id = os.environ["PIPELINE_RUN_ID"]

    # ========================================================================================
    # 1. Read Raw Bronze Files dynamically across nested partition trees
    
    bronze_path = str(
        MAIN_PROJECT_DIR /
        "data" /
        "bronze" /
        table_name /
        f"run_{run_id}"
    )

    logging.info(f"Reading Bronze path: {bronze_path}")

    if not os.path.exists(bronze_path):
        logging.warning(f"No Bronze directory discovered for table '{table_name}'. Skipping run.")
        return

    # Load raw file data as strings to ensure schema checking is fully deterministic
    raw_df = spark.read.option("header", "true").csv(bronze_path)

    if raw_df.isEmpty():
        logging.warning(f"Bronze files for '{table_name}' are empty. Skipping.")
        return

    # Run structural integrity check before casting
    check_bronze_schema(raw_df, expected_cols, table_name)
    total_input_count = raw_df.count()


    # =========================================================================================
    # 2. Transform: Apply Strict Type Casting
    typed_df = raw_df
    for field in schema.fields:
        typed_df = typed_df.withColumn(field.name, col(field.name).cast(field.dataType))


    # =========================================================================================
    # 3. Validation Gauntlet: Evaluate Custom Business Rules Natively
    
    # We append rule-specific text tags if a row violates a specific expression constraint
    rule_columns = []
    for rule_name, expression in rule_expressions.items():
        col_name = f"err_{rule_name}"
        typed_df = typed_df.withColumn(col_name, when(expr(expression), lit(rule_name)))
        rule_columns.append(col_name)

    # Consolidate failures using an inline SQL lambda to clean out NULL values safely
    error_array_expr = f"filter(array({', '.join(rule_columns)}), x -> x IS NOT NULL)"
    typed_df = typed_df.withColumn("validation_errors", expr(error_array_expr))
    typed_df = typed_df.withColumn("is_valid", expr("size(validation_errors) == 0"))

    # Cache dataframe to optimize memory pipelines across split streams
    typed_df.cache()


    # =========================================================================================
    # 4. Route Split: Separate Clean Records from the Quarantined Records
    clean_stream = typed_df.filter(col("is_valid") == True).drop(*rule_columns, "validation_errors", "is_valid")
    quarantine_stream = typed_df.filter(col("is_valid") == False)

    # Calculate exact rule breakdown metrics from the quarantined stream
    quarantine_count = quarantine_stream.count()
    rule_metrics = {}
    if quarantine_count > 0:
        for rule_name in rule_expressions.keys():
            fail_count = quarantine_stream.filter(col(f"err_{rule_name}").isNotNull()).count()
            rule_metrics[rule_name] = fail_count



    # =========================================================================================
    # 5. Deduplication: Apply Window Ranking Over Clean Records
    
    # If the exact same primary key pops up multiple times, grab the newest mutation snapshot
    window_spec = Window.partitionBy(pk_col).orderBy(col(timestamp_col).desc())
    deduplicated_clean_df = clean_stream.withColumn("row_rank", row_number().over(window_spec)) \
                                        .filter(col("row_rank") == 1) \
                                        .drop("row_rank")

    clean_promoted_count = deduplicated_clean_df.count()
    deduplicated_rows_dropped = clean_stream.count() - clean_promoted_count



    # =========================================================================================
    # 6. Physical Storage Commit (Write Outputs to Disk)

    # Write isolated raw corrupted items out to the Quarantine layer
    if quarantine_count > 0:
        quarantine_output_path = f"data/quarantine/{table_name}/run_{run_id}"
        quarantine_stream.drop("is_valid").write.mode("overwrite").parquet(quarantine_output_path)
        logging.warning(f"Table '{table_name}': Isolated {quarantine_count} bad rows inside: {quarantine_output_path}")

    # Write clean, typed, deduplicated items into the Silver layer
    silver_output_path = str(
        MAIN_PROJECT_DIR /
        "data" /
        "silver" /
        table_name
    )

    # If Silver already exists, merge it with the current batch
    if os.path.exists(silver_output_path):
        existing_silver_df = spark.read.parquet(silver_output_path)
    else:
        existing_silver_df = None

    if existing_silver_df is not None:

        merged_df = existing_silver_df.unionByName(deduplicated_clean_df)

        window_spec = Window.partitionBy(pk_col).orderBy(col(timestamp_col).desc())

        final_silver_df = (
            merged_df
            .withColumn("row_rank", row_number().over(window_spec))
            .filter(col("row_rank") == 1)
            .drop("row_rank")
        )

        final_silver_df = final_silver_df.cache()
        final_count = final_silver_df.count()

    else:
        final_silver_df = deduplicated_clean_df

    final_silver_df.write.mode("overwrite").parquet(silver_output_path)

    logging.info(
        f"Table '{table_name}': Silver warehouse now contains {final_count} records after incremental merge."
    )

    final_silver_df.unpersist()

    # =========================================================================================
    # 7. Generate Quality Metrics Payload
    yield_pct = round((clean_promoted_count / total_input_count) * 100, 2) if total_input_count > 0 else 0.0
    
    metrics_payload = {
        "pipeline_run_id": run_id,
        "table_name": table_name,
        "metrics": {
            "input_records_extracted": total_input_count,
            "clean_records_promoted": clean_promoted_count,
            "quarantined_records_isolated": quarantine_count,
            "duplicate_records_purged": deduplicated_rows_dropped,
            "data_health_yield_percentage": yield_pct
        },
        "failures_by_rule_breakdown": rule_metrics,
        "execution_status": "FAILED_TOLERANCE" if yield_pct < 90.0 and total_input_count > 0 else "PROCEEDED"
    }
    
    print("\n======================================================================")
    print(f"DATA QUALITY METRICS FOR {table_name.upper()}")
    print("======================================================================")
    print(json.dumps(metrics_payload, indent=2))
    print("======================================================================\n")

    # Clear cached frames from memory
    typed_df.unpersist()

    # Fail pipeline rule check: If data corruption rate is completely unacceptable, terminate downstream steps
    if metrics_payload["execution_status"] == "FAILED_TOLERANCE":
        sys.exit(f"Pipeline failed: Data quality yield for '{table_name}' dropped below strict 90% tolerance threshold.")



########################################################################################################

if __name__ == "__main__":
    logging.info("--- Starting Silver Processing Engine Pipeline ---")
    cfg = load_pipeline_config()
    spark_session = get_spark_session(cfg["pipeline"]["name"])
    
    try:
        # ----------------------------------------------------------------------
        # CONFIGURATION 1: SOURCE_PRODUCTS
        # ----------------------------------------------------------------------
        prod_schema = StructType([
            StructField("product_id", IntegerType(), True),
            StructField("product_name", StringType(), True),
            StructField("price", DecimalType(10, 2), True),
            StructField("category", StringType(), True),
            StructField("stock_quantity", IntegerType(), True),
            StructField("updated_at", TimestampType(), True)
        ])
        prod_cols = ["product_id", "product_name", "price", "category", "stock_quantity", "updated_at"]
        prod_rules = {
            "null_primary_key": "product_id IS NULL",
            "negative_or_zero_price": "price <= 0 OR price IS NULL",
            "invalid_category_enum": "category NOT IN ('Electronics', 'Apparel', 'Home & Kitchen', 'Books', 'Beauty')"
        }
        process_silver_table(spark_session, cfg, "source_products", "product_id", "updated_at", prod_schema, prod_cols, prod_rules)

        # ----------------------------------------------------------------------
        # CONFIGURATION 2: SOURCE_CUSTOMERS
        # ----------------------------------------------------------------------
        cust_schema = StructType([
            StructField("customer_id", IntegerType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("city", StringType(), True),
            StructField("created_at", TimestampType(), True)
        ])
        cust_cols = ["customer_id", "first_name", "last_name", "email", "city", "created_at"]
        cust_rules = {
            "null_primary_key": "customer_id IS NULL",
            "malformed_or_null_email": "email IS NULL OR email NOT LIKE '%@%'"
        }
        process_silver_table(spark_session, cfg, "source_customers", "customer_id", "created_at", cust_schema, cust_cols, cust_rules)

        # ----------------------------------------------------------------------
        # CONFIGURATION 3: SOURCE_ORDERS
        # ----------------------------------------------------------------------
        order_schema = StructType([
            StructField("order_id", IntegerType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("total_amount", DecimalType(10, 2), True),
            StructField("updated_at", TimestampType(), True)
        ])
        order_cols = ["order_id", "customer_id", "product_id", "quantity", "total_amount", "updated_at"]
        order_rules = {
            "null_primary_key": "order_id IS NULL",
            "invalid_quantity": "quantity <= 0 OR quantity IS NULL",
            "negative_total_amount": "total_amount < 0 OR total_amount IS NULL"
        }
        process_silver_table(spark_session, cfg, "source_orders", "order_id", "updated_at", order_schema, order_cols, order_rules)

        logging.info("--- Silver Processing Engine Completed Successfully ---")
    finally:
        spark_session.stop()

