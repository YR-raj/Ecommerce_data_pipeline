import os
import sys
import logging
import json
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, round, sum, count, lit, when

######################################################################################################
# dynamic path setting
if os.environ.get("AIRFLOW_HOME"):
    MAIN_PROJECT_DIR = Path("/opt/airflow")
else:
    MAIN_PROJECT_DIR = Path(__file__).resolve().parent.parent.parent
    
# Easily chain the folder and target file
SILVER_DIR = MAIN_PROJECT_DIR / "data" / "silver"
GOLD_DIR = MAIN_PROJECT_DIR / "data" / "gold"

os.makedirs(GOLD_DIR, exist_ok=True)

#######################################################################################################
# configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

########################################################################################################

def get_spark_session():
    """Initializes the Gold Spark Session."""
    return SparkSession.builder \
        .appName("Gold_Transformer_Engine") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

def build_gold_layer():
    logging.info("--- Initializing Gold Layer Transformation Suite ---")
    spark = get_spark_session()
    
    try:
        # ======================================================================
        # BLOCK 1: INGESTION (Reading Silver Parquet Files)
        # ======================================================================
        logging.info("Step 1: Reading pristine Silver Parquet assets...")

        required_tables = [
            "source_products",
            "source_customers",
            "source_orders"
        ]

        for table in required_tables:
            path = SILVER_DIR / table
            if not path.exists():
                logging.warning(f"Silver table '{table}' not found. Skipping Gold transformation.")
                return
        
        # Wrapped in str() to ensure full compatibility with Spark's Java layer
        silver_products = spark.read.parquet(str(SILVER_DIR / "source_products"))
        silver_customers = spark.read.parquet(str(SILVER_DIR / "source_customers"))
        silver_orders = spark.read.parquet(str(SILVER_DIR / "source_orders"))


        # ======================================================================
        # BLOCK 2: DIMENSIONAL MODELING (dim_products & dim_customers)
        # ======================================================================
        logging.info("Step 2: Transforming Silver structures into Dimensions...")
        
        # We select specific columns and rename them cleanly using .alias()
        dim_products = silver_products.select(
            col("product_id"),
            col("product_name").alias("product_name"),
            col("price").alias("unit_price"),
            col("category").alias("product_category")
        )
        
        dim_customers = silver_customers.select(
            col("customer_id"),
            col("first_name").alias("customer_first_name"),
            col("last_name").alias("customer_last_name"),
            col("email").alias("customer_email"),
            col("city").alias("customer_city")
        )
        
        
        # ======================================================================
        # BLOCK 3: FACT MODELING (fact_orders with business logic)
        # ======================================================================
        logging.info("Step 3: Engineering Core Fact Table with relational joins...")
        
        # TODO: Join silver_orders against dim_products and dim_customers on their keys
        # Hint: Use `df.join(other_df, on="key", how="inner")`
        # TODO: Add a calculated column for `net_sales` (quantity * price)
        # Hint: Use `.withColumn("net_sales", round(col("...") * col("..."), 2))`
        fact_orders = silver_orders.join(dim_products, on="product_id", how="left") \
                                   .join(dim_customers, on="customer_id", how="left")
            
        fact_orders = fact_orders.withColumn(
                    "net_sales", 
                    round(col("quantity") * col("unit_price"), 2)
                )        

        fact_orders = fact_orders.withColumn("order_date", expr("to_date(updated_at)"))
        
        # ======================================================================
        # BLOCK 4: BUSINESS AGGREGATIONS (Daily Category Performance)
        # ======================================================================
        logging.info("Step 4: Pre-calculating analytical business matrices...")
        
        # TODO: Group fact_orders by order_date (extracted from updated_at) and product category
        # TODO: Calculate total_revenue (sum of net_sales) and total_orders_placed (count of order_id)
        # Hint: Use `.groupBy("col1", "col2").agg(sum("...").alias("..."), count("...").alias("..."))`
        # Hint: To get date from timestamp, you can use `expr("to_date(updated_at)")`
        daily_category_sales = fact_orders \
                                .groupBy("order_date", "product_category") \
                                .agg(
                                    sum("net_sales").alias("total_revenue"),
                                    count("order_id").alias("total_orders")
                                )
        
        # ======================================================================
        # BLOCK 5: THE RECONCILIATION AUDIT ENGINE
        # ======================================================================
        logging.info("Step 5: Initiating financial reconciliation balancing audit...")
        
        # Pull row quantities out of Spark distributed memory into Python integers
        silver_orders_input_cnt = silver_orders.count()
        gold_fact_cnt = fact_orders.count()
        
        logging.info(f"Reconciliation Audit Ledger:")
        logging.info(f" -> Silver Input Records: {silver_orders_input_cnt}")
        logging.info(f" -> Gold Fact Records Generated: {gold_fact_cnt}")
        
        # Strict validation: Since we did a left join to attach dimensions, 
        # the total record count must match our input exactly (no rows dropped)
        if silver_orders_input_cnt != gold_fact_cnt:
            logging.error("CRITICAL CRASH: Data leakage detected! Records dropped during Gold transformation.")
            raise ValueError("Data pipeline halted due to reconciliation balance mismatch.")
        
        logging.info("Audit Passed: 100% of data successfully accounted for.")


        # ======================================================================
        # BLOCK 6: WRITING GOLD STANDARD ASSETS TO DISK
        # ======================================================================
        logging.info("Step 6: Saving optimized assets to Gold Storage Directory...")
        
        # Isolate each table cleanly into its own physical sub-warehouse folder
        dim_customers.write.mode("overwrite").parquet(str(GOLD_DIR / "dim_customers"))
        dim_products.write.mode("overwrite").parquet(str(GOLD_DIR / "dim_products"))
        
        # Write out the fact table optimized by date partition folders
        fact_orders.write.mode("overwrite") \
            .partitionBy("order_date") \
            .parquet(str(GOLD_DIR / "fact_orders"))
            
        # Write out pre-calculated business aggregation matrix
        daily_category_sales.write.mode("overwrite") \
            .parquet(str(GOLD_DIR / "aggregations" / "daily_category_sales"))

        logging.info("--- Gold Layer Transformation Completed Successfully ---")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    build_gold_layer()