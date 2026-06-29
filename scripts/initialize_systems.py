from pathlib import Path
import os
import logging 
import random
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values
from faker import Faker
import yaml


# If running inside Docker, use the standard Airflow home path directly
if os.environ.get("AIRFLOW_HOME"):
    MAIN_PROJECT_DIR = Path("/opt/airflow")
else:
    MAIN_PROJECT_DIR = Path(__file__).resolve().parent.parent

CONFIG_DIR = MAIN_PROJECT_DIR / 'config'
config_path = CONFIG_DIR / 'pipeline_config.yaml'

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# Initialize Faker with a fixed seed for reproducible test datasets
fake = Faker()
Faker.seed(42)
random.seed(42)


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

def get_db_connection(creds):
    """Establishes a connection to a specific PostgreSQL database instance."""
    return psycopg2.connect(
        host=creds.get("host", "localhost"),
        port=int(creds.get("port", 5432)),
        database=creds.get("database"),
        user=creds.get("user"),
        password=creds.get("password")
    )


def init_source_oltp(config):
    """Creates tables and streams high-volumn mocked operational data into Source oltp."""
    logging.info("connecting to SOURCE OLTP Database on port 5433...")
    creds = config["database"]["source_oltp"]

    queries_create_tables = [
        """
        DROP TABLE source_customers;
        CREATE TABLE IF NOT EXISTS source_customers (
            customer_id INT Primary Key,
            first_name VARCHAR(100) NOT NULL,
            last_name VARCHAR(100) NOT NULL,
            email VARCHAR(150) NOT NULL,
            city VARCHAR(100) NOT NULL,
            created_at TIMESTAMP NOT NULL
        );
        """,

        """
        DROP TABLE source_products;
        CREATE TABLE IF NOT EXISTS source_products (
            product_id INT PRIMARY KEY,
            product_name VARCHAR(255) NOT NULL,
            price NUMERIC(10, 2) NOT NULL,
            category VARCHAR(100) NOT NULL,
            stock_quantity INT NOT NULL,
            updated_at TIMESTAMP NOT NULL
        );
        """,

        """
        DROP TABLE source_orders;
        CREATE TABLE IF NOT EXISTS source_orders (
            order_id INT PRIMARY KEY,
            customer_id INT NOT NULL,
            product_id INT NOT NULL,
            quantity INT NOT NULL,
            total_amount NUMERIC(10, 2) NOT NULL,
            updated_at TIMESTAMP NOT NULL
        );
        """
    ]

    try:
        with get_db_connection(creds) as conn:
            with conn.cursor() as cur:
                # 1. Rebuild clean architecture schemas
                for q in queries_create_tables:
                    cur.execute(q)
                logging.info("Source database relational tables verified/created successfully.")

                # check if data already exists to prevent redundant long processing loops
                cur.execute("SELECT COUNT(*) FROM source_products;")
                if cur.fetchone()[0] > 0:
                    logging.info("Source database already populated. Skipping heavy generation phase.")
                    return 

                # 2. Generate and stream 6,000 Products
                logging.info("Generating 6,000 robust product rows...")
                categories = ['Electronics', 'Apparel', 'Home & Kitchen', 'Books', 'Beauty']
                product_templates = {
                    'Electronics': ['Pro Phone', 'Ultra Laptop', 'Wireless Buds', 'Smart Watch', '4K Monitor'],
                    'Apparel': ['Slim Fit Jeans', 'Classic Hoodie', 'Running Shoes', 'Leather Jacket', 'Formal Shirt'],
                    'Home & Kitchen': ['Blender Pro', 'Air Fryer XL', 'Coffee Maker', 'Chef Knife Set', 'Ergonomic Pillow'],
                    'Books': ['Data Engineering Guide', 'Mystery Novel', 'Sci-Fi Trilogy', 'Python Cookbook', 'History Blueprint'],
                    'Beauty': ['Hydrating Serum', 'Matte Lipstick', 'Sunscreen SPF50', 'Clay Face Mask', 'Beard Oil']
                }

                products_data = []
                base_time = datetime(2026, 1, 1, 10, 0, 0)
                for p_id in range(1, 6001):
                    cat = random.choice(categories)
                    name = f"{fake.company()} {random.choice(product_templates[cat])}"
                    price = round(random.uniform(299.00, 95000.00), 2)
                    stock = random.randint(10, 1000)
                    p_time = base_time + timedelta(minutes=p_id)
                    products_data.append((p_id, name, price, cat, stock, p_time))

                execute_values(
                    cur, 
                    """
                    INSERT INTO source_products (product_id, product_name, price, category, stock_quantity, updated_at)
                    VALUES %s ON CONFLICT (product_id) DO NOTHING;
                    """,
                    products_data)
                logging.info("Successfully pushed 6,000 rows to 'source_products'.")

                # 3. Generate and stream 10,000 Customers
                logging.info("Generating 10,000 customer records in memory-safe chunks...")
                customers_batch = []
                for c_id in range(1, 10001):
                    c_time = base_time + timedelta(minutes=c_id)
                    customers_batch.append((
                        c_id, 
                        fake.first_name(), 
                        fake.last_name(), 
                        fake.unique.email(),
                        fake.city(),
                        c_time
                    ))

                    if len(customers_batch) == 5000:
                        execute_values(cur, """
                            INSERT INTO source_customers (customer_id, first_name, last_name, email, city, created_at)
                            VALUES %s ON CONFLICT (customer_id) DO NOTHING;
                        """, customers_batch)
                        customers_batch.clear()

                if customers_batch:
                    execute_values(cur, """INSERT INTO source_customers VALUES %s ON CONFLICT DO NOTHING;""", customers_batch)
                logging.info("Successfully pushed 10,000 rows to 'source_customers'.")


                # 4. Generate and stream 100,000 Transactional Orders
                logging.info("Starting high-volume streaming for 100,000 orders...")
                orders_batch = []
                order_time_start = datetime(2026, 1, 1, 0, 0, 0)
                
                for o_id in range(1, 100001):
                    cust_id = random.randint(1, 10000)
                    prod_id = random.randint(1, 6001)
                    qty = random.randint(1, 4)
                    
                    # Compute pseudo-realistic baseline total pricing
                    mock_price = random.choice([500.00, 1200.00, 4500.00, 15000.00])
                    total = round(qty * mock_price, 2)
                    
                    # Linearly spread the order events continuously across a 5-month timeline
                    o_time = order_time_start + timedelta(seconds=o_id * 130)
                    orders_batch.append((o_id, cust_id, prod_id, qty, total, o_time))
                    
                    if len(orders_batch) == 10000:
                        execute_values(cur, """
                            INSERT INTO source_orders (order_id, customer_id, product_id, quantity, total_amount, updated_at)
                            VALUES %s ON CONFLICT (order_id) DO NOTHING;
                        """, orders_batch)
                        conn.commit()  # Flush transactions to disk to clear memory overhead
                        logging.info(f"Buffered and written {o_id} / 100,000 rows into 'source_orders' table.")
                        orders_batch.clear()

                if orders_batch:
                    execute_values(cur, """INSERT INTO source_orders VALUES %s ON CONFLICT DO NOTHING;""", orders_batch)
                logging.info("Successfully populated 100,000 rows inside 'source_orders'.")

    except Exception as e:
        logging.error(f"Failed to populate Source DB: {e}")
        raise e



def init_target_olap(config):
    """Creates the warehouse metadata tracking schema and logs our pipeline low watermark."""
    logging.info("Connecting to TARGET OLAP Data Warehouse on port 5434...")
    creds = config["database"]["target_olap"]
    
    query_create_metadata = """
    DROP TABLE etl_metadata;
    CREATE TABLE IF NOT EXISTS etl_metadata (
        pipeline_name VARCHAR(100),
        source_table VARCHAR(100),
        last_success_watermark TIMESTAMP NOT NULL,
        last_run_status VARCHAR(50) NOT NULL,
        PRIMARY KEY (pipeline_name, source_table)
    );
    """
    
    query_seed_metadata = """
    INSERT INTO etl_metadata (pipeline_name, source_table, last_success_watermark, last_run_status)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (pipeline_name, source_table) DO NOTHING;
    """
    
    pipeline_name = config["pipeline"]["name"]
    tables_to_track = ['source_products', 'source_customers', 'source_orders']
    
    try:
        with get_db_connection(creds) as conn:
            with conn.cursor() as cur:
                cur.execute(query_create_metadata)
                logging.info("Verified 'etl_metadata' control table inside the target analytical warehouse.")
                
                for t in tables_to_track:
                    cur.execute(query_seed_metadata, (pipeline_name, t, '1970-01-01 00:00:00', 'INITIALIZED'))
                logging.info("All pipeline tracking low-watermarks seeded inside target DW.")
    except Exception as e:
        logging.error(f"Failed to initialize Target OLAP metadata control structure: {e}")
        raise e

if __name__ == "__main__":
    logging.info("--- Starting Scaled System Ingestion Seeding Phase ---")
    start_time = datetime.now()
    try:
        pipeline_config = load_pipeline_config()
        init_source_oltp(pipeline_config)
        init_target_olap(pipeline_config)
        duration = datetime.now() - start_time
        logging.info(f"--- Initialization Successfully Finished in {duration.total_seconds():.2f} seconds ---")
    except Exception as main_err:
        logging.critical(f"System generation crashed completely: {main_err}")


