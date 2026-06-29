from pathlib import Path
import os
import logging
import random
from datetime import datetime, timedelta

import psycopg2
from psycopg2.extras import execute_values
from faker import Faker
import yaml

######################################################################################################
# Dynamic path handling
######################################################################################################

if os.environ.get("AIRFLOW_HOME"):
    MAIN_PROJECT_DIR = Path("/opt/airflow")
else:
    MAIN_PROJECT_DIR = Path(__file__).resolve().parent.parent

CONFIG_DIR = MAIN_PROJECT_DIR / "config"
config_path = CONFIG_DIR / "pipeline_config.yaml"

######################################################################################################
# Logging
######################################################################################################

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

######################################################################################################
# Faker
######################################################################################################

fake = Faker()

######################################################################################################
# Simulation Configuration
######################################################################################################

NEW_PRODUCTS = 20
UPDATED_PRODUCTS = 10
NEW_CUSTOMERS = 50
NEW_ORDERS = 300

######################################################################################################
# Configuration Loader
######################################################################################################

def load_pipeline_config(config_path=config_path):
    """
    Loads configurations and adapts networking automatically
    for Docker or local execution.
    """

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    if os.environ.get("AIRFLOW_HOME"):
        config["database"]["source_oltp"]["host"] = "source_oltp_db"
        config["database"]["source_oltp"]["port"] = 5432

    return config

######################################################################################################
# Database Connection
######################################################################################################

def get_db_connection(creds):

    return psycopg2.connect(
        host=creds["host"],
        port=creds["port"],
        database=creds["database"],
        user=creds["user"],
        password=creds["password"]
    )


######################################################################################################

def get_next_id(cur, table_name, id_column):
    """
    Returns the next available primary key for a table.
    """

    cur.execute(f"SELECT COALESCE(MAX({id_column}), 0) FROM {table_name};")
    return cur.fetchone()[0] + 1


######################################################################################################

def get_latest_timestamp(cur, table_name, timestamp_column):
    """
    Returns the latest timestamp currently present in the table.
    """

    cur.execute(f"""
        SELECT COALESCE(MAX({timestamp_column}), '1970-01-01')
        FROM {table_name};
    """)

    return cur.fetchone()[0]

######################################################################################################

def generate_timestamps(start_timestamp, number_of_rows):
    """
    Generates timestamps newer than the current maximum timestamp.
    """

    timestamps = []

    current = start_timestamp

    for _ in range(number_of_rows):

        current += timedelta(seconds=1)
        timestamps.append(current)

    return timestamps

######################################################################################################

def get_random_product_ids(cur, number_to_update):
    """
    Returns random existing product IDs.
    """

    cur.execute(f"""
        SELECT product_id
        FROM source_products
        ORDER BY RANDOM()
        LIMIT {number_to_update};
    """)

    return [row[0] for row in cur.fetchall()]

######################################################################################################

PRODUCT_CATEGORIES = [
    "Electronics",
    "Apparel",
    "Home & Kitchen",
    "Books",
    "Beauty"
]

PRODUCT_TEMPLATES = {

    "Electronics": [
        "Pro Phone",
        "Ultra Laptop",
        "Wireless Buds",
        "Smart Watch",
        "4K Monitor"
    ],

    "Apparel": [
        "Slim Fit Jeans",
        "Classic Hoodie",
        "Running Shoes",
        "Leather Jacket",
        "Formal Shirt"
    ],

    "Home & Kitchen": [
        "Blender Pro",
        "Air Fryer XL",
        "Coffee Maker",
        "Chef Knife Set",
        "Ergonomic Pillow"
    ],

    "Books": [
        "Data Engineering Guide",
        "Mystery Novel",
        "Sci-Fi Trilogy",
        "Python Cookbook",
        "History Blueprint"
    ],

    "Beauty": [
        "Hydrating Serum",
        "Matte Lipstick",
        "Sunscreen SPF50",
        "Clay Face Mask",
        "Beard Oil"
    ]
}


######################################################################################################

def simulate_products(cur):
    """
    Simulates new products and updates existing products.
    """

    logging.info("---------------------------------------------------")
    logging.info("Simulating Product Activity")
    logging.info("---------------------------------------------------")

    next_product_id = get_next_id(
        cur,
        "source_products",
        "product_id"
    )

    latest_timestamp = get_latest_timestamp(
        cur,
        "source_products",
        "updated_at"
    )

    timestamps = generate_timestamps(
        latest_timestamp,
        NEW_PRODUCTS + UPDATED_PRODUCTS
    )

    ####################################################################
    # Insert New Products
    ####################################################################

    new_products = []

    for i in range(NEW_PRODUCTS):

        category = random.choice(PRODUCT_CATEGORIES)

        product_name = (
            f"{fake.company()} "
            f"{random.choice(PRODUCT_TEMPLATES[category])}"
        )

        new_products.append(

            (
                next_product_id + i,
                product_name,
                round(random.uniform(500, 50000), 2),
                category,
                random.randint(20, 1000),
                timestamps[i]
            )

        )

    execute_values(
        cur,
        """
        INSERT INTO source_products
        (
            product_id,
            product_name,
            price,
            category,
            stock_quantity,
            updated_at
        )
        VALUES %s;
        """,
        new_products
    )

    logging.info(f"Inserted {NEW_PRODUCTS} new products.")

    ####################################################################
    # Update Existing Products
    ####################################################################

    ids = get_random_product_ids(cur, UPDATED_PRODUCTS)

    for index, product_id in enumerate(ids):

        cur.execute(
            """
            UPDATE source_products
            SET
                price=%s,
                stock_quantity=%s,
                updated_at=%s
            WHERE product_id=%s;
            """,
            (
                round(random.uniform(500, 50000), 2),
                random.randint(20, 1000),
                timestamps[NEW_PRODUCTS + index],
                product_id
            )
        )

    logging.info(f"Updated {UPDATED_PRODUCTS} existing products.")


######################################################################################################

def simulate_customers(cur):
    """
    Inserts new customers.
    """

    logging.info("---------------------------------------------------")
    logging.info("Simulating Customer Activity")
    logging.info("---------------------------------------------------")

    next_customer_id = get_next_id(
        cur,
        "source_customers",
        "customer_id"
    )

    latest_timestamp = get_latest_timestamp(
        cur,
        "source_customers",
        "created_at"
    )

    timestamps = generate_timestamps(
        latest_timestamp,
        NEW_CUSTOMERS
    )

    customers = []

    for i in range(NEW_CUSTOMERS):

        customers.append(

            (
                next_customer_id + i,
                fake.first_name(),
                fake.last_name(),
                fake.unique.email(),
                fake.city(),
                timestamps[i]
            )

        )

    execute_values(
        cur,
        """
        INSERT INTO source_customers
        (
            customer_id,
            first_name,
            last_name,
            email,
            city,
            created_at
        )
        VALUES %s;
        """,
        customers
    )

    logging.info(f"Inserted {NEW_CUSTOMERS} new customers.")


######################################################################################################

def simulate_orders(cur):
    """
    Inserts new orders.
    """

    logging.info("---------------------------------------------------")
    logging.info("Simulating Order Activity")
    logging.info("---------------------------------------------------")

    next_order_id = get_next_id(
        cur,
        "source_orders",
        "order_id"
    )

    latest_timestamp = get_latest_timestamp(
        cur,
        "source_orders",
        "updated_at"
    )

    timestamps = generate_timestamps(
        latest_timestamp,
        NEW_ORDERS
    )

    ####################################################################
    # Existing IDs
    ####################################################################

    cur.execute("SELECT MAX(customer_id) FROM source_customers;")
    max_customer = cur.fetchone()[0]

    cur.execute("SELECT MAX(product_id) FROM source_products;")
    max_product = cur.fetchone()[0]

    ####################################################################
    # Build Orders
    ####################################################################

    orders = []

    for i in range(NEW_ORDERS):

        quantity = random.randint(1, 4)

        unit_price = random.choice(
            [
                500,
                1200,
                4500,
                15000
            ]
        )

        orders.append(

            (
                next_order_id + i,
                random.randint(1, max_customer),
                random.randint(1, max_product),
                quantity,
                round(quantity * unit_price, 2),
                timestamps[i]
            )

        )

    execute_values(
        cur,
        """
        INSERT INTO source_orders
        (
            order_id,
            customer_id,
            product_id,
            quantity,
            total_amount,
            updated_at
        )
        VALUES %s;
        """,
        orders
    )

    logging.info(f"Inserted {NEW_ORDERS} new orders.")








######################################################################################################

def main():

    logging.info("===================================================")
    logging.info("Starting Incremental Load Simulator")
    logging.info("===================================================")

    config = load_pipeline_config()

    creds = config["database"]["source_oltp"]

    try:

        with get_db_connection(creds) as conn:
            with conn.cursor() as cur:

                logging.info("Connected successfully.")

                simulate_products(cur)
                simulate_customers(cur)
                simulate_orders(cur)
                
                conn.commit()

                logging.info("Incremental simulation completed successfully.")

    except Exception as e:

        logging.error(f"Simulation failed: {e}")
        raise

######################################################################################################

if __name__ == "__main__":
    main()