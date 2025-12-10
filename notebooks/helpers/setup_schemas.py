from pyspark.sql import SparkSession
from helpers import utils

spark = SparkSession.builder.getOrCreate()

ENVS = ("dev", "prd")

LAYERS = ("bronze", "silver", "gold")

CATALOG_PREFIX = "sales"

def create_user_schemas():
    """Create user schemas for each environment and layer."""
    
    for env in ENVS:

        spark.sql(f"USE CATALOG {CATALOG_PREFIX}_{env}")

        for layer in LAYERS:
            schema_name = f"{utils.get_base_user_schema()}_{layer}"
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            print(f"🏗️  Created schema: {CATALOG_PREFIX}_{env}.{schema_name}")

def clean_up_schemas():
    """Clean up user schemas and all tables."""
    
    for env in ENVS:

        spark.sql(f"USE CATALOG {CATALOG_PREFIX}_{env}")

        for layer in LAYERS:
            schema_name = f"{utils.get_base_user_schema()}_{layer}"
            spark.sql(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
            print(f"🧹 Cleaned up schema: {CATALOG_PREFIX}_{env}.{schema_name}")
