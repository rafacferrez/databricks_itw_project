from pyspark.sql import SparkSession
from helpers import utils

spark = SparkSession.builder.getOrCreate()

ENVS = ("dev", "prd")

VOLUMES = (
    "raw_files",
    "checkpoint_files",
    "schema_files"
)

def create_volumes():
    """Create volumes for each data entities"""
    
    for env in ENVS:

        spark.sql(f"USE CATALOG capstone_{env}")

        schema_name = f"{utils.get_base_user_schema()}_bronze"

        for volume in VOLUMES:

            spark.sql(f"CREATE VOLUME IF NOT EXISTS {schema_name}.{volume}")
            print(f"🏗️  Created volume: capstone_{env}.{schema_name}.{volume}")


def clean_up_volumes():
    """clean up volumes for each data entities"""
    
    for env in ENVS:

        spark.sql(f"USE CATALOG capstone_{env}")

        schema_name = f"{utils.get_base_user_schema()}_bronze"

        for volume in VOLUMES:

            spark.sql(f"DROP VOLUME IF EXISTS {schema_name}.{volume}")
            print(f"🏗️  Cleaned up volume: capstone_{env}.{schema_name}.{volume}")
