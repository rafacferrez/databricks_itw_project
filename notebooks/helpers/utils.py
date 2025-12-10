from pyspark.sql import SparkSession
from typing import List, Dict
from pyspark.dbutils import DBUtils
import hashlib


spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)


def get_configs(table_name: str) -> Dict:
    """
    Get configs based on current user and table name.
    """
    catalog_name = get_param("catalog", "capstone_dev")
    base_user = get_base_user_schema()
    schema_bronze = f"{base_user}_bronze"
    schema_silver = f"{base_user}_silver"
    schema_gold = f"{base_user}_gold"
    full_table_bronze = f"{catalog_name}.{schema_bronze}.{table_name}"
    full_table_silver = f"{catalog_name}.{schema_silver}.{table_name}"
    full_table_gold = f"{catalog_name}.{schema_gold}.{table_name}"
    configs = {
        "catalog": catalog_name,
        "schema_bronze": schema_bronze,
        "schema_silver": schema_silver,
        "schema_gold": schema_gold,
        "full_table_bronze": full_table_bronze,
        "full_table_silver": full_table_silver,
        "full_table_gold": full_table_gold,
    }
    return configs

def get_src_configs(table_name: str, env: str = "dev") -> Dict:
    """
    Get source configs based on a table name.
    """
    catalog_name = "capstone_src"
    schema_bronze = f"{env}_bronze"
    schema_silver = f"{env}_silver"
    schema_gold = f"{env}_gold"
    full_table_bronze = f"{catalog_name}.{schema_bronze}.{table_name}"
    full_table_silver = f"{catalog_name}.{schema_silver}.{table_name}"
    full_table_gold = f"{catalog_name}.{schema_gold}.{table_name}"
    configs = {
        "catalog": catalog_name,
        "schema_bronze": schema_bronze,
        "schema_silver": schema_silver,
        "schema_gold": schema_gold,
        "full_table_bronze": full_table_bronze,
        "full_table_silver": full_table_silver,
        "full_table_gold": full_table_gold,
    }
    return configs

def get_base_user_schema():
    """Get base user schema name based on current user."""
    user_email = spark.sql('SELECT current_user()').collect()[0][0]
    user_name = user_email.split("@")[0]
    user_schema = user_name.replace(".", "_")
    return user_schema


def get_spark():
    """Return or create a Spark session."""
    return SparkSession.builder.getOrCreate()


def path_exists(path: str) -> bool:
    """Check if a path exists in DBFS or local FS."""
    spark = get_spark()
    try:
        return len(spark._jvm.dbutils.fs.ls(path)) > 0
    except Exception:
        return False

def list_files(path: str) -> List[str]:
    files = [file.name for file in dbutils.fs.ls(path)]
    return files


def get_table_schema(table_name: str) -> List[tuple]:
    df = spark.table(table_name)
    return [(field.name, field.dataType.simpleString()) for field in df.schema.fields]


def get_table_row_count(table_name: str) -> int:
    return spark.table(table_name).count()


def get_param(name, default):
    try:
        return dbutils.widgets.get(name)
    except:
        return default


def calculate_dataframe_checksum(df) -> str:
    # Collect the DataFrame rows and convert to a sorted list of strings
    rows = df.collect()
    rows_as_strings = [str(row) for row in rows]
    rows_as_strings.sort()

    # Calculate the checksum
    checksum = hashlib.sha256()
    for row in rows_as_strings:
        checksum.update(row.encode('utf-8'))

    return checksum.hexdigest()
