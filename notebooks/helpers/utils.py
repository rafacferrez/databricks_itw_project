from pyspark.sql import SparkSession
from typing import List, Dict
from pyspark.dbutils import DBUtils
import hashlib


spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

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
