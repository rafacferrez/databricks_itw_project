import pytest
from helpers import utils


def _raw_file_count(table_name):
    configs = utils.get_configs(table_name)
    catalog_name = configs["catalog"]
    schema_bronze = configs["schema_bronze"]

    volume_path = f"/Volumes/{catalog_name}/{schema_bronze}/raw_files/{table_name}"
    files = utils.list_files(volume_path)
    assert len(files) > 0, f"Expected files in {volume_path}, but found none."

def test_raw_file_count_products(**kwargs):
    _raw_file_count("products")

def test_raw_file_count_users(**kwargs):
    _raw_file_count("users")

def test_bronze_table_schema_products(**kwargs):
    configs = utils.get_configs("products")
    table_bronze = configs["full_table_bronze"]

    expected_schema = [
        ("product_id", "int"),
        ("name", "string"),
        ("category", "string"),
        ("price", "double"),
        ("last_modified", "date"),
    ]
    actual_schema = utils.get_table_schema(f"{table_bronze}")
    assert actual_schema == expected_schema, f"Schema mismatch: {actual_schema} != {expected_schema}"

def test_bronze_table_schema_users(**kwargs):
    configs = utils.get_configs("users")
    table_bronze = configs["full_table_bronze"]

    expected_schema = [
        ("user_id", "int"),
        ("name", "string"),
        ("email", "string"),
        ("phone", "string"),
        ("is_active", "boolean"),
        ("last_modified", "date"),
    ]
    actual_schema = utils.get_table_schema(f"{table_bronze}")
    assert actual_schema == expected_schema, f"Schema mismatch: {actual_schema} != {expected_schema}"

def _bronze_table_row_count(table_name):
    configs = utils.get_configs(table_name)
    table_bronze = configs["full_table_bronze"]
    row_count = utils.get_table_row_count(f"{table_bronze}")
    assert row_count > 0, "Expected non-zero rows in bronze table."

def test_bronze_table_row_count_users(**kwargs):
    _bronze_table_row_count("users")

def test_bronze_table_row_count_products(**kwargs):
    _bronze_table_row_count("products")

def _silver_table_deduplication(table_name: str, column_id: str):
    configs = utils.get_configs(table_name)
    table_silver = configs["full_table_silver"]
    spark = utils.get_spark()

    query = f"""
    SELECT {column_id}, COUNT(*)
    FROM {table_silver}
    GROUP BY {column_id}
    HAVING COUNT(*) > 1
    """
    duplicates = spark.sql(query).count()
    assert duplicates == 0, f"Found duplicate {column_id} values in silver table."

def test_silver_table_deduplication_products(**kwargs):
    _silver_table_deduplication("products", "product_id")

def test_silver_table_deduplication_users(**kwargs):
    _silver_table_deduplication("users", "user_id")

def _silver_table_transformation(table_name: str, column_id: str):
    configs = utils.get_configs(table_name)
    table_bronze = configs["full_table_bronze"]
    table_silver = configs["full_table_silver"]
    spark = utils.get_spark()

    query = f"""
    select b.{column_id}, b.max_date, s.last_modified
    From (
        SELECT {column_id}, MAX(last_modified) AS max_date
        FROM {table_bronze}
        GROUP BY {column_id}
    ) AS b
    join (
        SELECT {column_id}, last_modified
        FROM {table_silver}
    ) s
    On b.{column_id} = s.{column_id}
    Where b.max_date != s.last_modified
    """
    actual = spark.sql(query).collect()
    expected = []

    assert expected == actual, "Transformation integrity check failed."

def test_silver_table_transformation_products(**kwargs):
    _silver_table_transformation("products", "product_id")

def test_silver_table_transformation_users(**kwargs):
    _silver_table_transformation("users", "user_id")

def _table_silver_checksum(table_name: str):
    configs = utils.get_configs(table_name)
    table_silver = configs["full_table_silver"]

    src_configs = utils.get_src_configs(table_name)
    src_table_silver = src_configs["full_table_silver"]
    spark = utils.get_spark()

    actual = utils.calculate_dataframe_checksum(spark.sql(f"SELECT * FROM {table_silver}"))
    expected = utils.calculate_dataframe_checksum(spark.sql(f"SELECT * FROM {src_table_silver}"))
    assert actual == expected, f"Checksum mismatch: {actual} != {expected}"

def test_silver_table_checksum_users(**kwargs):
    _table_silver_checksum("users")
    #expected="b44ed44f2ca99b0844116fe0b1fac1565f8605765959d601beb8ecbb95a683db"

def test_silver_table_checksum_products(**kwargs):
    _table_silver_checksum("products")
    #expected="106b853ee970e51239c3eaccbad2c22322c081d936f4bdc7cd8fc9f2370fb44c"
