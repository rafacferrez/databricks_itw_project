import os
from pyspark.sql import SparkSession
from helpers import utils


def test_bronze_table_schema(**kwargs):
    spark = utils.get_spark()
    catalog_name = utils.get_param("catalog", "capstone_dev")
    base_user = utils.get_base_user_schema()
    schema_bronze = f"{base_user}_bronze"
    table_name = "sales"
    full_table_bronze = f"{catalog_name}.{schema_bronze}.{table_name}"

    actual_schema = utils.get_table_schema(full_table_bronze)
    actual_columns = {col for col, _ in actual_schema}
    expected_columns = {"sale_id", "product_id", "user_id", "qty", "price", "status", "updated_at"}

    assert expected_columns.issubset(actual_columns), (
        f"Expected at least these columns: {expected_columns}, got {actual_columns}"
    )


def test_bronze_table_not_empty(**kwargs):
    catalog_name = utils.get_param("catalog", "capstone_dev")
    base_user = utils.get_base_user_schema()
    schema_bronze = f"{base_user}_bronze"
    full_table_bronze = f"{catalog_name}.{schema_bronze}.sales"

    row_count = utils.get_table_row_count(full_table_bronze)
    assert row_count > 0, "Expected bronze table to have rows."


def test_silver_table_schema(**kwargs):
    catalog_name = utils.get_param("catalog", "capstone_dev")
    base_user = utils.get_base_user_schema()
    schema_silver = f"{base_user}_silver"
    full_table_silver = f"{catalog_name}.{schema_silver}.sales"

    expected_schema = [
        ("sale_id", "int"),
        ("product_id", "int"),
        ("user_id", "int"),
        ("quantity", "int"),
        ("price", "double"),
        ("region", "string"),
        ("_is_active", "boolean"),
        ("_created_at", "date"),
        ("_updated_at", "date"),
        ("_file_name", "string"),
    ]
    actual_schema = utils.get_table_schema(full_table_silver)
    assert actual_schema == expected_schema, f"Schema mismatch: {actual_schema} != {expected_schema}"


def test_silver_active_rows(**kwargs):
    spark = utils.get_spark()
    catalog_name = utils.get_param("catalog", "capstone_dev")
    base_user = utils.get_base_user_schema()
    schema_silver = f"{base_user}_silver"
    full_table_silver = f"{catalog_name}.{schema_silver}.sales"

    df = spark.sql(f"SELECT _is_active FROM {full_table_silver}")
    assert df.count() > 0, "Silver table is empty."
    assert df.filter("_is_active = false").count() > 0, "Expected at least one inactive record."


def test_silver_schema_evolution(**kwargs):
    spark = utils.get_spark()
    catalog_name = utils.get_param("catalog", "capstone_dev")
    base_user = utils.get_base_user_schema()
    schema_silver = f"{base_user}_silver"
    full_table_silver = f"{catalog_name}.{schema_silver}.sales"

    region_count = spark.sql(
        f"SELECT COUNT(*) AS cnt FROM {full_table_silver} WHERE region IS NOT NULL"
    ).collect()[0].cnt

    assert region_count > 0, "Expected some records with non-null region (schema evolution not applied)."

def test_silver_table_checksum(**kwargs):
    table_name = "sales"
    configs = utils.get_configs(table_name)
    table = configs["full_table_silver"]

    src_configs = utils.get_src_configs(table_name)
    src_table = src_configs["full_table_silver"]
    spark = utils.get_spark()

    actual = utils.calculate_dataframe_checksum(spark.sql(f"SELECT * FROM {table}"))
    expected = utils.calculate_dataframe_checksum(spark.sql(f"SELECT * FROM {src_table}"))
    assert actual == expected, f"Checksum mismatch: {actual} != {expected}"
