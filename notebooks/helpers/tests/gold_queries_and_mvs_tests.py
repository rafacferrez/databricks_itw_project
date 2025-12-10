import pytest
from helpers import utils


def test_mv_summarized_event_metrics_by_user(**kwargs):
    configs = utils.get_configs("mv_summarized_event_metrics_by_user")
    full_mv = configs["full_table_gold"]

    # Sanity: total_revenue >= 0
    df = utils.spark.sql(f"SELECT SUM(total_revenue) AS total FROM {full_mv}")
    total = df.collect()[0]["total"]
    assert total is not None and total >= 0, "Expected non-negative total_revenue."


def test_mv_summarized_event_metrics_by_product(**kwargs):
    configs = utils.get_configs("mv_summarized_event_metrics_by_product")
    full_mv = configs["full_table_gold"]

    # Sanity: total_events and total_revenue should be positive
    df = utils.spark.sql(f"SELECT SUM(total_events) AS events, SUM(total_revenue) AS revenue FROM {full_mv}")
    res = df.collect()[0]
    assert res["events"] > 0, "Expected positive total_events."
    assert res["revenue"] >= 0, "Expected non-negative total_revenue."


def _schema_and_count_validation(table_name: str, expected_columns: list):
    """
    Validate schema and table count > 0.
    """
    configs = utils.get_configs(table_name)
    full_mv = configs["full_table_gold"]

    # Verify table exists and has rows
    row_count = utils.get_table_row_count(full_mv)
    assert row_count > 0, f"{full_mv} is empty or missing."

    # Check expected schema
    actual_schema = utils.get_table_schema(full_mv)
    actual_columns = {col for col, _ in actual_schema}
    assert expected_columns.issubset(actual_columns), (
        f"{full_mv} schema mismatch.\nExpected ⊆: {expected_columns}\nGot: {actual_columns}"
    )


def test_base_mv_summarized_event_metrics(**kwargs):
    """
    Schema validation for mv_summarized_event_metrics.
    """
    expected_columns = {
        "event_date", "active_users", "total_sessions", "avg_sessions_per_user",
        "total_events", "avg_events_per_user", "total_views", "total_clicks",
        "total_add_to_cart", "total_purchases", "click_through_rate", "cart_abandonment_rate"
    }
    _schema_and_count_validation("mv_summarized_event_metrics", expected_columns)


def test_base_mv_summarized_event_metrics_by_user(**kwargs):
    """
    Schema validation for mv_summarized_event_metrics_by_user.
    """
    expected_columns = {
        "user_id", "user_name", "event_date", "total_sessions", "total_events",
        "total_views", "total_clicks", "total_add_to_cart", "total_purchases", "total_revenue"
    }
    _schema_and_count_validation("mv_summarized_event_metrics_by_user", expected_columns)


def test_base_mv_summarized_event_metrics_by_product(**kwargs):
    """
    Schema validation for mv_summarized_event_metrics_by_product.
    """
    expected_columns = {
        "product_id", "product_name", "product_category", "event_date",
        "total_events", "unique_users", "total_revenue"
    }
    _schema_and_count_validation("mv_summarized_event_metrics_by_product", expected_columns)


def test_base_mv_fact_sales(**kwargs):
    """
    Schema validation for mv_fact_sales.
    """
    expected_columns = {
        "sale_date", "product_id", "user_id", "number_of_sales", "total_quantity_sold", "total_sales_amount", "avg_unit_price",
    }
    _schema_and_count_validation("mv_fact_sales", expected_columns)


def test_base_mv_dim_product(**kwargs):
    """
    Schema validation for mv_dim_product.
    """
    expected_columns = {
        "product_id", "name", "category", "price",
    }
    _schema_and_count_validation("mv_dim_product", expected_columns)


def test_base_mv_dim_user(**kwargs):
    """
    Schema validation for mv_summarized_mv_dim_userevent_metrics.
    """
    expected_columns = {
        "user_id", "name", "email", "phone",
    }
    _schema_and_count_validation("mv_dim_user", expected_columns)


def _table_gold_checksum(table_name: str, src_table_name: str):
    configs = utils.get_configs(table_name)
    table = configs["full_table_gold"]

    src_configs = utils.get_src_configs(src_table_name)
    src_table = src_configs["full_table_gold"]
    spark = utils.get_spark()

    actual = utils.calculate_dataframe_checksum(spark.sql(f"SELECT * FROM {table}"))
    expected = utils.calculate_dataframe_checksum(spark.sql(f"SELECT * FROM {src_table}"))
    assert actual == expected, f"Checksum mismatch: {actual} != {expected}"


def test_check_mv_fact_sales(**kwargs):
    """
    Checksum validation for mv_fact_sales.
    """
    _table_gold_checksum("mv_fact_sales", "fact_sales")


def test_check_mv_dim_product(**kwargs):
    """
    Checksum validation for mv_dim_product.
    """
    _table_gold_checksum("mv_dim_product", "dim_product")


def test_check_mv_dim_user(**kwargs):
    """
    Checksum validation for mv_summarized_mv_dim_userevent_metrics.
    """
    _table_gold_checksum("mv_dim_user", "dim_user")
