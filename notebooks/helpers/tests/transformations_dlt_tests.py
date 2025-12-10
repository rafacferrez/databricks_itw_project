import pytest
from helpers import utils


def test_silver_table_schema_and_row_count(**kwargs):
    configs = utils.get_configs("web_site_events")
    silver_table_path = configs["full_table_silver"]

    # Verify table exists and is not empty
    row_count = utils.get_table_row_count(silver_table_path)
    assert row_count > 0, f"{silver_table_path} is empty or missing."

    # Check expected columns
    actual_schema = utils.get_table_schema(silver_table_path)
    actual_columns = {col for col, _ in actual_schema}
    expected_columns = {
        "event_id", "event_timestamp", "event_type", "session_id", "device_type",
        "endpoint", "referrer_url", "user_id", "user_name", "user_email",
        "user_phone", "is_active_user", "product_id", "product_name",
        "product_category", "product_price"
    }

    assert expected_columns.issubset(actual_columns), (
        f"{silver_table_path} schema mismatch.\nExpected ⊆: {expected_columns}\nGot: {actual_columns}"
    )

def test_event_id_count_consistency_between_bronze_and_silver(**kwargs):
    configs = utils.get_configs("web_site_events")
    bronze_table_path = configs["full_table_bronze"]
    silver_table_path = configs["full_table_silver"]

    # Check if the total number of event IDs matches between Bronze and Silver tables
    bronze_event_count = utils.spark.sql(f"SELECT COUNT(event_id) AS count FROM {bronze_table_path} where user_id is not null").collect()[0]["count"]
    silver_event_count = utils.spark.sql(f"SELECT COUNT(event_id) AS count FROM {silver_table_path}").collect()[0]["count"]

    assert bronze_event_count == silver_event_count, (
        f"Mismatch in total event IDs between Bronze and Silver tables.\n"
        f"Bronze: {bronze_event_count}, Silver: {silver_event_count}"
    )

    print("Total event IDs match between Bronze and Silver tables.")

def test_positive_product_prices_in_silver(**kwargs):
    configs = utils.get_configs("web_site_events")
    silver_table_path = configs["full_table_silver"]

    # Verify table exists and is not empty
    row_count = utils.get_table_row_count(silver_table_path)
    assert row_count > 0, f"{silver_table_path} is empty or missing."

    # Check for non-positive product prices
    non_positive_prices = utils.spark.sql(
        f"SELECT COUNT(*) AS count FROM {silver_table_path} WHERE product_price <= 0"
    ).collect()[0]["count"]

    assert non_positive_prices == 0, (
        f"Found {non_positive_prices} rows in {silver_table_path} with non-positive product_price."
    )

    print("All product prices in the Silver table are positive.")
