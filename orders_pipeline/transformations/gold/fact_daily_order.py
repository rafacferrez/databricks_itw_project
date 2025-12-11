from pyspark import pipelines as dp

# 🧰 Get configuration from pipeline parameters
catalog = spark.conf.get("catalog")
silver_schema = spark.conf.get("silver_schema")
gold_schema = spark.conf.get("gold_schema")

@dp.materialized_view(
    name=f"{catalog}.{gold_schema}.fact_daily_order"
)
def dim_user():

    return spark.sql(f"""
    SELECT
        to_date(co.updated_at) AS order_date,
        co.order_id as order_id,
        coi.product_id,
        co.user_id,
        COUNT(co.order_id) AS number_of_orders,
        SUM(coi.qty) AS total_quantity,
        ROUND(SUM(coi.qty * coi.price), 2) AS total_orders_amount
    FROM {catalog}.{silver_schema}.cleaned_order co
    INNER JOIN {catalog}.{silver_schema}.cleaned_order_item coi
    ON co.order_id = coi.order_id
    GROUP BY
        to_date(co.updated_at),
        co.order_id,
        coi.product_id,
        co.user_id
    """)