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
        to_date(updated_at) AS order_date,
        sale_id as order_id,
        product_id,
        user_id,
        COUNT(sale_id) AS number_of_orders,
        SUM(qty) AS total_quantity,
        ROUND(SUM(qty * price), 2) AS total_orders_amount
    FROM {catalog}.{silver_schema}.cleaned_order
    GROUP BY
        to_date(updated_at),
        order_id,
        product_id,
        user_id
    """)