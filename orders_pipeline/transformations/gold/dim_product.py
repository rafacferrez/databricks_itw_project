from pyspark import pipelines as dp

# 🧰 Get configuration from pipeline parameters
catalog = spark.conf.get("catalog")
silver_schema = spark.conf.get("silver_schema")
gold_schema = spark.conf.get("gold_schema")

@dp.materialized_view(
    name=f"{catalog}.{gold_schema}.dim_product"
)
def dim_user():

    return spark.sql(f"""
        SELECT
            DISTINCT
            product_id,
            name as product_name,
            category as product_category,
            price as product_price
        FROM {catalog}.{silver_schema}.cleaned_product
    """)