from pyspark import pipelines as dp

# 🧰 Get configuration from pipeline parameters
catalog = spark.conf.get("catalog")
silver_schema = spark.conf.get("silver_schema")
gold_schema = spark.conf.get("gold_schema")

@dp.materialized_view(
    name=f"{catalog}.{gold_schema}.dim_user"
)
def dim_user():

    return spark.sql(f"""
        SELECT
            DISTINCT
            user_id,
            name as user_name,
            email as user_email,
            phone as user_phone
        FROM {catalog}.{silver_schema}.cleaned_user
    """)