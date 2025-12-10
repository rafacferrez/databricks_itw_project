from pyspark import pipelines as dp

# 🧰 Get configuration from pipeline parameters
catalog = spark.conf.get("catalog")
bronze_schema = spark.conf.get("bronze_schema")
silver_schema = spark.conf.get("silver_schema")

@dp.table(
    name=f"{catalog}.{silver_schema}.cleaned_user"
)
def users_silver():

    return (spark.readStream.table(f"{catalog}.{bronze_schema}.user")
        .filter("is_active is true")
        .filter("user_id is not null")
    )