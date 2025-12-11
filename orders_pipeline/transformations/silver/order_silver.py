from pyspark import pipelines as dp

# 🧰 Get configuration from pipeline parameters
catalog = spark.conf.get("catalog")
bronze_schema = spark.conf.get("bronze_schema")
silver_schema = spark.conf.get("silver_schema")

@dp.table(
    name=f"{catalog}.{silver_schema}.cleaned_order"
)
def orders_silver():

    orders = (spark.readStream.table(f"{catalog}.{bronze_schema}.order")
        .filter("order_id is not null")
        .dropDuplicates(["order_id"])
    )

    users = (spark.readStream.table(f"{catalog}.{silver_schema}.cleaned_user")
        .drop("is_active", "last_modified")
        .selectExpr(
            "user_id",
            "name as user_name",
            "email as user_email",
            "phone as user_phone"
        )
    )

    return (orders
        .join(users, "user_id")
    )