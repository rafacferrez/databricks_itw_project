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
        .filter("sale_id is not null")
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

    products = (spark.readStream.table(f"{catalog}.{silver_schema}.cleaned_product")
        .drop("price", "last_modified")
        .selectExpr(
            "product_id",
            "name as product_name",
            "category as product_category"
        )
    )

    return (orders
        .join(users, "user_id")
        .join(products, "product_id")
    )