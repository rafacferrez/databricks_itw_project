from pyspark import pipelines as dp

# 🧰 Get configuration from pipeline parameters
catalog = spark.conf.get("catalog")
bronze_schema = spark.conf.get("bronze_schema")
silver_schema = spark.conf.get("silver_schema")

@dp.table(
    name=f"{catalog}.{silver_schema}.cleaned_order_item"
)
def order_items_silver():

    order_items = (spark.readStream.table(f"{catalog}.{bronze_schema}.order_item")
        .filter("order_id is not null")
        .filter("order_item_id is not null")
        .dropDuplicates(["order_item_id"])
    )

    products = (spark.readStream.table(f"{catalog}.{silver_schema}.cleaned_product")
        .drop("price", "last_modified")
        .selectExpr(
            "product_id",
            "name as product_name",
            "category as product_category"
        )
    )

    return (order_items
        .join(products, "product_id")
    )