from pyspark import pipelines as dp

# 🧰 Get configuration from pipeline parameters
catalog = spark.conf.get("catalog")
bronze_schema = spark.conf.get("bronze_schema")

@dp.table(
    name=f"{catalog}.{bronze_schema}.order_item"
)
def orders_bronze():
    schema = """
        order_id int, product_id int, order_item_id int, qty int, price double, status string, updated_at timestamp
    """
    return (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .schema(schema)
        .load(f"/Volumes/{catalog}/{bronze_schema}/raw_files/order_items")
    )