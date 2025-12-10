from pyspark import pipelines as dp

# 🧰 Get configuration from pipeline parameters
catalog = spark.conf.get("catalog")
bronze_schema = spark.conf.get("bronze_schema")

@dp.table(
    name=f"{catalog}.{bronze_schema}.user"
)
def users_bronze():
    schema = """
        user_id int, name string, email string, phone string, is_active boolean, last_modified date
    """
    return (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .schema(schema)
        .load(f"/Volumes/{catalog}/{bronze_schema}/raw_files/users")
    )