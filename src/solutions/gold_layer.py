from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.builder.getOrCreate()


class GoldLayer:
    @staticmethod
    def create_fact_sales(silver_table: str, gold_table: str) -> DataFrame:
        """
        Creates or replaces the fact_sales table
        """
        query = f"""
        CREATE OR REPLACE TABLE {gold_table} AS
        SELECT
            _created_at AS sale_date,
            product_id,
            user_id,
            COUNT(sale_id) AS number_of_sales,
            SUM(quantity) AS total_quantity_sold,
            SUM(quantity * price) AS total_sales_amount,
            ROUND(AVG(price), 2) AS avg_unit_price
        FROM {silver_table}
        WHERE _is_active IS TRUE
        GROUP BY
            _created_at,
            product_id,
            user_id
        """
        return spark.sql(query)

    @staticmethod
    def create_dim_product(silver_table: str, gold_table: str) -> DataFrame:
        """
        Creates or replaces the dim_product table.
        """
        query = f"""
        CREATE OR REPLACE TABLE {gold_table} AS
        SELECT DISTINCT
            product_id,
            name,
            category,
            price
        FROM {silver_table}
        """
        return spark.sql(query)

    @staticmethod
    def create_dim_user(silver_table: str, gold_table: str) -> DataFrame:
        """
        Creates or replaces the dim_user table.
        """
        query = f"""
        CREATE OR REPLACE TABLE {gold_table} AS
        SELECT DISTINCT
            user_id,
            name,
            email,
            phone
        FROM {silver_table}
        WHERE is_active IS TRUE
        """
        return spark.sql(query)
