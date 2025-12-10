from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.builder.getOrCreate()


class BatchIngestion:
    def __init__(self):
        self.name = "BatchIngestion"

    def create_bronze_products(self, full_table_name: str) -> DataFrame:
        """
        Create a bronze (raw) table for product data.
        Args:
            full_table_name (str): Fully qualified table name in the format 'catalog.schema.table'.
        Returns:
            DataFrame: Result of the Spark SQL execution.
        """
        query = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            product_id INT,
            name STRING,
            category STRING,
            price DOUBLE,
            last_modified DATE
        )
        PARTITIONED BY (last_modified)
        """
        return spark.sql(query)
    
    def create_bronze_users(self, full_table_name: str) -> DataFrame:
        """
        Create a bronze (raw) table for user data.
        Args:
            full_table_name (str): Fully qualified table name in the format 'catalog.schema.table'.
        Returns:
            DataFrame: Result of the Spark SQL execution.
        """
        query = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            user_id INT,
            name STRING,
            email STRING,
            phone STRING,
            is_active BOOLEAN,
            last_modified DATE
        )
        PARTITIONED BY (last_modified)
        """
        return spark.sql(query)
    
    def copy_into(self, full_table_name: str, data_path) -> DataFrame:
        """
        Load data from a file path (CSV) into an existing table.
        Args:
            full_table_name (str): Fully qualified table name in the format 'catalog.schema.table'.
            data_path (str): Path to the CSV data source.
        Returns:
            DataFrame: Result of the Spark SQL execution.
        """
        query = f"""
        COPY INTO {full_table_name}
        FROM '{data_path}'
        FILEFORMAT = CSV
        FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
        COPY_OPTIONS ('mergeSchema' = 'true')
        """
        return spark.sql(query)
    
    def create_silver_products(self, full_table_bronze: str, full_table_silver: str) -> DataFrame:
        """
        Create or replace a silver (curated) products table by deduplicating and selecting the most recent records.
        Args:
            full_table_bronze (str): Fully qualified table name in the format 'catalog.schema.table' for the bronze origin table.
            full_table_silver (str): Fully qualified table name in the format 'catalog.schema.table' for the silver target table.
        Returns:
            DataFrame: Result of the Spark SQL execution.
        """
        query = f"""
        CREATE OR REPLACE TABLE {full_table_silver} AS
        WITH ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (
                PARTITION BY product_id
                ORDER BY last_modified DESC
                ) AS rn
            FROM {full_table_bronze}
        )
        SELECT product_id, name, category, price, last_modified
        FROM ranked
        WHERE rn = 1
        """
        return spark.sql(query)

    def create_silver_users(self, full_table_bronze: str, full_table_silver: str) -> DataFrame:
        """
        Create or replace a silver (curated) users table by deduplicating and selecting the most recent records.
        Args:
            full_table_bronze (str): Fully qualified table name in the format 'catalog.schema.table' for the bronze origin table.
            full_table_silver (str): Fully qualified table name in the format 'catalog.schema.table' for the silver target table.
        Returns:
            DataFrame: Result of the Spark SQL execution.
        """
        query = f"""
        CREATE OR REPLACE TABLE {full_table_silver} AS
        WITH ranked AS (
        SELECT *,
            ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY last_modified DESC
            ) AS rn
        FROM {full_table_bronze}
        )
        SELECT user_id, name, email, phone, is_active, last_modified
        FROM ranked
        WHERE rn = 1
        """
        return spark.sql(query)
