from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.builder.getOrCreate()


class CDCMerge:
    def __init__(self):
        self.name = "CDCMerge"

    def create_bronze_sales(self, full_table_name: str) -> DataFrame:
        """
        Create a bronze (raw) table for sales data.
        Args:
            full_table_name (str): Fully qualified table name in the format 'catalog.schema.table'.
        Returns:
            DataFrame: Result of the Spark SQL execution.
        """
        query = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            sale_id INT,
            product_id INT,
            user_id INT,
            qty INT,
            price DOUBLE,
            status STRING,
            updated_at DATE
        )
        """
        return spark.sql(query)
    
    def create_silver_sales(self, full_table_name: str) -> DataFrame:
        """
        Create a silver table for sales data.
        Args:
            full_table_name (str): Fully qualified table name in the format 'catalog.schema.table'.
        Returns:
            DataFrame: Result of the Spark SQL execution.
        """
        query = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            sale_id INT,
            product_id INT,
            user_id INT,
            quantity INT,
            price DOUBLE,
            region STRING,
            _is_active BOOLEAN,
            _created_at DATE,
            _updated_at DATE,
            _file_name STRING
        )
        USING DELTA
        """
        return spark.sql(query)
    
    def copy_into_with_metadata(self, full_table_name: str, data_path) -> DataFrame:
        """
        Load data from a file path (CSV) into an existing table using COPY INTO.
        Args:
            full_table_name (str): Fully qualified table name in the format 'catalog.schema.table'.
            data_path (str): Path to the CSV data source.
        Returns:
            DataFrame: Result of the Spark SQL execution.
        """
        query = f"""
        COPY INTO {full_table_name}
        FROM (
            SELECT *, _metadata FROM '{data_path}'
        )
        FILEFORMAT = CSV
        FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'mergeSchema' = 'true')
        COPY_OPTIONS ('mergeSchema' = 'true')
        """
        return spark.sql(query)
    
    def auto_loader_with_metadata(self, full_table_name: str, data_path: str, schema_path: str, checkpoint_path: str):
        """
        Load data from a file path (CSV) into an existing table using Autoloader.
        Args:
            full_table_name (str): Fully qualified table name in the format 'catalog.schema.table'.
            data_path (str): Path to the CSV data source.
            schema_path(str): Path to the table schema.
            checkpoint_path(str): Path to the checkpoint schema.
        Returns:
            DataFrame: Result of the Spark SQL execution.
        """
        df = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", True)
            .option("cloudFiles.schemaLocation", schema_path)
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("header", "true")
            .load(data_path)
            .select(
                "*",
                "_metadata"
            )
        )
        return (
            df.writeStream
            .format("delta")
            .option("checkpointLocation", checkpoint_path)
            .option("mergeSchema", "true")
            .outputMode("append")
            .trigger(availableNow=True)
            .toTable(full_table_name)
        )

    def cdc_merge(self, full_table_bronze: str, full_table_silver: str, filter_date: str) -> DataFrame:
        """
        Simulates daily CDC ingestion for the given date.
        - Filters input table for that date.
        - Applies upsert (merge) logic into Silver table.
        - Handles Insert, Update, Delete semantics.
        Args:
            full_table_bronze (str): Fully qualified table name in the format 'catalog.schema.table' for the bronze origin table.
            full_table_silver (str): Fully qualified table name in the format 'catalog.schema.table' for the silver target table.
            filter_date (str): Date to filter the bronze table for (YYYY-MM-DD)
        Returns:
            DataFrame: Result of the Spark SQL execution.
        """
        query = f"""
        MERGE INTO {full_table_silver} AS target
        USING (
            SELECT *
            FROM {full_table_bronze}
            WHERE updated_at = '{filter_date}'
        ) AS source
        ON target.sale_id = source.sale_id

        -- Handle deletes only if newer than target._updated_at
        WHEN MATCHED AND source.status = 'Delete'
        AND source.updated_at > target._updated_at THEN
            UPDATE SET
            _is_active = false,
            _updated_at = source.updated_at

        -- Handle updates only if newer than target._updated_at
        WHEN MATCHED AND source.status = 'Update' 
        AND source.updated_at > target._updated_at THEN
            UPDATE SET
            quantity = source.qty,
            price = source.price,
            region = COALESCE(source.region, target.region),
            _is_active = true,
            _updated_at = source.updated_at
            
        -- Handle inserts only if not already present
        WHEN NOT MATCHED AND source.status = 'Insert' THEN
            INSERT (
                sale_id,
                product_id,
                user_id,
                quantity,
                price,
                region,
                _is_active,
                _created_at,
                _updated_at,
                _file_name
            )
            VALUES (
                source.sale_id,
                source.product_id,
                source.user_id,
                source.qty,
                source.price,
                source.region,
                true,
                source.updated_at,
                source.updated_at,
                source._metadata.file_name
            )
        """
        return spark.sql(query)

    def list_dates(self, full_table_bronze: str) -> DataFrame:
        """
        Returns a list of dates present in the bronze table.
        Args:
            full_table_bronze (str)
        """
        query = f"""
        SELECT DISTINCT CAST(updated_at AS STRING)
        FROM {full_table_bronze}
        ORDER BY updated_at
        """
        dates = [row['updated_at'] for row in spark.sql(query).collect()]
        return dates
