from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

spark = SparkSession.builder.getOrCreate()


class StreamingIngestion:
    def __init__(self):
        self.name = "StreamingIngestion"

    def read_stream_json(self, source_path: str, schema_path: str) -> DataFrame:
        """
        This function returns a streaming DataFrame that can be used for further transformations or writes.
        Args:
            source_path (str): Path to the directory containing the source JSON files.
            schema_path (str): Path to the schema location used by Auto Loader to track and evolve the schema.
        Returns:
            DataFrame: A streaming DataFrame representing the ingested JSON data.
        """
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.schemaLocation", schema_path)
            .option("multiLine", "true")
            .load(source_path)
        )

    def write_stream_to_delta(self, df: DataFrame, checkpoint_path: str, table_name: str) -> StreamingQuery:
        """
        This function returns a `StreamingQuery` object that represents the active streaming job.
        Args:
            df (DataFrame): The streaming DataFrame to be written to Delta Lake.
            checkpoint_path (str): Path to the directory for storing checkpoint data.
            table_name (str): Name of the target Delta Lake table.
        Returns:
            StreamingQuery: The active streaming query writing data to Delta Lake.
        """
        return (
            df.writeStream
            .format("delta")
            .option("checkpointLocation", checkpoint_path)
            .option("mergeSchema", "true")
            .outputMode("append")
            .trigger(availableNow=True)
            .toTable(table_name)
        )
