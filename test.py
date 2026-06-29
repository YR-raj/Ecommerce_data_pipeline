from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()

df = (
    spark.read
    .option("header", "true")
    .csv(
        r"data\bronze\source_products\year=2026\month=06\day=07"
    )
)

df.show(5)

spark.stop()