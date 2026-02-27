from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("pyspark").getOrCreate()

df =spark.read.parquet("practice\pyspark\src\Files\output_parquet")

df.show()
df.printSchema()