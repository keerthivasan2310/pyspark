from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("aggregations").getOrCreate()

data = [
    (1, "Ram", 78),
    (2, "Sam", 45),
    (3, "John", 90),
    (4, "Anu", 66)
]

df = spark.createDataFrame(data, ["id", "name", "marks"])

df.select(mean("marks").alias("mean"),
          avg("marks").alias("avg"),
          min("marks").alias("min"),
          max("marks").alias("max"),
          sum("marks").alias("sum"),
          first("marks").alias("first"),
          last("marks").alias("last")).show()