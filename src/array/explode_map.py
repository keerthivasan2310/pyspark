from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("ExplodeFunctions").getOrCreate()

data = [
    (1, ["A", "B", "C"]),
    (2, None)
]

df = spark.createDataFrame(data, ["id", "letters"])

df.select("id", explode("letters").alias("letter")).show()

df.select("id", explode_outer("letters").alias("letter")).show()

df.select("id", posexplode_outer("letters")).show()
