from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ShuffleDemo").getOrCreate()

df = spark.range(0, 1000000)

df2 = df.repartition(8).groupBy("id").count()

df2.show()

input("Open UI now")
