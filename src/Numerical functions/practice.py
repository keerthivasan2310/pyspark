from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("Numeric functions").getOrCreate()


data = [
    (1,-78.5),
    (2,45.3),
    (3,90.7),
    (4,-66.2)
]

df = spark.createDataFrame(data,["id","marks"])

print("og data")
df.show()
print("numeric functions")

df.select(


    ceil("marks").alias("marks_ceil_rounded"),
    floor("marks").alias("marks_floor_rounded"),
    abs("marks").alias("marks_abs")
).show()

df.select(round(avg("marks"),2),
    sum("marks").alias("marks_sum"),
    avg("marks").alias("marks_avg"),
    max("marks").alias("marks_max"),
    min("marks").alias("marks_min")
).show()

print("maths")
df.select(
    log(abs(col("marks"))).alias("marks_log"),
    pow("marks",3).alias("cube"),
    exp("marks").alias("marks_exp"),
    sqrt(abs("marks").alias("marks_sqrt"))
).show(truncate= False)
