from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("AllFunctionsExample").getOrCreate()


data = [
    (1, "Ram", -78.5),
    (2, "Sam", 45.3),
    (3, "John", 90.7),
    (4, "Anu", -66.2)
]

df = spark.createDataFrame(data, ["id", "name", "marks"])

print("Original Data")
df.show()

print("Numerical Functions")
df_num = df.select(
    "name",
    "marks",
    abs("marks").alias("ABS"),
    ceil("marks").alias("CEIL"),
    floor("marks").alias("FLOOR"),
    least("marks", lit(50)).alias("LEAST_with_50")
)
df_num.show()


print("Mathematical Functions")
df_math = df.select(
    "name",
    "marks",
    pow("marks", 2).alias("POWER_square"),
    sqrt(abs("marks")).alias("SQRT"),
    log(abs("marks")).alias("LOG"),
    exp(lit(1)).alias("EXP_e_power_1")
)
df_math.show()


print("Aggregate Functions")
df_agg = df.select(
    sum("marks").alias("SUM"),
    avg("marks").alias("AVG"),
    max("marks").alias("MAX"),
    min("marks").alias("MIN"),
    count("marks").alias("COUNT")
)
df_agg.show()
