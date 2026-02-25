from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("ArrayFunctions").getOrCreate()

data = [
    (1, "Ram", 80, 75, 90),
    (2, "Sam", 60, 70, 65),
    (3, "John", 88, 92, 85)
]

df = spark.createDataFrame(data, ["id", "name", "sub1", "sub2", "sub3"])

df_array = df.withColumn("marks_array", array("sub1", "sub2", "sub3"))

result = (
    df_array
    .withColumn("has_90", array_contains("marks_array", 90))
    .withColumn("array_size", size("marks_array"))
    .withColumn("position_of_90", array_position("marks_array", 90))
    .withColumn("removed_90", array_remove("marks_array", 90))
)

result.show(truncate=False)
