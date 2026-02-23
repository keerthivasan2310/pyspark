from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create Spark Session
spark = SparkSession.builder.appName("AllNumericFunctions").getOrCreate()

# Sample Data
data = [
    (1, 78.456),
    (2, -45.234),
    (3, 90.789),
    (4, 66.111)
]

# Create DataFrame
df = spark.createDataFrame(data, ["id", "marks"])

print("Original DataFrame")
df.show()

# Apply Numeric Functions
result_df = df.select(

    sum(col("marks")).alias("SUM"),
    avg(col("marks")).alias("AVG"),
    min(col("marks")).alias("MIN"),
    max(col("marks")).alias("MAX"),

    round(avg(col("marks")), 2).alias("ROUND_AVG"),

    sum(abs(col("marks"))).alias("SUM_OF_ABS")
)

print("Applying All Numeric Functions")
result_df.show()

# Stop Spark Session
spark.stop()
