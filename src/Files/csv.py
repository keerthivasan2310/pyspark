from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import IntegerType, StringType, DoubleType

spark = SparkSession.builder.appName("ReadCSVWithSchema").getOrCreate()

# Define Schema
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", DoubleType(), True)
])

# Read CSV
df = spark.read \
    .option("header", True) \
    .schema(schema) \
    .csv("D:\practice\pyspark\src\Files\emp.csv")

df.show()
df.printSchema()
