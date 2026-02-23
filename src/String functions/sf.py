from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create Spark Session
spark = SparkSession.builder.appName("AllStringFunctions").getOrCreate()

# Sample Data
data = [
    (1, "  ram kumar  "),
    (2, "samuel"),
    (3, "JOHN DOE"),
    (4, " anu ")
]

# Create DataFrame
df = spark.createDataFrame(data, ["id", "name"])

print("Original DataFrame")
df.show(truncate=False)

# Apply all string functions
result_df = df.select(
    col("name"),

    upper(col("name")).alias("upper"),
    lower(col("name")).alias("lower"),

    trim(col("name")).alias("trim"),
    ltrim(col("name")).alias("ltrim"),
    rtrim(col("name")).alias("rtrim"),

    substring(col("name"), 1, 4).alias("substring"),

    substring_index(col("name"), " ", 1).alias("substring_index"),

    split(trim(col("name")), " ").alias("split"),

    repeat(col("name"), 2).alias("repeat"),

    rpad(trim(col("name")), 15, "*").alias("rpad"),
    lpad(trim(col("name")), 15, "*").alias("lpad"),

    regexp_replace(col("name"), " ", "").alias("regex_replace"),

    regexp_extract(col("name"), "[A-Za-z]+", 0).alias("regex_extract"),

    length(col("name")).alias("length"),

    instr(col("name"), "a").alias("instr_position"),

    initcap(trim(col("name"))).alias("initcap")
)

print("Applying All String Functions")
result_df.show(truncate=False)

# Stop Spark Session
spark.stop()
