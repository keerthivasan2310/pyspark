from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WriteCSV")\
    .config("spark.hadoop.io.native.lib.available", "false") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem").getOrCreate()

data = [
    (1, "Ram", "CSE", 78),
    (2, "Sam", "ECE", 45),
    (3, "Ravi", "MECH", 88)
]

columns = ["id", "name", "department", "marks"]

df = spark.createDataFrame(data, columns)

df.write \
  .mode("overwrite") \
    .parquet("practice/pyspark/src/Files/output_parquet")
