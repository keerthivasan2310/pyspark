from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark Session
spark = SparkSession.builder.appName("GeneralDFunctions").getOrCreate()

# Sample Data
data = [
    (1, "Ram", 78, "CSE"),
    (2, "Sam", 45, "ECE"),
    (3, "John", 90, "CSE"),
    (4, "Anu", 66, "EEE")
]

# Create DataFrame
df = spark.createDataFrame(data, ["id", "name", "marks", "department"])

print("Original DataFrame using show()")
df.show()

# 1. printSchema()
print("Schema of DataFrame")
df.printSchema()

# 2. count()
print("Total number of rows")
print(df.count())

# 3. columns()
print("Column names")
print(df.columns)

# 4. select()
print("Selecting name and marks columns")
df.select("name", "marks").show()

# 5. filter()
print("Filter rows where marks > 60")
df.filter(col("marks") > 60).show()

# 6. where()
print("Where condition marks > 60")
df.where(col("marks") > 60).show()

# 7. like()
print("Names starting with 'R'")
df.filter(col("name").like("R%")).show()

# 8. sort()
print("Sort by marks ascending")
df.sort("marks").show()

print("Sort by marks descending")
df.sort(col("marks").desc()).show()

# 9. describe()
print("Summary statistics for marks")
df.describe("marks").show()

# 10. take(n)
print("First 2 rows using take()")
print(df.take(2))

# 11. collect()
print("Collect all rows")
print(df.collect())

# Stop Spark Session
spark.stop()
