from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, concat, upper, lower, avg, sum, count

# Create Spark Session
spark = SparkSession.builder \
    .appName("DataFrameTransformationsExample") \
    .master("local[*]") \
    .getOrCreate()

# Sample Data
data = [
    (1, "Ram", "CSE", 78),
    (2, "Sam", "ECE", 45),
    (3, "John", "CSE", 90),
    (4, "Anu", "EEE", 66),
    (5, "Priya", "ECE", 88),
    (6, "Kiran", "CSE", 35)
]

columns = ["id", "name", "department", "marks"]

df = spark.createDataFrame(data, columns)

print("Original Data")
df.show()


# 1. select()
print("Select name and marks")
df.select("name", "marks").show()


# 2. withColumn() (Add new column)
print("Add Bonus Marks Column")
df_bonus = df.withColumn("bonus_marks", col("marks") + 5)
df_bonus.show()


# 3. withColumnRenamed()
print("Rename Column")
df_rename = df_bonus.withColumnRenamed("marks", "total_marks")
df_rename.show()


# 4. drop()
print("Drop Column")
df_drop = df_rename.drop("bonus_marks")
df_drop.show()


# 5. filter() / where()
print("Filter marks > 60")
df.filter(col("marks") > 60).show()


# 6. distinct()
print("Distinct Departments")
df.select("department").distinct().show()


# 7. sort() / orderBy()
print("Sort by marks ascending")
df.orderBy("marks").show()

print("Sort by marks descending")
df.orderBy(col("marks").desc()).show()


# 8. groupBy() with aggregations
print("Group By Department")
df.groupBy("department").agg(
    count("id").alias("total_students"),
    avg("marks").alias("average_marks"),
    sum("marks").alias("total_marks")
).show()


# 9. dropDuplicates()
print("Drop Duplicates Example")
df_dup = df.union(df)   # create duplicate rows
df_dup.dropDuplicates().show()


# 10. limit()
print("Limit 3 Rows")
df.limit(3).show()


# 11. union()
print("Union Example")
df_small = df.limit(2)
df_union = df_small.union(df_small)
df_union.show()


# 12. join()
print("Join Example")

data2 = [
    (1, "A"),
    (2, "B"),
    (3, "A"),
    (4, "C"),
    (5, "B"),
    (6, "A")
]

df_grade = spark.createDataFrame(data2, ["id", "section"])

df_join = df.join(df_grade, "id", "inner")
df_join.show()


# 13. when() (Conditional column)
print("Conditional Column")
df_grade_status = df.withColumn(
    "result",
    when(col("marks") >= 50, "Pass").otherwise("Fail")
)
df_grade_status.show()


# 14. concat(), upper(), lower()
print("String Functions")
df_string = df.withColumn("name_upper", upper(col("name"))) \
              .withColumn("dept_lower", lower(col("department")))
df_string.show()


# Stop Spark
try:
    spark.stop()
except:
    pass
