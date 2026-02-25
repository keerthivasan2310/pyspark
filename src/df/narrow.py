from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper

# Create Spark Session
spark = SparkSession.builder \
    .appName("DataFrame Narrow Transformations") \
    .getOrCreate()

# Create DataFrame
data = [
    (1, "Ram", "CSE", 78),
    (2, "Sam", "ECE", 45),
    (3, "John", "CSE", 90),
    (4, "Anu", "IT", 66)
]

df = spark.createDataFrame(data, ["id", "name", "department", "marks"])

print("Original DataFrame")
df.show()


# 1 select()
df_select = df.select("name", "marks")
print("select()")
df_select.show()


# 2 withColumn()
df_withcolumn = df.withColumn("bonus_marks", col("marks") + 5)
print("withColumn()")
df_withcolumn.show()


# 3 withColumnRenamed()
df_rename = df.withColumnRenamed("name", "student_name")
print("withColumnRenamed()")
df_rename.show()


# 4 drop()
df_drop = df.drop("department")
print("drop()")
df_drop.show()


# 5 filter()
df_filter = df.filter(col("marks") > 60)
print("filter()")
df_filter.show()


# 6 where()
df_where = df.where(col("department") == "CSE")
print("where()")
df_where.show()


# 7 limit()
df_limit = df.limit(2)
print("limit()")
df_limit.show()


# 8 sample()
df_sample = df.sample(False, 0.5)
print("sample()")
df_sample.show()


# 9 union()
data2 = [
    (5, "Kumar", "MECH", 70)
]

df2 = spark.createDataFrame(data2, ["id", "name", "department", "marks"])
df_union = df.union(df2)
print("union()")
df_union.show()


# 10 coalesce() (reduce partitions without shuffle)
df_coalesce = df.coalesce(1)
print("coalesce partitions:", df_coalesce.rdd.getNumPartitions())

spark.stop()
