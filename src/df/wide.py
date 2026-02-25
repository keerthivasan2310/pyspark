from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg

# Create Spark Session
spark = SparkSession.builder \
    .appName("DataFrame Wide Transformations") \
    .getOrCreate()

# Create DataFrame
data = [
    (1, "Ram", "CSE", 78),
    (2, "Sam", "ECE", 45),
    (3, "John", "CSE", 90),
    (4, "Anu", "IT", 66),
    (5, "Kumar", "ECE", 70)
]

df = spark.createDataFrame(data, ["id", "name", "department", "marks"])

print("Original DataFrame")
df.show()


# 1 groupBy()
df_group = df.groupBy("department").sum("marks")
print("groupBy()")
df_group.show()


# 2 agg()
df_agg = df.groupBy("department").agg(avg("marks"))
print("agg()")
df_agg.show()


# 3 cube()
df_cube = df.cube("department").sum("marks")
print("cube()")
df_cube.show()


# 4 rollup()
df_rollup = df.rollup("department").sum("marks")
print("rollup()")
df_rollup.show()


# 5 join()
data2 = [
    ("CSE", "Engineering"),
    ("ECE", "Electronics"),
    ("IT", "Information Technology")
]

df2 = spark.createDataFrame(data2, ["department", "dept_fullname"])

df_join = df.join(df2, "department")
print("join()")
df_join.show()


# 6 distinct()
df_distinct = df.select("department").distinct()
print("distinct()")
df_distinct.show()


# 7 dropDuplicates()
df_dropdup = df.dropDuplicates(["department"])
print("dropDuplicates()")
df_dropdup.show()


# 8 repartition()
df_repartition = df.repartition(3)
print("repartition partitions:", df_repartition.rdd.getNumPartitions())


# 9 orderBy()
df_order = df.orderBy("marks")
print("orderBy()")
df_order.show()


spark.stop()
