from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("JoinsExample").getOrCreate()

students = [
    (1, "Ram"),
    (2, "Sam"),
    (3, "John"),
    (4, "Anu")
]

marks = [
    (1, 78),
    (2, 45),
    (3, 90),
    (5, 66)
]

df_students = spark.createDataFrame(students, ["id", "name"])
df_marks = spark.createDataFrame(marks, ["id", "marks"])
'''
df_students.join(df_marks, "id", "inner").show()

df_students.crossJoin(df_marks).show()

df_students.join(df_marks, "id", "outer").show()
'''
df_students.join(df_marks, "id", "left").show()

df_students.join(df_marks, "id", "right").show()

df_students.join(df_marks, "id", "left_semi").show()

df_students.join(df_marks, "id", "left_anti").show()

