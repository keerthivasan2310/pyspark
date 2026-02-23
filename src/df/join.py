from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder \
    .appName("JoinExample") \
    .getOrCreate()


emp_data = [
    (1,"Arun",30000),
    (2,"Divya",40000),
    (3,"Kiran",50000)
]

df_emp = spark.createDataFrame(emp_data, ["id","name","salary"])


dept_data = [
    (1,"IT"),
    (2,"IT"),
    (3,"Sales")
]

df_dept = spark.createDataFrame(dept_data, ["empid","dept"])

print("Employee Table")
df_emp.show()

print("Department Table")
df_dept.show()

df_join = df_emp.join(df_dept,
                      df_emp.id == df_dept.empid)

df_join.groupBy("dept").agg(avg("salary").alias("avgsalary")).orderBy("avgsalary").show()
