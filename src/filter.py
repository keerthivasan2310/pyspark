from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FilterExample").getOrCreate()

data = [(1,"Ram",78),(2,"Sam",45),(3,"John",90),(4,"Anu",66)]
df = spark.createDataFrame(data,["id","name","marks"])

df.filter(df.marks > 70).show()

spark.stop()
