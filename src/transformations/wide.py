from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()

data = [(1,"Ram",78),(2,"Sam",45),(3,"John",90),(4,"Anu",66)]
df = spark.createDataFrame(data,["id","name","marks"])
df.show()

input("Press Enter to stop...")
