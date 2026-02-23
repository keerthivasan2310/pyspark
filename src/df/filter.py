from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("filter").getOrCreate()

data = [
    (1,"Ram",78,"CSE"),
    (2,"Sam",45,"ECE"),
    (3,"John",90,"CSE"),
    (4,"Anu",66,"EEE"),
    (5,"Priya",55,"ECE")
]

df=spark.createDataFrame(data,["id","name","marks","dept"])

df.filter(col('marks') > 50).show()

