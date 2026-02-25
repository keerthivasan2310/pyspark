from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("WindowExample").getOrCreate()

data = [
    (1,"Ram","CSE",80),
    (2,"Sam","CSE",90),
    (3,"John","CSE",90),
    (4,"Anu","ECE",75),
    (5,"Meena","ECE",85),
    (6,"Ravi","ECE",85)
]

df = spark.createDataFrame(data,["id","name","dept","marks"])

windowSpec = Window.partitionBy("dept").orderBy(desc("marks"))
windowSpec2 = Window.orderBy("id")

result = df \
.withColumn("row_number", row_number().over(windowSpec)) \
.withColumn("rank", rank().over(windowSpec)) \
.withColumn("dense_rank", dense_rank().over(windowSpec)) \
.withColumn("previous_marks", lag("marks",1).over(windowSpec2)) \
.withColumn("next_marks", lead("marks",1).over(windowSpec2))

result.show()
