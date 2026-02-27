from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark =SparkSession.builder.appName("output_csv").getOrCreate()

data =[
    (1, "A", 1),
    (2, "B", 2),
    (3, "C", 3),
]
schema= StructType([StructField("id", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("age", IntegerType(), True),])

df= spark.createDataFrame(data, schema)

df.write.option("header",True)\
    .mode("overwrite")\
    .csv("src/Files/output_csv")