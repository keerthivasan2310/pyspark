from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Spark").getOrCreate()

schema= StructType([StructField("id",IntegerType(),True),
                    StructField("name",StringType(),True),
                    StructField("age",IntegerType(),True)])

df =spark.read.option("header",True).schema(schema)\
    .csv("src\Files\output_csv")

df.show()
df.printSchema()