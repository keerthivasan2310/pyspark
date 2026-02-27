from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Spark_json").getOrCreate()

schema= StructType([StructField("id",IntegerType(),True),
                    StructField("name",StringType(),True),
                    StructField("age",IntegerType(),True),
                    StructField("salary",DoubleType(),True),])

df =spark.read.schema(schema).json("src/Files/emp.json")
df.show()
df.printSchema()