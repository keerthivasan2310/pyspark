from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("date").getOrCreate()

data = [("2026-02-24",)]

#date of current
'''
df=spark.range(1)
df.select(
    current_date().alias("date"),
    current_timestamp().alias("timestamp")).show(truncate = False)
'''
df = spark.createDataFrame(data, ["start_date"])

#convert to date

#df.withColumn("start_date",col("start_date").cast("date"))

df.withColumn("start_date",to_date("start_date"))


#date_add
'''
df.select("start_date",
date_add("start_date",10).alias("after_10")).show(truncate = False)
'''

#date_diff
'''
df.select( datediff(date_add("start_date",10),"start_date").alias("diff")).show()
'''

# year, month, day
df.select("start_date",
          date_format(current_timestamp(),"dd-(E)-MM-(MMM)-[MMMM]-yyyy-(HH:mm)").alias("diff"),
          year("start_date").alias("year"),
          month("start_date").alias("month"),
          dayofmonth("start_date").alias("dayofmonth"),
          dayofyear("start_date").alias("dayofyear"),
          dayofweek("start_date").alias("dayofweek"),
        ).show(truncate=False)

