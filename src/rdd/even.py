from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark").getOrCreate()
sc= spark.sparkContext
rdd = sc.parallelize( range(0, 10),2)

even = rdd.filter(lambda x:x%2==0)

print(even.collect())