from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("rdd").getOrCreate()
sc = spark.sparkContext


rdd = sc.parallelize([1,2,3,4,5],2)

sqr = rdd.map(lambda x:x*x)

print(sqr.collect())
