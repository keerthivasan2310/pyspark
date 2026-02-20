from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder \
    .appName("NarrowTransformationDemo") \
    .master("local[*]") \
    .getOrCreate()

# get spark context
sc = spark.sparkContext

# RDD
rdd = sc.parallelize([1,2,3,4,5,6,7,8], 4)

mapped = rdd.map(lambda x: x * 2)

print(mapped.collect())

# keep UI alive
input("Press Enter to stop...")

spark.stop()

