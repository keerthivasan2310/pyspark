from pyspark.sql import SparkSession

# Create Spark Session
spark = SparkSession.builder \
    .appName("RDD Narrow Transformations") \
    .getOrCreate()

sc = spark.sparkContext

# Create RDD
rdd = sc.parallelize([1, 2, 3, 4, 5, 6], 2)

print("Original RDD:", rdd.collect())


#
#
#
# map()
map_rdd = rdd.map(lambda x: x * 2)
print("map():", map_rdd.collect())


# flatMap()
flatmap_rdd = rdd.flatMap(lambda x: (x, x * 10))
print("flatMap():", flatmap_rdd.collect())


# filter()
filter_rdd = rdd.filter(lambda x: x % 2 == 0)
print("filter():", filter_rdd.collect())


#  mapValues()
pair_rdd = sc.parallelize([("CSE", 80), ("ECE", 70), ("CSE", 90)])
mapvalues_rdd = pair_rdd.mapValues(lambda x: x + 5)
print("mapValues():", mapvalues_rdd.collect())


# flatMapValues()
flatmapvalues_rdd = pair_rdd.flatMapValues(lambda x: (x, x + 10))
print("flatMapValues():", flatmapvalues_rdd.collect())


# sample()
sample_rdd = rdd.sample(False, 0.5)
print("sample():", sample_rdd.collect())


# union()
rdd2 = sc.parallelize([7, 8, 9])
union_rdd = rdd.union(rdd2)
print("union():", union_rdd.collect())


# coalesce() (without shuffle)
coalesce_rdd = rdd.coalesce(1)
print("coalesce partitions:", coalesce_rdd.getNumPartitions())

spark.stop()


'''Original RDD: [1, 2, 3, 4, 5, 6]
map(): [2, 4, 6, 8, 10, 12]
flatMap(): [1, 10, 2, 20, 3, 30, 4, 40, 5, 50, 6, 60]
filter(): [2, 4, 6]
mapValues(): [('CSE', 85), ('ECE', 75), ('CSE', 95)]
flatMapValues(): [('CSE', 80), ('CSE', 90), ('ECE', 70), ('ECE', 80), ('CSE', 90), ('CSE', 100)]
sample(): [2, 3, 5]
union(): [1, 2, 3, 4, 5, 6, 7, 8, 9]
coalesce partitions: 1
'''
