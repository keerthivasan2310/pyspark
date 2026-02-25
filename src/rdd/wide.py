from pyspark.sql import SparkSession

# Create Spark Session
spark = SparkSession.builder \
    .appName("RDD Wide Transformations") \
    .getOrCreate()

sc = spark.sparkContext

# Create Pair RDD
rdd = sc.parallelize([
    ("CSE", 80),
    ("ECE", 70),
    ("CSE", 90),
    ("ECE", 60),
    ("IT", 75)
], 2)

print("Original RDD:", rdd.collect())


# 1 groupByKey()
grouped = rdd.groupByKey()
print("groupByKey():", [(k, list(v)) for k, v in grouped.collect()])


# 2 reduceByKey()
reduced = rdd.reduceByKey(lambda x, y: x + y)
print("reduceByKey():", reduced.collect())


# 3 aggregateByKey()
aggregated = rdd.aggregateByKey(
    0,
    lambda acc, value: acc + value,
    lambda acc1, acc2: acc1 + acc2
)
print("aggregateByKey():", aggregated.collect())


# 4 combineByKey()
combined = rdd.combineByKey(
    lambda value: (value, 1),
    lambda acc, value: (acc[0] + value, acc[1] + 1),
    lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])
)

average = combined.mapValues(lambda x: x[0] / x[1])
print("combineByKey() (Average):", average.collect())


# 5 sortByKey()
sorted_rdd = rdd.sortByKey()
print("sortByKey():", sorted_rdd.collect())


# 6 join()
rdd2 = sc.parallelize([
    ("CSE", "Engineering"),
    ("ECE", "Electronics"),
    ("IT", "Information Tech")
])

joined = rdd.join(rdd2)
print("join():", joined.collect())


# 7 distinct()
numbers = sc.parallelize([1, 2, 3, 2, 1, 4])
distinct_rdd = numbers.distinct()
print("distinct():", distinct_rdd.collect())


# 8 repartition()
repartitioned = rdd.repartition(3)
print("repartition partitions:", repartitioned.getNumPartitions())


# 9 intersection()
rdd3 = sc.parallelize([("CSE", 80), ("IT", 75)])
intersection_rdd = rdd.intersection(rdd3)
print("intersection():", intersection_rdd.collect())

spark.stop()


'''
Original RDD: [('CSE', 80), ('ECE', 70), ('CSE', 90), ('ECE', 60), ('IT', 75)]
groupByKey(): [('CSE', [80, 90]), ('ECE', [70, 60]), ('IT', [75])]
reduceByKey(): [('CSE', 170), ('ECE', 130), ('IT', 75)]
aggregateByKey(): [('CSE', 170), ('ECE', 130), ('IT', 75)]
combineByKey() (Average): [('CSE', 85.0), ('ECE', 65.0), ('IT', 75.0)]
sortByKey(): [('CSE', 80), ('CSE', 90), ('ECE', 70), ('ECE', 60), ('IT', 75)]
join(): [('ECE', (70, 'Electronics')), ('ECE', (60, 'Electronics')), ('IT', (75, 'Information Tech')), ('CSE', (80, 'Engineering')), ('CSE', (90, 'Engineering'))]
distinct(): [1, 2, 3, 4]
repartition partitions: 3
intersection(): [('IT', 75), ('CSE', 80)]
'''