import sys
from pyspark import SparkContext
from operator import add
from csv import reader

sc = SparkContext()
lines = sc.textFile(sys.argv[1], 1)
lines = lines.mapPartitions(lambda x: reader(x))
counts = lines.map(lambda x: ((x[14], x[16]), 1)).reduceByKey(lambda x,y: x+y)
counts = counts.sortBy(lambda x: x[0][0]).sortBy(lambda x : x[1], ascending = False)
top20 = sc.parallelize(counts.take(20))
res = top20.map(lambda x : '{0:s}, {1:s}\t{2:d}'.format(x[0][0], x[0][1], x[1]))
res.saveAsTextFile("task6.out")