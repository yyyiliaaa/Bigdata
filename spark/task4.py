import sys
from pyspark import SparkContext
#from operator import add
from csv import reader

def state(x):
	if x == "NY":
		return "NY"
	else:
		return "Other"

sc = SparkContext()
lines = sc.textFile(sys.argv[1], 1)
lines = lines.mapPartitions(lambda x: reader(x))
counts = lines.map(lambda x : (state(x[16]), 1)).reduceByKey(lambda x,y: x+y)
res = counts.map(lambda x : '{0:s}\t{1:d}'.format(x[0], x[1]))
res.saveAsTextFile("task4.out")