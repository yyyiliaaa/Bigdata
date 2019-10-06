import sys
from pyspark import SparkContext
#from operator import add
from csv import reader

sc = SparkContext()
lines = sc.textFile(sys.argv[1], 1)
lines = lines.mapPartitions(lambda x: reader(x))
counts = lines.map(lambda x : (x[2], (float(x[12]), 1))).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
res = counts.map(lambda x : '{0:s}\t{1:.2f}, {2:.2f}'.format(x[0], x[1][0], (x[1][0]/x[1][1])))
res.saveAsTextFile("task3.out")
