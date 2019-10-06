import sys
from pyspark import SparkContext
from operator import add
from csv import reader

def isWeekend(x):
	x = x.split('-')
	weekends = ['05', '06', '12', '13', '19', '20', '26', '27']
	if x[2] in weekends:
		return (1,0)
	else:
		return (0,1)
sc = SparkContext()
lines = sc.textFile(sys.argv[1], 1)
lines = lines.mapPartitions(lambda x: reader(x))
counts = lines.map(lambda x : (x[2], isWeekend(x[1]))).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
counts = counts.sortBy(lambda x : x[0])
res = counts.map(lambda x : '{0:s}\t{1:.2f}, {2:.2f}'.format(x[0], x[1][0]/8.0, x[1][1]/23.0))
res.saveAsTextFile("task7.out")
