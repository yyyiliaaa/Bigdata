import sys
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
lines1 = sc.textFile(sys.argv[1], 1)
lines1 = lines1.mapPartitions(lambda x: reader(x))
counts1 = lines1.map(lambda x : (x[0], (x[14], x[6], x[2], x[1])))

lines2 = sc.textFile(sys.argv[2], 1)
lines2 = lines2.mapPartitions(lambda x : reader(x))
counts2 = lines2.map(lambda x : (x[0], "open_violations"))

joinrdd = counts1.leftOuterJoin(counts2)
res = joinrdd.filter(lambda x : x[1][1] != "open_violations")

res = res.map(lambda x : '{0:s}\t{1:s}, {2:s}, {3:s}, {4:s}'.format(x[0], x[1][0][0], x[1][0][1], x[1][0][2], x[1][0][3]))
res.saveAsTextFile("task1.out")

