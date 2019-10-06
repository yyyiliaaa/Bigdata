from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys


spark = SparkSession.builder.appName("task2-sql").config("spark.some.config.option", "some-value").getOrCreate()
parkingviolations = spark.read.format('csv').options(header='true',
inferschema='true').load(sys.argv[1])

parkingviolations.createOrReplaceTempView("parkingviolations")

result = spark.sql("SELECT violation_code, COUNT(*) as frequency FROM parkingviolations GROUP BY violation_code")

result.select(format_string('%d\t%d',result.violation_code,result.frequency)).write.save("task2-sql.out",format="text")