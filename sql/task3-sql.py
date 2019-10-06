from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys


spark = SparkSession.builder.appName("task3-sql").config("spark.some.config.option", "some-value").getOrCreate()
openviolations = spark.read.format('csv').options(header='true',
inferschema='true').load(sys.argv[1])

openviolations.createOrReplaceTempView("openviolations")

result = spark.sql("SELECT license_type, SUM(amount_due) as sum, AVG(amount_due) as average FROM openviolations GROUP BY license_type")

result.select(format_string('%s\t%.2f, %.2f',result.license_type,result.sum,result.average)).write.save("task3-sql.out",format="text")