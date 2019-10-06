from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys


spark = SparkSession.builder.appName("task4-sql").config("spark.some.config.option", "some-value").getOrCreate()
parkingviolations = spark.read.format('csv').options(header='true',
inferschema='true').load(sys.argv[1])

parkingviolations.createOrReplaceTempView("parkingviolations")

result = spark.sql("SELECT registration_state, COUNT(*) as total_number FROM (SELECT CASE WHEN registration_state = 'NY' THEN 'NY' ELSE 'Other' END AS registration_state FROM parkingviolations) GROUP BY registration_state")

result.select(format_string('%s\t%d',result.registration_state,result.total_number)).write.save("task4-sql.out",format="text")