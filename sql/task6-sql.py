from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys


spark = SparkSession.builder.appName("task6-sql").config("spark.some.config.option", "some-value").getOrCreate()
parkingviolations = spark.read.format('csv').options(header='true',
inferschema='true').load(sys.argv[1])

parkingviolations.createOrReplaceTempView("parkingviolations")

result = spark.sql("SELECT plate_id, registration_state, COUNT(*) as total_number FROM parkingviolations GROUP BY plate_id, registration_state ORDER BY total_number desc, plate_id asc limit 20")

result.select(format_string('%s, %s\t%d',result.plate_id,result.registration_state,result.total_number)).write.save("task6-sql.out",format="text")