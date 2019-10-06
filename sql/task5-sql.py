from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys


spark = SparkSession.builder.appName("task5-sql").config("spark.some.config.option", "some-value").getOrCreate()
parkingviolations = spark.read.format('csv').options(header='true',
inferschema='true').load(sys.argv[1])

parkingviolations.createOrReplaceTempView("parkingviolations")

groupedCars = spark.sql("SELECT plate_id, registration_state, COUNT(*) as total_number FROM parkingviolations GROUP BY plate_id, registration_state")

groupedCars.createOrReplaceTempView("groupedCars")

#maxNumber = spark.sql("SELECT MAX(count) FROM groupedCars")

#maxNumber.createOrReplaceTempView("maxNumber")

result = spark.sql("SELECT plate_id, registration_state, total_number FROM groupedCars WHERE groupedCars.total_number IN (SELECT MAX(total_number) FROM groupedCars)")

result.select(format_string('%s, %s\t%d',result.plate_id,result.registration_state,result.total_number)).write.save("task5-sql.out",format="text")