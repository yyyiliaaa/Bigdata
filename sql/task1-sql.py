from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys


spark = SparkSession.builder.appName("task1-sql").config("spark.some.config.option", "some-value").getOrCreate()
parkingviolations = spark.read.format('csv').options(header='true',
inferschema='true').load(sys.argv[1])
openviolations = spark.read.format('csv').options(header='true',
inferschema='true').load(sys.argv[2])

parkingviolations.createOrReplaceTempView("parkingviolations")
openviolations.createOrReplaceTempView("openviolations")

result = spark.sql("SELECT p.summons_number, p.plate_id, p.violation_precinct, p.violation_code, p.issue_date FROM parkingviolations p LEFT JOIN openviolations o ON p.summons_number = o.summons_number WHERE o.summons_number is NULL")

result.select(format_string('%d\t%s, %d, %d, %s',result.summons_number,result.plate_id,result.violation_precinct,result.violation_code,date_format(result.issue_date,'yyyy-MM-dd'))).write.save("task1-sql.out",format="text")