from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys


spark = SparkSession.builder.appName("task7-sql").config("spark.some.config.option", "some-value").getOrCreate()
parkingviolations = spark.read.format('csv').options(header='true',
inferschema='true').load(sys.argv[1])

parkingviolations.createOrReplaceTempView("parkingviolations")

all_codes = spark.sql("SELECT DISTINCT violation_code FROM parkingviolations")
all_codes.createOrReplaceTempView("all_codes")

weekend = spark.sql("SELECT all_codes.violation_code, ifnull(temp.avg, 0) as we_avg FROM (SELECT violation_code, COUNT(*)/8 as avg FROM parkingviolations WHERE DATE_FORMAT(issue_date, 'yyyy-MM-dd') in ('2016-03-05', '2016-03-06','2016-03-12','2016-03-13','2016-03-19','2016-03-20','2016-03-26','2016-03-27') GROUP BY violation_code) AS temp RIGHT JOIN all_codes on temp.violation_code = all_codes.violation_code")

weekday = spark.sql("SELECT all_codes.violation_code, ifnull(temp.avg, 0) as wk_avg FROM (SELECT violation_code, COUNT(*)/23 as avg FROM parkingviolations WHERE DATE_FORMAT(issue_date, 'yyyy-MM-dd') NOT IN('2016-03-05','2016-03-06','2016-03-12','2016-03-13','2016-03-19','2016-03-20','2016-03-26','2016-03-27') GROUP BY violation_code) as temp RIGHT JOIN all_codes on temp.violation_code = all_codes.violation_code") 
weekday.createOrReplaceTempView("weekday")
weekend.createOrReplaceTempView("weekend")

result = spark.sql("SELECT weekday.violation_code, weekend.we_avg as weekend_average, weekday.wk_avg as weekday_average from weekday join weekend on weekday.violation_code = weekend.violation_code")

result.select(format_string('%d\t%.2f, %.2f',result.violation_code,result.weekend_average,result.weekday_average)).write.save("task7-sql.out",format="text")