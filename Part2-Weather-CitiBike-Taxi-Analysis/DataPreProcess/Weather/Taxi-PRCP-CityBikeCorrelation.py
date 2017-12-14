from operator import add
from pyspark import SparkContext
from csv import reader
#import org.apache.spark.sql._
from pyspark.sql.types import DoubleType, IntegerType,DateType


bike = spark.read.option("header",True).csv("/Users/junzhi/citi_1to24.csv")
w15 = spark.read.option("header",True).csv("/Users/junzhi/Downloads/Weather2015.csv")
w16 = spark.read.option("header",True).csv("/Users/junzhi/Downloads/Weather2016.csv")
taxi15 = spark.read.option("header",True).csv("/Users/junzhi/Downloads/dataAndPlots/taxi/taxi_day_count_2015.csv")
taxi16 = spark.read.option("header",True).csv("/Users/junzhi/Downloads/dataAndPlots/taxi/taxi_day_count_2016.csv")
bike1516 = sc.textFile("/Users/junzhi/citi_1to24.csv").map(lambda x: x.encode('utf-8')).mapPartitions(lambda x: reader(x))


w15 = spark.read.option("header",True).csv("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/Weather2015.csv")
w16 = spark.read.option("header",True).csv("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/Weather2016.csv")
taxi15 = spark.read.option("header",True).csv("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/taxi_day_count_2015.csv")
taxi16 = spark.read.option("header",True).csv("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/taxi_day_count_2016.csv")

#bike1516 = sc.textFile("/user/jw4468/bigDataProject/data/cleaned/citi_1to24.csv").map(lambda x: x.encode('utf-8')).mapPartitions(lambda x: reader(x))

#bikecount = bike1516.map(lambda x: (x[1].split()[0], 1)).reduceByKey(lambda a, b: a + b)
#bikedf= sqlContext.createDataFrame(bikecount, ('date', 'BikeRecordcount'))

bike = bike.withColumn('sTime', bike['starttime'].cast('timestamp'))
bike = bike.withColumn('sDate', bike['sTime'].cast('date'))
bike.registerTempTable('biket')
bikedf = sqlContext.sql('SELECT sDate as date, count(*) as BikeRecordcount FROM biket group by sDate ORDER BY sDate')
bikedf.registerTempTable('biketable')
taxi15.registerTempTable('t15table')
taxi16.registerTempTable('t16table')
w15.registerTempTable('w15table')
w16.registerTempTable('w16table')

weatherbike2015 = sqlContext.sql('SELECT date, BikeRecordcount, TEMP, DEWP, VISIB, WDSP, PRCP,SNDP FROM w15table JOIN biketable ON biketable.date = w15table.YEARMODA ORDER BY date')
weatherbike2015 = weatherbike2015.withColumn('TEMP', (weatherbike2015['TEMP'] - 32)*5/9)
weatherbike2016 = sqlContext.sql('SELECT date, BikeRecordcount, TEMP, DEWP, VISIB, WDSP, PRCP,SNDP FROM w16table JOIN biketable ON biketable.date = w16table.YEARMODA ORDER BY date')
weatherbike2016 = weatherbike2016.withColumn('TEMP', (weatherbike2016['TEMP'] - 32)*5/9)

weatherbike2015.registerTempTable('w15Biketable')
weatherbike2016.registerTempTable('w16Biketable')


weather_bike_taxi_2015 = sqlContext.sql('SELECT w15Biketable.date AS date, BikeRecordcount, taxi_day_count_2015 AS TaxiRecordcount, TEMP, DEWP, VISIB, WDSP, PRCP FROM w15Biketable JOIN t15table ON month(w15Biketable.date) = t15table.month and day(w15Biketable.date) = t15table.day ORDER BY date')
#weather_bike_taxi_2015.describe().show()

weather_bike_taxi_2016 = sqlContext.sql('SELECT w16Biketable.date AS date, BikeRecordcount, taxi_day_count_2016 AS TaxiRecordcount, TEMP, DEWP, VISIB, WDSP, PRCP FROM w16Biketable JOIN t16table ON month(w16Biketable.date) = t16table.month and day(w16Biketable.date) = t16table.day ORDER BY date')
#weather_bike_taxi_2016.describe().show()

# aggregate the data into 4 seasons (spring (3- 5), summer(6-8), fall(9-11) and winter(12-2)
weather_bike_taxi_2015_1 = sqlContext.sql('SELECT w15Biketable.date AS date, BikeRecordcount, taxi_day_count_2015 AS TaxiRecordcount, TEMP, DEWP, VISIB, WDSP, PRCP FROM w15Biketable JOIN t15table ON month(w15Biketable.date) = t15table.month and day(w15Biketable.date) = t15table.day WHERE month(w15Biketable.date) >= 3 AND month(w15Biketable.date) < 6 ORDER BY date')

weather_bike_taxi_2015_2 = sqlContext.sql('SELECT w15Biketable.date AS date, BikeRecordcount, taxi_day_count_2015 AS TaxiRecordcount, TEMP, DEWP, VISIB, WDSP, PRCP FROM w15Biketable JOIN t15table ON month(w15Biketable.date) = t15table.month and day(w15Biketable.date) = t15table.day WHERE month(w15Biketable.date) >= 6 AND month(w15Biketable.date) < 9 ORDER BY date')

weather_bike_taxi_2015_3 = sqlContext.sql('SELECT w15Biketable.date AS date, BikeRecordcount, taxi_day_count_2015 AS TaxiRecordcount, TEMP, DEWP, VISIB, WDSP, PRCP FROM w15Biketable JOIN t15table ON month(w15Biketable.date) = t15table.month and day(w15Biketable.date) = t15table.day WHERE month(w15Biketable.date) >= 9 AND month(w15Biketable.date) < 12 ORDER BY date')

weather_bike_taxi_2015_4 = sqlContext.sql('SELECT w15Biketable.date AS date, BikeRecordcount, taxi_day_count_2015 AS TaxiRecordcount, TEMP, DEWP, VISIB, WDSP, PRCP FROM w15Biketable JOIN t15table ON month(w15Biketable.date) = t15table.month and day(w15Biketable.date) = t15table.day WHERE month(w15Biketable.date) =12 or month(w15Biketable.date) < 3 ORDER BY date')

weather_bike_taxi_2016_1 = sqlContext.sql('SELECT w16Biketable.date AS date, BikeRecordcount, taxi_day_count_2016 AS TaxiRecordcount, TEMP, DEWP, VISIB, WDSP, PRCP FROM w16Biketable JOIN t16table ON month(w16Biketable.date) = t16table.month and day(w16Biketable.date) = t16table.day WHERE month(w16Biketable.date) >= 3 AND month(w16Biketable.date) < 6 ORDER BY date')

weather_bike_taxi_2016_2 = sqlContext.sql('SELECT w16Biketable.date AS date, BikeRecordcount, taxi_day_count_2016 AS TaxiRecordcount, TEMP, DEWP, VISIB, WDSP, PRCP FROM w16Biketable JOIN t16table ON month(w16Biketable.date) = t16table.month and day(w16Biketable.date) = t16table.day WHERE month(w16Biketable.date) >= 6 AND month(w16Biketable.date) < 9 ORDER BY date')

weather_bike_taxi_2016_3 = sqlContext.sql('SELECT w16Biketable.date AS date, BikeRecordcount, taxi_day_count_2016 AS TaxiRecordcount, TEMP, DEWP, VISIB, WDSP, PRCP FROM w16Biketable JOIN t16table ON month(w16Biketable.date) = t16table.month and day(w16Biketable.date) = t16table.day WHERE month(w16Biketable.date) >= 9 AND month(w16Biketable.date) < 12 ORDER BY date')

weather_bike_taxi_2016_4 = sqlContext.sql('SELECT w16Biketable.date AS date, BikeRecordcount, taxi_day_count_2016 AS TaxiRecordcount, TEMP, DEWP, VISIB, WDSP, PRCP FROM w16Biketable JOIN t16table ON month(w16Biketable.date) = t16table.month and day(w16Biketable.date) = t16table.day WHERE month(w16Biketable.date) =12 or month(w16Biketable.date) < 3 ORDER BY date')

#change data type
weather_bike_taxi_2015_1 = weather_bike_taxi_2015_1.withColumn('PRCP', weather_bike_taxi_2015_1['PRCP'].substr(1,5))
weather_bike_taxi_2015_2 = weather_bike_taxi_2015_2.withColumn('PRCP', weather_bike_taxi_2015_2['PRCP'].substr(1,5))
weather_bike_taxi_2015_3 = weather_bike_taxi_2015_3.withColumn('PRCP', weather_bike_taxi_2015_3['PRCP'].substr(1,5))
weather_bike_taxi_2015_4 = weather_bike_taxi_2015_4.withColumn('PRCP', weather_bike_taxi_2015_4['PRCP'].substr(1,5))

weather_bike_taxi_2016_1 = weather_bike_taxi_2016_1.withColumn('PRCP', weather_bike_taxi_2016_1['PRCP'].substr(1,5))
weather_bike_taxi_2016_2 = weather_bike_taxi_2016_2.withColumn('PRCP', weather_bike_taxi_2016_2['PRCP'].substr(1,5))
weather_bike_taxi_2016_3 = weather_bike_taxi_2016_3.withColumn('PRCP', weather_bike_taxi_2016_3['PRCP'].substr(1,5))
weather_bike_taxi_2016_4 = weather_bike_taxi_2016_4.withColumn('PRCP', weather_bike_taxi_2016_4['PRCP'].substr(1,5))


weather_bike_taxi_2015_1.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/weather_bike_taxi_2015_1.csv")
weather_bike_taxi_2015_2.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/weather_bike_taxi_2015_2.csv")
weather_bike_taxi_2015_3.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/weather_bike_taxi_2015_3.csv")
weather_bike_taxi_2015_4.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/weather_bike_taxi_2015_4.csv")

weather_bike_taxi_2016_1.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/weather_bike_taxi_2016_1.csv")
weather_bike_taxi_2016_2.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/weather_bike_taxi_2016_2.csv")
weather_bike_taxi_2016_3.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/weather_bike_taxi_2016_3.csv")
weather_bike_taxi_2016_4.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/weather_bike_taxi_2016_4.csv")

