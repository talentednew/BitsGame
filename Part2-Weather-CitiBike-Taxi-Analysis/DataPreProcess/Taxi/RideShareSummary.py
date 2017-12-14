from operator import add
from pyspark import SparkContext
from csv import reader
from pyspark.sql.types import DoubleType, IntegerType, DateType,StringType,FloatType
from pyspark.mllib.stat import Statistics

# preprocess data
t15f = spark.read.option("header",True).csv("/user/jw4468/bigDataProject/data/cleaned/2015_1to6-f.csv")
t15s = spark.read.option("header",True).csv("/user/jw4468/bigDataProject/data/cleaned/2015_7to12-f.csv")
t16f = spark.read.option("header",True).csv("/user/jw4468/bigDataProject/data/cleaned/2016_1to6-f.csv")
t16s = spark.read.option("header",True).csv("/user/jw4468/bigDataProject/data/cleaned/201602half.csv")
t15f.registerTempTable('f15')
t16f.registerTempTable('f16')
t16s.registerTempTable('s16')
t15s.registerTempTable('s15')

p15f = t15f.select("passenger_count","tpep_pickup_datetime","payment_type","tip_amount","total_amount")
p15s = t15s.select("passenger_count","tpep_pickup_datetime","payment_type","tip_amount","total_amount")
p15 = p15f.union(p15s)

p16f = t16f.select("passenger_count","tpep_pickup_datetime","payment_type","tip_amount","total_amount")
p16s = t16s.select("passenger_count","tpep_pickup_datetime","payment_type","tip_amount","total_amount")
p16 = p16f.union(p16s)

p15 = p15.withColumn('PUTime', p15['tpep_pickup_datetime'].cast('timestamp'))
p15 = p15.withColumn('PUDate', p15['PUTime'].cast('date')).withColumn("passenger_count", p15["passenger_count"].cast(IntegerType())).withColumn("payment_type", p15["payment_type"].cast(IntegerType())).withColumn("tip_amount", p15["tip_amount"].cast(FloatType())).withColumn("total_amount", p15["total_amount"].cast(FloatType()))
p15.registerTempTable('pp15')

p16 = p16.withColumn('PUTime', p16['tpep_pickup_datetime'].cast('timestamp'))
p16 = p16.withColumn('PUDate', p16['PUTime'].cast('date')).withColumn("passenger_count", p16["passenger_count"].cast(IntegerType())).withColumn("payment_type", p16["payment_type"].cast(IntegerType())).withColumn("tip_amount", p16["tip_amount"].cast(FloatType())).withColumn("total_amount", p16["total_amount"].cast(FloatType()))
p16.registerTempTable('pp16')


ppdist15 = sqlContext.sql("SELECT passenger_count, COUNT(*) AS size FROM pp15 group by passenger_count order by size")

ppdist16 = sqlContext.sql("SELECT passenger_count, COUNT(*) AS size FROM pp16 group by passenger_count order by size")

ppdistime15 = sqlContext.sql("SELECT PUDate, count(*) AS sharedRideTimes FROM pp15 where passenger_count > 1 group by PUDate order by PUDate")

#1. share count change vs date
ppdistime15 = sqlContext.sql("SELECT PUDate, count(*) AS sharedRideTimes FROM pp15 where passenger_count = 1 group by PUDate order by PUDate")

ppdistime15.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/user/jw4468/bigDataProject/data/cleaned/15SingleRidevsDay.csv")

ppdistime16 = sqlContext.sql("SELECT PUDate, count(*) AS sharedRideTimes FROM pp16 where passenger_count = 1 group by PUDate order by PUDate")

ppdistime16.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/user/jw4468/bigDataProject/data/cleaned/16SingleRidevsDay.csv")


#2. share ride count change vs day of week
ppdistime15Mon = sqlContext.sql("SELECT hour(tpep_pickup_datetime) as time,count(*) AS Monday2015 FROM p where passenger_count > 1 AND  date_format(tpep_pickup_datetime, 'EEEE') = 'Monday' group by hour(tpep_pickup_datetime) order by time")

ppdistime15Tue = sqlContext.sql("SELECT hour(tpep_pickup_datetime) as time,count(*) AS Tuesday2015 FROM p where passenger_count > 1 AND  date_format(tpep_pickup_datetime, 'EEEE') = 'Tuesday' group by hour(tpep_pickup_datetime) order by time")

ppdistime15Wed = sqlContext.sql("SELECT hour(tpep_pickup_datetime) as time,count(*) AS Wensday2015 FROM p where passenger_count > 1 AND  date_format(tpep_pickup_datetime, 'EEEE') = 'Wednesday' group by hour(tpep_pickup_datetime) order by time")

ppdistime15Thu = sqlContext.sql("SELECT hour(tpep_pickup_datetime) as time,count(*) AS Thursday2015 FROM p where passenger_count > 1 AND  date_format(tpep_pickup_datetime, 'EEEE') = 'Thursday' group by hour(tpep_pickup_datetime) order by time")

ppdistime15Fri = sqlContext.sql("SELECT hour(tpep_pickup_datetime) as time,count(*) AS Friday2015 FROM p where passenger_count > 1 AND  date_format(tpep_pickup_datetime, 'EEEE') = 'Friday' group by hour(tpep_pickup_datetime) order by time")


ppdistime15Sat = sqlContext.sql("SELECT hour(tpep_pickup_datetime) as time,count(*) AS Saturday2015 FROM p where passenger_count > 1 AND  date_format(tpep_pickup_datetime, 'EEEE') = 'Saturday' group by hour(tpep_pickup_datetime) order by time")

ppdistime15Sun = sqlContext.sql("SELECT hour(tpep_pickup_datetime) as time,count(*) AS Sunday2015 FROM p where passenger_count > 1 AND  date_format(tpep_pickup_datetime, 'EEEE') = 'Sunday' group by hour(tpep_pickup_datetime) order by time")

ppdistime16Mon = sqlContext.sql("SELECT hour(tpep_pickup_datetime) as time,count(*) AS Monday2016 FROM p2 where passenger_count > 1 AND  date_format(tpep_pickup_datetime, 'EEEE') = 'Monday' group by hour(tpep_pickup_datetime) order by time")

ppdistime16Tue = sqlContext.sql("SELECT hour(tpep_pickup_datetime) as time,count(*) AS Tuesday2016 FROM p2 where passenger_count > 1 AND  date_format(tpep_pickup_datetime, 'EEEE') = 'Tuesday' group by hour(tpep_pickup_datetime) order by time")

ppdistime16Wed = sqlContext.sql("SELECT hour(tpep_pickup_datetime) as time,count(*) AS Wensday2016 FROM p2 where passenger_count > 1 AND  date_format(tpep_pickup_datetime, 'EEEE') = 'Wednesday' group by hour(tpep_pickup_datetime) order by time")

ppdistime16Thu = sqlContext.sql("SELECT hour(tpep_pickup_datetime) as time,count(*) AS Thursday2016 FROM p2 where passenger_count > 1 AND  date_format(tpep_pickup_datetime, 'EEEE') = 'Thursday' group by hour(tpep_pickup_datetime) order by time")

ppdistime16Fri = sqlContext.sql("SELECT hour(tpep_pickup_datetime) as time,count(*) AS Friday2016 FROM p2 where passenger_count > 1 AND  date_format(tpep_pickup_datetime, 'EEEE') = 'Friday' group by hour(tpep_pickup_datetime) order by time")

ppdistime16Sat = sqlContext.sql("SELECT hour(tpep_pickup_datetime) as time,count(*) AS Saturday2016 FROM p2 where passenger_count > 1 AND  date_format(tpep_pickup_datetime, 'EEEE') = 'Saturday' group by hour(tpep_pickup_datetime) order by time")

ppdistime16Sun = sqlContext.sql("SELECT hour(tpep_pickup_datetime) as time,count(*) AS Sunday2016 FROM p2 where passenger_count > 1 AND  date_format(tpep_pickup_datetime, 'EEEE') = 'Sunday' group by hour(tpep_pickup_datetime) order by time")

df151= ppdistime15Mon.toPandas()
df152= ppdistime15Tue.toPandas()
df153= ppdistime15Wed.toPandas()
df154= ppdistime15Thu.toPandas()
df155= ppdistime15Fri.toPandas()
df156= ppdistime15Sat.toPandas()
df157= ppdistime15Sun.toPandas()
df161= ppdistime16Mon.toPandas()
df162= ppdistime16Tue.toPandas()
df163= ppdistime16Wed.toPandas()
df164= ppdistime16Thu.toPandas()
df165= ppdistime16Fri.toPandas()
df166= ppdistime16Sat.toPandas()
df167= ppdistime16Sun.toPandas()

df15 = df151.merge(df152, on='time',how='inner').merge(df153, on='time',how='inner').merge(df154, on='time',how='inner').merge(df155, on='time',how='inner').merge(df156, on='time',how='inner').merge(df157, on='time',how='inner')

df16= df161.merge(df162, on='time',how='inner').merge(df163, on='time',how='inner').merge(df164, on='time',how='inner').merge(df165, on='time',how='inner').merge(df166, on='time',how='inner').merge(df167, on='time',how='inner')
df15.to_csv('/home/jw4468/project/cleaned/15ShareRidevsHour.csv')
df16.to_csv('/home/jw4468/project/cleaned/16ShareRidevsHour.csv')


#3. share ride passenger (total, avg) count change vs precipitation
w15 = spark.read.option("header",True).csv("/user/jw4468/bigDataProject/data/cleaned/Weather2015.csv")
w16 = spark.read.option("header",True).csv("/user/jw4468/bigDataProject/data/cleaned/Weather2016.csv")


rain15=w15.select("YEARMODA","PRCP")
rain15=rain15.withColumn('PRCP',rain15['PRCP'].substr(1,5))
rain15 =rain15.withColumn("PRCP",rain15["PRCP"].cast(FloatType())).withColumn("YEARMODA",rain15["YEARMODA"].cast('date'))

rain16=w16.select("YEARMODA","PRCP")
rain16=rain16.withColumn('PRCP',rain16['PRCP'].substr(1,5))
rain16 =rain16.withColumn("PRCP",rain16["PRCP"].cast(FloatType())).withColumn("YEARMODA",rain16["YEARMODA"].cast('date'))


rain15.registerTempTable('r15')
rain16.registerTempTable('r16')


share15rain = sqlContext.sql("SELECT PUTime, PUDate, PRCP, passenger_count FROM pp15 JOIN r15 ON pp15.PUDate = r15.YEARMODA where PRCP > 0 and passenger_count > 1")

share16rain = sqlContext.sql("SELECT PUTime, PUDate, PRCP, passenger_count FROM pp16 JOIN r16 ON pp16.PUDate = r16.YEARMODA where PRCP > 0 and passenger_count > 1")

share15rain.registerTempTable('share15')
share16rain.registerTempTable('share16')


avgshare15 = sqlContext.sql("SELECT PUDate, PRCP, count(passenger_count) as shareCount, sum(passenger_count) as totalPeopleCount, avg(passenger_count)as avgShareCount From share15 group by PUDate, PRCP order by PUDate")

avgshare16 = sqlContext.sql("SELECT PUDate, PRCP, count(passenger_count) as shareCount, sum(passenger_count) as totalPeopleCount, avg(passenger_count)as avgShareCount From share16 group by PUDate, PRCP order by PUDate")

avgshare15.registerTempTable('share15avg')
avgshare16.registerTempTable('share16avg')


share15VSRain = sqlContext.sql("SELECT PRCP, sum(shareCount) as shareCount, sum(totalPeopleCount) as totalPeopleCount, avg(avgShareCount)as avgShareCount From share15avg group by PRCP order by PRCP")

share16VSRain = sqlContext.sql("SELECT PRCP, sum(shareCount) as shareCount, sum(totalPeopleCount) as totalPeopleCount, avg(avgShareCount)as avgShareCount From share16avg group by PRCP order by PRCP")


#share change statistics with rain day
avgshare15.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/user/jw4468/bigDataProject/data/cleaned/share15vsRainDay.csv")

avgshare16.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/user/jw4468/bigDataProject/data/cleaned/share16vsRainDay.csv")


#share change statistics with rain
share15VSRain.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/user/jw4468/bigDataProject/data/cleaned/share15vsPRCP.csv")

share16VSRain.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/user/jw4468/bigDataProject/data/cleaned/share16vsPRCP.csv")


#4. single ride count change vs precipitation
#single ride in rainny day
single15rain = sqlContext.sql("SELECT PUTime, PUDate, PRCP, passenger_count FROM pp15 JOIN r15 ON pp15.PUDate = r15.YEARMODA where PRCP > 0 and passenger_count = 1")
single16rain = sqlContext.sql("SELECT PUTime, PUDate, PRCP, passenger_count FROM pp16 JOIN r16 ON pp16.PUDate = r16.YEARMODA where PRCP > 0 and passenger_count = 1")


single15rain.registerTempTable('single15r')
single16rain.registerTempTable('single16r')

single15VSRain = sqlContext.sql("SELECT PRCP, sum(passenger_count) From single15r group by PRCP order by PRCP")

single16VSRain = sqlContext.sql("SELECT PRCP, sum(passenger_count) From single16r group by PRCP order by PRCP")

single15VSRain.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/user/jw4468/bigDataProject/data/cleaned/single15vsPRCP.csv")

single16VSRain.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/user/jw4468/bigDataProject/data/cleaned/single16vsPRCP.csv")

#5. tip fraction vs passenger count
# every day
Tip15 = sqlContext.sql("SELECT PUTime, PUDate, passenger_count, Round(tip_amount/total_amount,2) as tipFraction FROM pp15 where passenger_count > 0 and payment_type = 1")
Tip16 = sqlContext.sql("SELECT PUTime, PUDate, passenger_count, Round(tip_amount/total_amount,2) as tipFraction FROM pp16 where passenger_count > 0 and payment_type = 1")

Tip15.registerTempTable('tip15')
Tip16.registerTempTable('tip16')
tipVsPcnt15 = sqlContext.sql("SELECT passenger_count, avg(tipFraction) From Tip15 group by passenger_count order by passenger_count")
tipVsPcnt16 = sqlContext.sql("SELECT passenger_count, avg(tipFraction) From Tip16 group by passenger_count order by passenger_count")

tipVsPcnt15.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/user/jw4468/bigDataProject/data/cleaned/TipFracvsPcnt15.csv")

tipVsPcnt16.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/user/jw4468/bigDataProject/data/cleaned/TipFracvsPcnt16.csv")


#6. tip fraction vs passenger count vs precipitation 
Tip15rain = sqlContext.sql("SELECT PUTime, PUDate, passenger_count, PRCP, Round(tip_amount/total_amount,2) as tipFraction FROM pp15 JOIN r15 ON pp15.PUDate = r15.YEARMODA where passenger_count > 0 and PRCP > 0 and payment_type = 1")
Tip16rain = sqlContext.sql("SELECT PUTime, PUDate, passenger_count, PRCP, Round(tip_amount/total_amount,2) as tipFraction FROM pp16 JOIN r16 ON pp16.PUDate = r16.YEARMODA where passenger_count > 0 and PRCP > 0 and payment_type = 1")

Tip15rain.registerTempTable('tip15Rain')
Tip16rain.registerTempTable('tip16Rain')
tipRVsPcnt15 = sqlContext.sql("SELECT PRCP, passenger_count, avg(tipFraction) From tip15Rain group by PRCP, passenger_count order by PRCP, passenger_count")
tipRVsPcnt16 = sqlContext.sql("SELECT PRCP, passenger_count, avg(tipFraction) From tip16Rain group by PRCP, passenger_count order by PRCP, passenger_count")

tipRVsPcnt15.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/user/jw4468/bigDataProject/data/cleaned/TipFravsPRCPvsPcnt15.csv")

tipRVsPcnt16.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/user/jw4468/bigDataProject/data/cleaned/TipFravsPRCPvsPcnt16.csv")



