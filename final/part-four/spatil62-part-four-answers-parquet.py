from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import to_date, percentile_approx

# Removing hard coded password - using os module to import them
import os
import sys

# Required configuration to load S3/Minio access credentials securely - no hardcoding keys into code
conf = SparkConf()

#removed some configurations 

# Create SparkSession Object - tell the cluster the FQDN of the host system)
spark = SparkSession.builder.appName("spatil62-part-four").config('spark.driver.host','').config(conf=conf).getOrCreate()

df = spark.read.format("parquet").load('s3:60-parquet')
df.createOrReplaceTempView("partfourData")

# Count the number of records
count = spark.sql("SELECT year(ObservationDate) As Year, count(*) as cnt FROM partfourData WHERE Month(ObservationDate) = '2'group by Year;")

count.show(10)

#Average air temperature for month of February
average = spark.sql(" SELECT year(ObservationDate) As Year, AVG(AirTemperature) AS AvgTemp FROM partfourData WHERE Month(ObservationDate) = '2' AND AirTemperature < 999 AND AirTemperature > -999 group by Year;")
average.show(10)

# Median air temperature for the month of February
median = df.groupBy("ObservationDate", "WeatherStation").agg(percentile_approx("AirTemperature",0.5).alias("MedianAirTemp"))
median.show(10)

# Standard Deviation of air temperature for month of February
stdAir = spark.sql(" SELECT year(ObservationDate) As Year, std(AirTemperature) as sd FROM partfourData WHERE Month(ObservationDate) = '2' AND AirTemperature < 999 AND AirTemperature > -999 group by Year ;")
stdAir.show(10)

# Find AVG air temperature per StationID in the month of February
avgFebAir = spark.sql(" SELECT year(ObservationDate) As Year, WeatherStation as Station_id, std(AirTemperature) as sd_st FROM partfourData WHERE Month(ObservationDate) = '2' AND AirTemperature < 999 AND AirTemperature > -999 group by Year, WeatherStation;")
avgFebAir.show(10)

count.write.format("parquet").mode("overwrite").parquet("s3a://spatil62/spatil62-part-four-answers-count-parquet")
average.write.format("parquet").mode("overwrite").parquet("s3a://spatil62/spatil62-part-four-answers-avg-parquet")
median.write.format("parquet").mode("overwrite").parquet("s3a://spatil62/spatil62-part-four-answers-median-parquet")
stdAir.write.format("parquet").mode("overwrite").parquet("s3a://spatil62/spatil62-part-four-answers-stddev-parquet")
