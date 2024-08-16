from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import to_date

# Removing hard coded password - using os module to import them
import os
import sys

# Required configuration to load S3/Minio access credentials securely - no hardcoding keys into code
conf = SparkConf()

#Removed Some configuration 

# Create SparkSession Object - tell the cluster the FQDN of the host system)
spark = SparkSession.builder.appName("spatil62-minio-read-and-process-30").config('spark.driver.host','spark-edge-vm0.service.consul').config(conf=conf).getOrCreate()

# Read the datatype into a DataFrame
df = spark.read.csv('s3:20.txt')

# Custom code needed to split the original source -- it has a column based delimeter
# Each record is a giant string - with predefined widths being the columns
# Example: 0088999999949101950123114005+42550-092400SAO  +026599999V02015859008850609649N012800599-00504-00785999999EQDN01 00000JPWTH
splitDF = df.withColumn('WeatherStation', df['_c0'].substr(5, 6)) \
.withColumn('WBAN', df['_c0'].substr(11, 5)) \
.withColumn('ObservationDate',to_date(df['_c0'].substr(16,8), 'yyyyMMdd')) \
.withColumn('ObservationHour', df['_c0'].substr(24, 4).cast(IntegerType())) \
.withColumn('Latitude', df['_c0'].substr(29, 6).cast('float') / 1000) \
.withColumn('Longitude', df['_c0'].substr(35, 7).cast('float') / 1000) \
.withColumn('Elevation', df['_c0'].substr(47, 5).cast(IntegerType())) \
.withColumn('WindDirection', df['_c0'].substr(61, 3).cast(IntegerType())) \
.withColumn('WDQualityCode', df['_c0'].substr(64, 1).cast(IntegerType())) \
.withColumn('SkyCeilingHeight', df['_c0'].substr(71, 5).cast(IntegerType())) \
.withColumn('SCQualityCode', df['_c0'].substr(76, 1).cast(IntegerType())) \
.withColumn('VisibilityDistance', df['_c0'].substr(79, 6).cast(IntegerType())) \
.withColumn('VDQualityCode', df['_c0'].substr(86, 1).cast(IntegerType())) \
.withColumn('AirTemperature', df['_c0'].substr(88, 5).cast('float') /10) \
.withColumn('ATQualityCode', df['_c0'].substr(93, 1).cast(IntegerType())) \
.withColumn('DewPoint', df['_c0'].substr(94, 5).cast('float')) \
.withColumn('DPQualityCode', df['_c0'].substr(99, 1).cast(IntegerType())) \
.withColumn('AtmosphericPressure', df['_c0'].substr(100, 5).cast('float')/ 10) \
.withColumn('APQualityCode', df['_c0'].substr(105, 1).cast(IntegerType())).drop('_c0')

splitDF.printSchema()
splitDF.show(5)

splitDF.write.csv("s3:20-csv")
splitDF.write.parquet("s3:20-parquet")
splitDF.write.json("s3:20-json")
splitDF.write.format("overwrite").option("compression", "lz4").csv("s3:20-lz4")
writeDF = splitDF.coalesce(1)
writeDF.write.csv("s3:20-single")
