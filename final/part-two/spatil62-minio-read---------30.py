from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import to_date

# Removing hard coded password - using os module to import them
import os
import sys
 

# Required configuration to load S3/Minio access credentials securely - no hardcoding keys into code
conf = SparkConf()

#Removed Some configuration 

# Create SparkSession Object - tell the cluster the FQDN of the host system)
spark = SparkSession.builder.appName("spatil62-minio-read-part2").config('spark.driver.host','').config(conf=conf).getOrCreate()

df = spark.read.csv('s3:30.txt')

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


schema_value = StructType([StructField('WeatherStation', StringType(), True),
        StructField('WBAN', StringType(), True),
        StructField('ObservationDate',  DateType(), True),
        StructField('ObservationHour', IntegerType(), True),
        StructField('Latitude', FloatType(), True),   
        StructField('Longitude', FloatType(), True),
        StructField('Elevation', IntegerType(), True),
        StructField('WindDirection', IntegerType(), True),
        StructField('WDQualityCode', IntegerType(), True),
        StructField('SkyCeilingHeight', IntegerType(), True),
        StructField('SCQualityCode', IntegerType(), True),
        StructField('VisibilityDistance', IntegerType(), True),
        StructField('VDQualityCode', IntegerType(), True),
        StructField('AirTemperature', FloatType(), True),
        StructField('ATQualityCode', FloatType(), True),
        StructField('DewPoint', FloatType(), True),
        StructField('DPQualityCode', IntegerType(), True),
        StructField('AtmosphericPressure', FloatType(), True),
        StructField('APQualityCode', IntegerType(), True)])


# Read the partitioned CSV into CSV
csvdf = spark.read.csv('s3:30.txt', header=True, schema=schema_value)



csvdf.write.format("jdbc").option("url","").option("driver","").option("dbtable","spatil62-thirties").option("user",os.getenv("MYSQLUSER")).option("truncate",True).mode("overwrite").option("password", os.getenv("MYSQLPASS")).save()

jdbcDF = csvdf.read.format("jdbc").option("url","").option("driver","").option("dbtable","spatil62-thirties").option("user",os.getenv("MYSQLUSER")).option("truncate",True).mode("overwrite").option("password", os.getenv("MYSQLPASS")).load()

jdbcDF.show()

print(jdbcDF.schema)



df.write.format("jdbc").option("url","").option("driver","").option("dbtable","SN2thirties").option("user",os.getenv('MYSQLUSER')).option("truncate",True).mode("overwrite").option("password",os.getenv('MYSQLPASS')).save()

# connect to the MariaDB server
Sn2_maria_DB = spark.read.format("jdbc").option("url","").option("driver","").option("dbtable","SN2thirties").option("user",os.getenv('MYSQLUSER')).option("password", os.getenv('MYSQLPASS')).load()

Sn2_maria_DB.show(10)
Sn2_maria_DB.printSchema()


