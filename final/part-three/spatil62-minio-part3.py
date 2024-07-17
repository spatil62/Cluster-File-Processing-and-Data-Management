from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import *
from pyspark.sql.functions import count, year, desc, col

# Removing hard coded password - using os module to import them
import os
import sys

   
# Required configuration to load S3/Minio access credentials securely - no hardcoding keys into code
conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.3')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider')

conf.set('spark.hadoop.fs.s3a.access.key', os.getenv('SECRETKEY'))
conf.set('spark.hadoop.fs.s3a.secret.key', os.getenv('ACCESSKEY'))
conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio1.service.consul:9000")
conf.set("fs.s3a.path.style.access", "true")
conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("fs.s3a.connection.ssl.enabled", "false")

# Create SparkSession Object - tell the cluster the FQDN of the host system)
spark = SparkSession.builder.appName("spatil62-minio-part3-second").config('spark.driver.host','spark-edge-vm0.service.consul').config(conf=conf).getOrCreate()

# # show the result
# result_df.show()

schema = StructType([StructField('WeatherStation', StringType(), True),
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



# Reading from the parquet
df = spark.read.parquet('s3a://spatil62/60-parquet',header=True, schema=schema)
print(df.schema)
df.select("WeatherStation", "VisibilityDistance", "ObservationDate").where(col("VisibilityDistance") < 200).groupBy("WeatherStation", "VisibilityDistance", year("ObservationDate")).count().orderBy(desc(year("ObservationDate"))).show(10)

#