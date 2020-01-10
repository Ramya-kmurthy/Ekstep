from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import lit, unix_timestamp

import time
import datetime

spark = SparkSession.builder.appName("school_master_join").getOrCreate()

# Reading CSV file
df = spark.read.format("csv").option('header',True).load("/home/ramya/snap/skype/common/chandigrah_merged_data.csv")

# Date formate
timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

# Adding timestamp column to file
new_df = df.withColumn('time',unix_timestamp(lit(timestamp),'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
new_df.show(truncate = False)
# Saving Resulted Data in a File
new_df.coalesce(1).write.format("csv").option("header",True).mode('overwrite')\
    .save('/home/ramya/Documents/timestamp1')