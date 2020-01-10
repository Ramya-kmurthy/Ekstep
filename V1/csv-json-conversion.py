from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName("PISA_Dashboard").getOrCreate();
# TO read CSV file
df_global = spark.read.csv("/home/ramya/Documents/timestamp1/chandigarh_mergeddata_timestamp.csv",inferSchema=True,header=True);
#  To wite CSV in Json Formate
df_global.coalesce(1).write.format("json").option("header",True).mode("overwrite").save("/home/ramya/PISA_Consolidated_Json")