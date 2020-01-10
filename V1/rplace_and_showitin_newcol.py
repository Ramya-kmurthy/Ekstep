import spark
from pyspark.sql import SparkSession, udf
from pyspark.sql.types import *
from pyspark.sql.functions import when


def main():
    spark = SparkSession.builder.appName("Merged_kvs_stud").getOrCreate()

    # Reading CSV file
    df1 = spark.read.format("csv").option("header", True).load("/home/ramya/Documents/data11/11.csv")

    # Replacing Data in a column
    # df2= df1.withColumn('Score_M', when(df1.Score_M.between (0.75,1) ,"(d) 75% - 100%").otherwise(df1.Score_M))
    df2 = df1.withColumn('Score_M', when(df1.Score_M < 0.25, "(a) below  25%").otherwise(df1.Score_M))

    # Saving resulted data in a file
    df2.coalesce(1).write.format("csv").option("header", True).mode("overwrite") \
        .save("/home/ramya/Documents/data12")
    

    return


main()
