from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import count
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName("school_master_join").getOrCreate()

# Reading 1st CSV file
df = spark.read.format("csv").option('header',True).load("C:\\Users\\ramya\\Documents\\PycharmProjects\\Firstproject\\test_once\\Master_stud_data.csv")
# print("df count::::::::::::::::::::::>>>>>>>>>>>>>>>>>>>>>>>>>.",df.count())

#  Reading 2nd CSV file
df1 = spark.read.format("csv").option('header',True).load('C:\\Users\\ramya\\Documents\\PycharmProjects\\Firstproject\\test_once\\school_master.csv')

#Renaming  Column name  of 2nd CSV file
df1 = df1.withColumnRenamed("School Code","schcd")
print("df1 count::::::::::::::::::::::>>>>>>>>>>>>>>>>>>>>>>>>>.",df1.count())

# Joining TWO CSV files
df_data = df.join(df1,"schcd",how="left_outer").select("slno","schcd","stdid","class","se_x","cwsn","cwsn_type","dob",df["med_ium"],"mgt","admision_id"\
                ,"stnm","dstnm","mgt_nm","medm","typ_name","year_ag","month_ag","days_ag","schnm","board",df["affil_no"],"sch_head","desig","mob"\
            ,"sch_f_name","sch_add1","sch_add2","sch_add3",df["rur_urb"],"catg","cw_yn",df["Management - Breakdown"],"School Name",\
                            "Type","Cluster","head_teacher","mobile_teacher","email","lclass","hclass","Management","sch_type")
#  filtering Data which is equal to Chandigar city
df_data = df_data.filter(df['stnm'] == "Chandigarh")

#  Calculating Percentage
df_agg = df_data.select('stdid').count()
df_final =df_data.withColumn("percentage_computation",lit(1/df_agg))
df_final = df_final.withColumnRenamed("stdid","std_no")

# Reading 3rd CSV file
df3 = spark.read.format("csv").option("header",True)\
    .load('C:\\Users\\ramya\\Documents\\PycharmProjects\\Firstproject\\test_once\\cct_result_pisa21.csv')

# Joing 3rd CSV file with above joined file
df_final_done = df_final.join(df3,["std_no"],how="left_outer")
df_final_done.printSchema()

#  Saving result in a file
df_final_done.coalesce(1).write.format("csv").option("header",True).mode('overwrite')\
    .save('C:\\Users\\ramya\\Documents\\Pycha rmProjects\\Firstproject\\test_once\\final_done')