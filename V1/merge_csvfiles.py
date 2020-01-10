import spark
from pyspark.sql import SparkSession
from pyspark.sql.types import *

def main():
   spark = SparkSession.builder.appName("Merged_kvs_stud").getOrCreate()

   #  Reading 1st CSV file

   df1 = spark.read.format("csv").option("header", True).load("merged_master_school.csv")

   # Reading 2nd CSV file

   df2 = spark.read.format("csv").option("header", True).load("cct_result_pisa21.csv")

   # Renaming column of 2nd CSV file
   df2 = df2.withColumnRenamed("cls","class")

   # Joining 2 CSV files
   merged_data =df1.join(df2, ["std_no","class"], how="left_outer").select("slno","schcd","std_no","class","se_x","cwsn","cwsn_type","dob","med_ium","mgt",\
                                                                "admision_id","stnm","dstnm","mgt_nm","medm","typ_name","year_ag","month_ag",\
                                                                "days_ag","schnm","board","affil_no","sch_head","desig","mob","sch_f_name",\
                                                                "sch_add1","sch_add2","sch_add3","rur_urb","catg","cw_yn","Management - Breakdown",\
                                                                "School Name","Type","Cluster","head_teacher","mobile_teacher","email","lclass",
                                                                "hclass","Management","sch_type", "udise_cd","sci6","mth6","eng_rl6","hin_rl6","sci9","mth9","eng_rl9","hin_rl9")



   # saving Result in file
   merged_data.coalesce(1).write.format("csv").option("header", True).mode("overwrite")\
       .save("h/home/ramya/Documents/merged_final_data1")
   # df = df.select("date").distinct()
   # print("df count:::::::::::::::::::::::>>>>>>>>>>>>>>>>>>>>>.",df.count())
   return
main()



