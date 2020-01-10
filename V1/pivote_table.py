from pyspark.sql import SparkSession
from pyspark.sql.functions import array, col, explode, struct, lit
from pyspark.sql import functions as F


def to_long(df, by):
    # Filter dtypes and split into column names and type description
    cols, dtypes = zip(*((c, t) for (c, t) in df.dtypes if c not in by))
    # Spark SQL supports only homogeneous columns
    assert len(set(dtypes)) == 1, "All columns have to be of the same type"

    # Create and explode an array of (column_name, column_value) structs
    kvs = explode(array([
        struct(lit(c).alias("key"), col(c).alias("val")) for c in cols])).alias("kvs")

    return \
        df.select(by + [kvs]).select(by + ["kvs.key", "kvs.val"])


spark = SparkSession.builder.appName("transpose").getOrCreate()
df = spark.read.format("csv").option("header", True).load("/home/ramya/Documents/data12/clean_modified_final_data.csv")
df = df.select("udise_cd","std_no","cls", "sci6", "mth6", "eng_rl6", "hin_rl6", "sci9", "mth9", "eng_rl9", "hin_rl9")
df = to_long(df, ["udise_cd","std_no","cls"])
df.coalesce(1).write.format("csv").option("header", True).mode("overwrite").save("/home/ramya/Downloads/FINAL_PIVOTED_CCT")
