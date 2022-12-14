from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col
spark = SparkSession.builder.master("local[1]") .appName('Example').getOrCreate()


df=spark.read.option("header",True).csv("/config/workspace/file_8.csv")

df_new =  df.withColumn('hobby',explode(split("hobbies","-")))

df_final = df_new.select(col("name"),col("age"),col("hobby")).show()
