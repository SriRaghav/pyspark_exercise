from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder.master("local[8]") \
    .appName('SparkByExamples.com') \
    .getOrCreate()

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

mySchema = StructType([StructField("channelId", StringType(), True)
                          , StructField("videoCategoryId", IntegerType(), True)
                          , StructField("subscriberCount", IntegerType(), True)])

pandasDF = pd.read_csv('../resources/yt.csv')
print(len(pandasDF))

# sparkDf = spark.createDataFrame(pandasDF)

yt_path = '/Users/raghavendiran.n/Learn/resources/yt.csv'

rdd1 = spark.sparkContext.textFile(yt_path)
print("Spark Dataframe created")
print(rdd1.repartition(16).count())
print(rdd1.repartition(24).count())
print(rdd1.repartition(32).count())
print(rdd1.repartition(64).count())
print("RDD created")

input("Press any key to exit!")
