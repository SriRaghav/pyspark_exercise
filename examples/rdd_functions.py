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

def fun_temp(x):
    split_string = x.split(",")
    channelid = split_string[0]
    videoid = int(split_string[1])
    subscriber_count = int(split_string[2])
    return (subscriber_count, videoid*subscriber_count)

yt_path = '/Users/raghavendiran.n/Learn/resources/yt_small_9_noheader.csv'

rdd1 = spark.sparkContext.textFile(yt_path)
#yt_new_df = rdd1.map(lambda x: fun_temp(x)).collect()
video_subscriber = rdd1.flatMap(lambda x: fun_temp(x))
print(video_subscriber.collect())

#subscriber_less_than_threshold = rdd1.filter(lambda x: int(x.split(",")[2]) < 2000).collect()

input("Press any key to exit!")
