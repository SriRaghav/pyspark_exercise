import datetime

import pyspark
from pyspark.sql import SparkSession
import pandas as pd


spark = SparkSession.builder.master("local[8]") \
    .appName('SparkByExamples.com') \
    .getOrCreate()

# Create the pandas DataFrame

pandasDF = pd.read_csv('../resources/yt_small_9.csv')

pandas_start = datetime.datetime.now()
#pandasDF = pd.DataFrame(data, columns=['Name', 'Age'])

sparkDF = spark.createDataFrame(pandasDF)
sparkDF.printSchema()
sparkDF.show()

pandas_end = datetime.datetime.now()
print((pandas_end-pandas_start)/1000)

#sparkDF=spark.createDataFrame(pandasDF.astype(str))
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, FloatType, LongType
'''mySchema = StructType([ StructField("totalviews_channelelapsedtime", StringType(), True) \
                          ,StructField("channelId", StringType(), True)
                        ,StructField("videoCategoryId", IntegerType(), True)
                        #,StructField("channelViewCount", LongType(), True)
                        #,StructField("likes_subscribers", FloatType(), True)
                        #,StructField("views_subscribers", FloatType(), True)
                        #,StructField("video_count", IntegerType(), True)
                        ,StructField("subscriberCount", IntegerType(), True))
                        #,StructField("videoId", StringType(), True)])'''

mySchema = StructType([ StructField("channelId", StringType(), True)
                        ,StructField("videoCategoryId", IntegerType(), True)
                        ,StructField("subscriberCount", IntegerType(), True)])

sparkDF2 = spark.createDataFrame(pandasDF,schema=mySchema)
sparkDF2.printSchema()
sparkDF2.show()

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled","true")
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled","true")

pandasDF2=sparkDF2.select("*").toPandas
print(pandasDF2)
