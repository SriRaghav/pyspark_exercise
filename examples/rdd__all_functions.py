from pyspark.sql import SparkSession


class RDDSpark:

    def __init__(self, file_path):
        self.ss = SparkSession.builder \
            .master("local[8]") \
            .appName("RDD_ALL") \
            .getOrCreate()
        self.sc = self.ss.sparkContext
        self.file_path = file_path
        self.rdd = self.sc.textFile(self.file_path, 8)

    def spark_properties(self):
        print(self.sc.defaultParallelism)
        print(self.sc.getConf().getAll())

    def map_functions(self):
        def sum_subscriber_count(iterator):
            yield sum(iterator)

        rdd_map = self.rdd.map(lambda x: int(x.split(",")[2]) / 100).collect()
        rdd_map_partition = self.rdd \
            .repartition(8) \
            .map(lambda x: int(x.split(",")[2])) \
            .mapPartitions(sum_subscriber_count) \
            .collect()

        rdd_map_values = self.rdd.map(lambda x: (x, x.split(","))).mapValues(lambda x:len(x)).collect()

    def flat_functions(self):
        rdd_flatmap = self.rdd.map(lambda x: x.split(",")).flatMap(lambda x: x).collect()
        rdd_flatmap_values = self.rdd.map(lambda x: (x, x.split(","))).flatMapValues(lambda x: x).collect()

    def filter_functions(self):
        rdd_filter = self.rdd.filter(lambda x: int(x.split(",")[2]) < 1000).collect()



if __name__ == '__main__':
    rdd_spark = RDDSpark("/Users/raghavendiran.n/Learn/resources/yt_small_9_noheader.csv")
    rdd_spark.spark_properties()
    rdd_spark.map_functions()
    rdd_spark.flat_functions()
    rdd_spark.filter_functions()
    input("Press any key to exit!")
