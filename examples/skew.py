from pyspark.sql import SparkSession


class SkewRDD:

    def __init__(self, file_path):
        self.ss = SparkSession.builder \
            .master("local[8]") \
            .appName("RDD_ALL") \
            .getOrCreate()
        self.sc = self.ss.sparkContext
        self.big_rdd = self.sc.range(1, 16000000, 1, 8)
        self.small_rdd = self.sc.parallelize([(1, "a"), (2, "b"), (3, "c"), (4, "d")])
        # self.sc.parallelize(range(1, 16000000, 1), 8)
        self.file_path = file_path

    def compute_skew_data(self):

        def skewed_bins(number, bin_size=100000):
            return 1 if 0 < number <= bin_size else 2

        def print_partition(rdd_partition):
            yield sum(row[1] for row in rdd_partition)

        print(self.big_rdd
              .map(lambda x: (skewed_bins(x, 1000000), x))
              .partitionBy(5)
              .mapPartitions(print_partition)
              .collect())

    def compute_unskew_data(self):

        def equal_bins(number, bin_size=100000):
            return number // bin_size

        def print_partition(rdd_partition):
            yield sum(row[1] for row in rdd_partition)

        print(self.big_rdd
              .map(lambda x: (equal_bins(x, 1000000), x))
              .partitionBy(16)
              .mapPartitions(print_partition)
              .collect())


    def join_rdd(self):
        print(self.small_rdd.join(self.big_rdd).collect())

if __name__ == '__main__':
    rdd_spark = SkewRDD("/Users/raghavendiran.n/Learn/resources/yt_small_9_noheader.csv")
    #rdd_spark.compute_skew_data()
    rdd_spark.join_rdd()
    # rdd_spark.compute_unskew_data()

    # Strategies to fixing skewed data
    # 1. More partitions
    # 2. BroadCast Join Threshold
    # 3. Iterative, chunked broadcast join
    # 4. Adding Salt

    input("Press any key to exit!")
