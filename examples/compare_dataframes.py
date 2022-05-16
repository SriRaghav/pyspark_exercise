import abc
import datetime
from abc import ABC

from pyspark.sql import SparkSession
import pandas as pd


class CompareFunctions(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def calculate_records(self):
        pass

    def compute_timetaken(self, start_time, end_time):
        return (end_time-start_time)/1000


class SparkDataframe(CompareFunctions, ABC):

    def __init__(self, filepath):
        self.spark = SparkSession.builder.master("local[8]") \
            .appName('SparkByExamples.com') \
            .getOrCreate()
        self.dataframe = self.spark.read.csv(filepath, header=True)

    def calculate_records(self):
        return self.dataframe.count()


class PandasDataframe(CompareFunctions, ABC):

    def __init__(self, file_path):
        self.dataframe = pd.read_csv(file_path)

    def calculate_records(self):
        return len(self.dataframe)


class Dataframe(object):
    @staticmethod
    def get_instance(type, filepath):
        if type == "Spark":
            return SparkDataframe(filepath)
        elif type == "Pandas":
            return PandasDataframe(filepath)


myDf = Dataframe.get_instance("Pandas", "../resources/yt.csv")
start_time = datetime.datetime.now()
value = myDf.calculate_records()
end_time = datetime.datetime.now()
print(start_time)
print(end_time)

print (myDf.compute_timetaken(start_time, end_time))
print(value)

