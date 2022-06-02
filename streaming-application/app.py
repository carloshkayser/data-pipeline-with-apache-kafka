from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

def main():
  
  spark = SparkSession.builder.getOrCreate()

  


if __name__ == '__main__':
  main()