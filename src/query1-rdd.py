#!/bin/python3
from pyspark.sql import SparkSession
from io import StringIO
import csv
import sys
import time

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

file = open('times.txt', 'a+')

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

def mapper1(x):
    title=x[1]
    year=x[3].split("-")[0]
    cost=int(x[5])
    revenue=int(x[6])
    profit=((revenue-cost)/cost)*100
    return [year,(title,profit)]

def filter1(x):
    year=x[3].split("-")[0]
    cost=x[5]
    revenue=x[6]
    if year=="" or cost=='0' or revenue=='0':
        return False
    return int(year)>=2000

spark = SparkSession.builder.appName("query1-rdd").getOrCreate()
sc = spark.sparkContext

rdd = sc.textFile('hdfs://master:9000/movie_data/movies.csv')

# print(rdd.take(10))

start_time = time.time()
rdd = rdd.map(split_complex).filter(filter1).map(mapper1).reduceByKey(lambda x,y: max((x, y), key=lambda x: x[1])).map(lambda x:(x[0],x[1][0])).sortByKey()

end_time = time.time()

file.write(str((end_time-start_time)/60)+'\n')

file.close()
print(rdd.collect())
