#!/bin/python3
from pyspark.sql import SparkSession
from io import StringIO
import csv
import time
import sys

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
file = open('times.txt', 'a+')

def mapper1(x):
    tokens=x.split(",")
    _id=int(tokens[0])
    rating=float(tokens[2])
    return [_id,(rating,1)]

def mapper2(x):
    _id=x[0]
    ratings=x[1][0]
    total=x[1][1]
    return (_id,ratings/total)

def mapper3(x):
    tokens=x.split(",")
    _id=int(tokens[0])
    return (_id,0)

spark = SparkSession.builder.appName("query1-rdd").getOrCreate()
sc = spark.sparkContext

rdd = sc.textFile('hdfs://master:9000/movie_data/ratings.csv')
start_time = time.time()
rdd_some = rdd.map(mapper1).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).map(mapper2).filter(lambda x: x[1]>3.0).map(lambda x: (1,1)).reduceByKey(lambda x,y: x+y)

rdd_total = rdd.map(mapper3).reduceByKey(lambda x,y: 0).map(lambda x: (1,1)).reduceByKey(lambda x,y: x+y)
end_time = time.time()
file.write(str((end_time-start_time)/60)+'\n')
some=rdd_some.collect()[0][1]
total=rdd_total.collect()[0][1]
print("Some users:"+str(some))
print("Total users:"+str(total))
print("Percentage:"+str(some/total))
print(rdd.take(10))
file.close()
