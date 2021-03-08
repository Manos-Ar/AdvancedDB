#!/bin/python3
from pyspark.sql import SparkSession
from io import StringIO
import csv

import sys

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

def mapper1(x):
    tokens=x.split(",")
    _id=int(tokens[1])
    rating=float(tokens[2])
    return(_id,rating)

def mapper2(x):
    tokens=x.split(",")
    _id=int(tokens[0])
    genre=tokens[1]
    return (_id,genre)

def filter1(x):
    genre = x[1][0]
    if genre=="Drama":
        return True
    return False

def map_distinct(x):
    tokens=x.split(",")
    # _id=int(tokens[0])
    genre=tokens[1]
    return (genre)

spark = SparkSession.builder.appName("query3-rdd").getOrCreate()
sc = spark.sparkContext

rating = sc.textFile('hdfs://master:9000/movie_data/ratings.csv')
genres = sc.textFile('hdfs://master:9000/movie_data/movie_genres.csv')

rating_t = rating.map(mapper1)
genres_t = genres.map(mapper2)

# distinct = genres.map(map_distinct).countByValue()


joined = genres_t.join(rating_t)
distinct = joined.map(lambda x: (x[0],x[1][0])).distinct()
# map(lambda x: x[1][0]).countByValue()
print(distinct)
# print(joined.take(30))
# print(joined.count())

# output = joined.map(lambda x: (x[1][0],x[0],x[1][1]))
# .groupByKey().map()
# .reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))
# .map(lambda x: (x[0],x[1][0],x[1][1]/x[1][0]))
# .reduceByKey(lambda x,y : x)

# print(output.take(25))
