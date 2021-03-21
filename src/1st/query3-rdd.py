#!/bin/python3
from pyspark.sql import SparkSession
from io import StringIO
import csv
import time
import sys

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
times = open('times2.txt', 'a+')

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

spark = SparkSession.builder.appName("query3-rdd").getOrCreate()
sc = spark.sparkContext

start_time = time.time()

rating = sc.textFile('hdfs://master:9000/movie_data/ratings.csv')
genres = sc.textFile('hdfs://master:9000/movie_data/movie_genres.csv')
rating_t = rating.map(mapper1)
genres_t = genres.map(mapper2)

join_genres_rating = genres_t.join(rating_t)
# print(join_genres_rating.first())
# (id,(genre,rating))



mean_rating = join_genres_rating.map(lambda x: [x[1][0],[x[1][1],1]]).reduceByKey(lambda x,y: [x[0]+y[0],x[1]+y[1]]).map(lambda x: (x[0],x[1][0]/x[1][1]))
# print(mean_rating.first())


distinct_genres_movies_count = join_genres_rating.map(lambda x: (x[1][0],x[0])).distinct().map(lambda x: (x[0],1)).reduceByKey(lambda x,y: x+y)
# print(distinct_genres_movies_count.first())

output = mean_rating.join(distinct_genres_movies_count).map(lambda x: (x[0],x[1][0],x[1][1]))
end_time = time.time()

times.write("Query3-rdd: "+str((end_time-start_time)/60)+'\n')

output_file = open("3_rdd.txt", "w+")
output_file.write("Genre\tMean\tTotal\n")

output_list = output.collect()

for line in output_list:
    for l in line:
        output_file.write("%s\t" %l)
    output_file.write("\n")

output_file.close()
times.close()