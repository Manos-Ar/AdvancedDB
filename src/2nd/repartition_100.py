#!/bin/python3

from pyspark.sql import SparkSession
from io import StringIO
from itertools import product
import csv
import sys
import time
times = open('times_2nd.txt', 'a+')
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

def map_genre(x):
    tokens=x.split(",")
    _id=int(tokens[0])
    genre=tokens[1]
    return (_id,("g",genre))

def map_rating(x):
    tokens = x.split(",")
    movie_id = int(tokens[1])
    user_id = int(tokens[0])
    rating = float(tokens[2])
    time = tokens[3]
    return (movie_id,("r",(user_id,rating,time)))

def map_list(x):
    movie_id = x[0]
    tag = x[1][0]
    value = x[1][1]
    if tag=="r":
        return (movie_id,([value],[])) 
    else :
        return (movie_id,([],[value]))

# (movie_id,(rating,movie))

def reducer(x,y):
    listx_m = x[1]
    listx_r = x[0]
    listy_m = y[1]
    listy_r = y[0]

    list_movies = listx_m + listy_m
    list_ratings = listx_r + listy_r

    return (list_ratings,list_movies)

# (list_ratings,list_movies)

def map_output(x):
    movie_id = x[0]
    list_ratings = x[1][0]
    list_movies = x[1][1]

    if list_ratings==[] or list_movies==[]:
        return []
    else:
        return ((movie_id, i, j) for i, j in product(list_ratings, list_movies))


spark = SparkSession.builder.appName("repartition-join").getOrCreate()
sc = spark.sparkContext
start_time = time.time()
genres = sc.textFile('hdfs://master:9000/movie_data/movie_genres_100.csv')
rating = sc.textFile('hdfs://master:9000/movie_data/ratings.csv')

movies = sc.parallelize(genres.map(map_genre).collect())

rating = rating.map(map_rating)

output = rating.union(movies).map(map_list).reduceByKey(reducer).flatMap(map_output)
output_list = output.collect()

end_time = time.time()
times.write("Repartition-100: "+str(end_time-start_time)+'\n')

print(output_list)

times.close()

# output_file = open("Repartition.txt", "w+")

# output_file.write("Movie_id\tRating\tMovies\n")

# for line in output_list:
#     for l in line:
#         output_file.write("%s\t" %l)
#     output_file.write("\n")

# output_file.close()
