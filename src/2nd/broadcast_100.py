#!/bin/python3

from pyspark.sql import SparkSession
from io import StringIO
import csv
import sys
import time
times = open('times_2nd.txt', 'a+')
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

# def split_complex(x):
#     return list(csv.reader(StringIO(x), delimiter=','))[0]

# def map_movie(x):
#     movie_id = int(x[0])
#     return (movie_id,(x[1],x[2],x[3],x[4],x[5],x[6],x[7]))

def map_genre(x):
    tokens=x.split(",")
    _id=int(tokens[0])
    genre=tokens[1]
    return (_id,genre)

def map_rating(x):
    tokens = x.split(",")
    movie_id = int(tokens[1])
    user_id = int(tokens[0])
    rating = float(tokens[2])
    time = tokens[3]
    return (movie_id,(user_id,rating,time))

def filter_keys(x):
    movie_id = x[0]
    if movie_id in br.value.keys():
        return True
    else :
        return False 


def join_broadcast(x):
    movie_id = x[0]
    return(movie_id,br.value[movie_id],x[1][0],x[1][1],x[1][2])
    # x[1],br.value[movie_id])

spark = SparkSession.builder.appName("Broadcast").getOrCreate()
sc = spark.sparkContext
start_time = time.time()

genres = sc.textFile('hdfs://master:9000/movie_data/movie_genres_100.csv')
rating = sc.textFile('hdfs://master:9000/movie_data/ratings.csv')

br = sc.broadcast(dict(genres.map(map_genre).take(100)))

# print(br.value)

rating = rating.map(map_rating)

output = rating.filter(filter_keys).map(join_broadcast)
output_list = output.collect()

end_time = time.time()
times.write("Broadcast-100: "+str(end_time-start_time)+'\n')
print(output_list)

# output_file = open("Broadcast.txt", "w+")

# output_file.write("Movie_id\tGenre\tUser_id\tRating\tTimestamp\n")

# for line in output_list:
#     for l in line:
#         output_file.write("%s\t" %l)
#     output_file.write("\n")

# output_file.close()
times.close()