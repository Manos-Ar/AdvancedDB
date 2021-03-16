#!/bin/python3

from pyspark.sql import SparkSession
from io import StringIO
import csv
import sys
import time
times = open('times.txt', 'w+')
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

def user_movie_ids(x):
    tokens = x.split(",")
    movie_id = int(tokens[1])
    user_id = int(tokens[0])
    return (movie_id,user_id)    

def rating_map(x):
    tokens = x.split(",")
    movie_id = int(tokens[1])
    user_id = int(tokens[0])
    rating = float(tokens[2])
    return (movie_id, (user_id, rating))


def genres_map(x):
    tokens = x.split(",")
    movie_id = int(tokens[0])
    genre = tokens[1]
    return (movie_id, genre)


def movies_map(x):
    movie_id = int(x[0])
    # title = tokens[1]
    popularity = float(x[7])
    return (movie_id,popularity)


spark = SparkSession.builder.appName("query5-rdd").getOrCreate()
sc = spark.sparkContext

rating = sc.textFile('hdfs://master:9000/movie_data/ratings.csv')
genres = sc.textFile('hdfs://master:9000/movie_data/movie_genres.csv')
movies = sc.textFile('hdfs://master:9000/movie_data/movies.csv')
start_time = time.time()

rating_user_movie_ids = rating.map(user_movie_ids)

# rating = rating.map(rating_map)
genres = genres.map(genres_map)
# _movies = movies.map(split_complex)
# movies_popularity  = _movies.map(movies_map)
# movies_title  = _movies.map(lambda x: (int(x[0]),x[1]))





# join_ratings_genres = genres.join(rating).map(lambda x : (x[0],(x[1][0],x[1][1][0],x[1][1][1])))
join_ratings_genres = genres.join(rating_user_movie_ids)
print(join_ratings_genres.first())
user_count = join_ratings_genres.map(lambda x : (x[1],1)).reduceByKey(lambda x,y: x+y).map(lambda x: (x[0][0],(x[0][1],x[1]))).reduceByKey(lambda x,y: max((x, y), key=lambda x: x[1])).distinct()
print(user_count.collect())




# print(join_ratings_genres.first())
# # (movie_id,(genre, user_id,rating))

# movies_popularity  = _movies.map(movies_map)
# join_ratings_genres_movies = join_ratings_genres.join(movies_popularity).map(lambda x: ((x[1][0][0],x[1][0][1]),(x[0],x[1][0][2],x[1][1])))

# print(join_ratings_genres_movies.first())
# # # ((genre,user_i),(movie_id,rating,popularity))

# count_users = join_ratings_genres_movies.map(lambda x: (x[0],1)).reduceByKey(lambda x,y : x+y).map(lambda x: (x[0][0],(x[0][1],x[1]))).reduceByKey(lambda x,y: max((x, y), key=lambda x: x[1])).distinct().map(lambda x: ((x[0],x[1][0]),x[1][1]))
# # ((genre,user_id),count)


# join_ratings_genres_movies_count = join_ratings_genres_movies.join(count_users).map(lambda x: (x[0][0],(x[0][1],x[1][0][0],x[1][0][1],x[1][0][2],x[1][1])))
# print(join_ratings_genres_movies_count.first())
# # (genre,(user_id,movie_id,rating,popularity,count))


# max_rating = join_ratings_genres_movies_count.reduceByKey(lambda x,y: max((x, y), key=lambda x: x[2])).reduceByKey(lambda x,y: max((x, y), key=lambda x: x[2]))
# min_rating = join_ratings_genres_movies_count.reduceByKey(lambda x,y: min((x, y), key=lambda x: x[2])).reduceByKey(lambda x,y: max((x, y), key=lambda x: x[2])) 
# # (genre,(user_id,movie_id,rating,popularity,count))


# movies_title  = _movies.map(lambda x: (int(x[0]),x[1]))
# max_rating_title = max_rating.map(lambda x: (x[1][1],(x[0],x[1][0],x[1][2],x[1][4]))).join(movies_title).map(lambda x: (x[1][0][0],(x[1][0][1],x[1][1],x[1][0][2],x[1][0][3])))
# min_rating_title = min_rating.map(lambda x: (x[1][1],(x[0],x[1][2]))).join(movies_title).map(lambda x: (x[1][0][0],(x[1][1],x[1][0][1])))
# # (genre,(user_id,title,rating,count))
# # (genre,(title,rating))

# output = max_rating_title.join(min_rating_title).map(lambda x: (x[0],x[1][0][0],x[1][0][1],x[1][0][2],x[1][1][0],x[1][1][1],x[1][0][3]))

# # print(max_rating_title.collect())
# # print(min_rating_title.collect())
# end_time = time.time()
# times.write("Query5-rdd: "+str((end_time-start_time)/60)+'\n')
# output_list = output.collect()

# output_file = open("5_rdd.txt", "w+")

# output_file.write("Genre\tUser\tTitle_Max\tRating_max\tTitle_Min\tRating_Min\tCount\n")

# for line in output_list:
#     for l in line:
#         output_file.write("%s\t" %l)
#     output_file.write("\n")

# output_file.close()
# times.close()

