#!/bin/python3

from pyspark.sql import SparkSession
from io import StringIO
import csv
import sys

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

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
    tokens = x.split(",")
    movie_id = int(tokens[0])
    title = tokens[1]
    return (movie_id,title)

def final_map(x):
    key = x[0]
    genre = x[0][0]
    user_id = x[0][1]
    max_title = x[1][0][1]
    max_rating_m = x[1][0][0]
    count = x[1][0][2]
    min_title = x[1][1][1]
    min_rating_m = x[1][1][0]
    return (genre,user_id,max_title,max_rating_m,min_title,min_rating_m,count)

# ((genre,user_id),rating,title,count)
# ((genre,user_id),((max_rating,title,count),(min_rating,title,count)))


spark = SparkSession.builder.appName("query5-rdd").getOrCreate()
sc = spark.sparkContext

rating = sc.textFile('hdfs://master:9000/movie_data/ratings.csv')
genres = sc.textFile('hdfs://master:9000/movie_data/movie_genres.csv')
movies = sc.textFile('hdfs://master:9000/movie_data/movies.csv')

rating = rating.map(rating_map)
genres = genres.map(genres_map)
movies = movies.map(movies_map)


join_ratings_genres = genres.join(rating).map(lambda x : (x[0],(x[1][0],x[1][1][0],x[1][1][1])))

join_ratings_genres_movies = join_ratings_genres.join(movies)

# print(join_ratings_genres_movies.first())
# (15, (('Mystery', 225, 3.0), 'Citizen Kane'))

join_ratings_genres_movies_format = join_ratings_genres_movies.map(lambda x: ((x[1][0][0],x[1][0][1]),(x[0],x[1][0][2],x[1][1])))
# ((genre,user_i),(movie_id,rating,title))

tmp_count = join_ratings_genres_movies_format.map(lambda x: (x[0],1)).reduceByKey(lambda x,y : x+y)

max_genre_user_count = tmp_count.map(lambda x: (x[0][0],(x[0][1],x[1]))).reduceByKey(lambda x,y: max((x, y), key=lambda x: x[1])).distinct().map(lambda x: ((x[0],x[1][0]),x[1][1]))

popularity = rating.map(lambda x: (x[0],1)).reduceByKey(lambda x,y : x+y)

max_rating = max_genre_user_count.join(join_ratings_genres_movies_format).map(lambda x: (x[0],(x[1][1][0],x[1][1][1],x[1][1][2],x[1][0]))).reduceByKey(lambda x,y: max((x, y), key=lambda x: x[1]))
# ((genre,user_id),(movie_id,rating,title,count))
# (('Mystery', 8659), (994, 5.0, 'Straw Dogs', 239))

max_rating_movie = max_rating.map(lambda x: (x[1][0],(x[0],x[1][1],x[1][2],x[1][3])))
max_rating_movie = max_rating_movie.join(popularity).map(lambda x: (x[1][0][0],(x[1][1],x[1][0][1],x[1][0][2],x[1][0][3]))).reduceByKey(lambda x,y: max((x, y), key=lambda x: x[0])).map(lambda x: (x[0],(x[1][1],x[1][2],x[1][2])))
# (movie_id,(((genre,user_id),rating,title,count),popularity))
# (8446, ((('Family', 45811), 5.0, 'Bugsy Malone', 198), 215))
# ((genre,user_id),rating,title,count)
print(max_rating_movie.first())

min_rating = max_genre_user_count.join(join_ratings_genres_movies_format).map(lambda x: (x[0],(x[1][1][0],x[1][1][1],x[1][1][2],x[1][0]))).reduceByKey(lambda x,y: min((x, y), key=lambda x: x[1]))
min_rating_movie = min_rating.map(lambda x: (x[1][0],(x[0],x[1][1],x[1][2],x[1][3])))
min_rating_movie = min_rating_movie.join(popularity).map(lambda x: (x[1][0][0],(x[1][1],x[1][0][1],x[1][0][2],x[1][0][3]))).reduceByKey(lambda x,y: max((x, y), key=lambda x: x[0])).map(lambda x: (x[0],(x[1][1],x[1][2],x[1][2])))

output = max_rating_movie.join(min_rating_movie)

print(output.first())
output = output.map(final_map)

# print(join_ratings_genres_movies_format.first())
# print(max_rating.first())
# print(max_rating_movie.first())

print(output.collect())

