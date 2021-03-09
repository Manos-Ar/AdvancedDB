#!/bin/python3
#Kk9Q5BSLKx
from pyspark.sql import SparkSession
from io import StringIO
import csv
import sys


sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

spark = SparkSession.builder.appName("query5-sql").getOrCreate()

movies_genres_csv = spark.read.csv('hdfs://master:9000/movie_data/movie_genres.csv',inferSchema='true')
movies_data_csv = spark.read.csv('hdfs://master:9000/movie_data/movies.csv',inferSchema='true')
ratings_csv = spark.read.csv('hdfs://master:9000/movie_data/ratings.csv',inferSchema='true')

movies_genres_csv.printSchema()
movies_data_csv.printSchema()
ratings_csv.printSchema()

movies_genres_csv.registerTempTable("movies_genres_csv")
movies_data_csv.registerTempTable("movies_csv")
ratings_csv.registerTempTable("ratings_csv")

temp1 = spark.sql("select user_id,rating,movie_id,genre\
                    from\
                    (select _c0 as user_id, _c1 as movie_id, _c2 as rating\
                    from ratings_csv)\
                    join\
                    (select _c1 as genre, _c0 as movie_id\
                    from movies_genres_csv)\
                    using (movie_id)")

temp1.registerTempTable("join_genres_rating")

temp = spark.sql("  select genre,user_id,max(rating) as max_rating,min(rating) as min_rating, count(rating) as count\
                    from join_genres_rating\
                    group by genre,user_id")

temp.registerTempTable("temp")

temp2 = spark.sql(" select genre,max(count) as max_count\
                    from temp\
                    group by genre")

temp2.registerTempTable("temp2")

temp3 = spark.sql(" select temp.genre as genre, user_id, max_rating, min_rating, max_count\
                    from temp\
                    join\
                    temp2\
                   on temp.genre=temp2.genre and temp.count=temp2.max_count")

temp3.registerTempTable("temp3")

temp4 = spark.sql(" select *\
                    from\
                    (select _c0 as movie_id, _c1 as title\
                    from\
                    movies_csv)\
                    join\
                    join_genres_rating\
                    using(movie_id)")

temp4.registerTempTable("join_genres_rating_movies")

tmp5 = spark.sql("  select ")

# spark.sql("select *\
#         from temp").show()

# spark.sql(" select *\
#             from\
#             (select genre,  max(count) as max_count\
#             from\
#             (select genre,user_id,max(rating) as max_rating,min(rating) as min_rating,count(rating) as count\
#             from\
#             (select user_id,rating,movie_id,genre\
#             from\
#             (select _c0 as user_id, _c1 as movie_id, _c2 as rating\
#             from ratings_csv)\
#             join\
#             (select _c1 as genre, _c0 as movie_id\
#             from movies_genres_csv)\
#             using (movie_id))\
#             join\
#             (select genre,user_id, max(max_rating), min(min_rating), max(count) as max_count\
#             from\
#             (select genre,user_id,max(rating) as max_rating,min(rating) as min_rating,count(rating) as count\
#             from\
#             (select user_id,rating,movie_id,genre\
#             from\
#             (select _c0 as user_id, _c1 as movie_id, _c2 as rating\
#             from ratings_csv)\
#             join\
#             (select _c1 as genre, _c0 as movie_id\
#             from movies_genres_csv)\
#             using (movie_id))\
#             group by genre,user_id)\
#             group by genre,user_id)\
#             using (genre,max_count)) ").show()

# spark.sql(" select genre,user_id, max(max_rating), min(min_rating), max(count) as max_count\
#             from\
#             (select genre,user_id,max(rating) as max_rating,min(rating) as min_rating,count(rating) as count\
#             from\
#             (select user_id,rating,movie_id,genre\
#             from\
#             (select _c0 as user_id, _c1 as movie_id, _c2 as rating\
#             from ratings_csv)\
#             join\
#             (select _c1 as genre, _c0 as movie_id\
#             from movies_genres_csv)\
#             using (movie_id))\
#             group by genre,user_id)\
#             group by genre,user_id\
#             ").show()

# join
# (select _c0 as movie_id, _c1 as title
# from movies_csv)
# using (movie_id)
