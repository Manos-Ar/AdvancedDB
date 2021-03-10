#!/bin/python3
#Kk9Q5BSLKx
from pyspark.sql import SparkSession
from io import StringIO
import csv
import sys


sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

spark = SparkSession.builder.appName("query5-sql").getOrCreate()

movies_genres_parquet = spark.read.load('hdfs://master:9000/movie_data/movie_genres.parquet')
movies_data_parquet = spark.read.load('hdfs://master:9000/movie_data/movies.parquet')
ratings_parquet = spark.read.load('hdfs://master:9000/movie_data/ratings.parquet')

movies_genres_parquet.printSchema()
movies_data_parquet.printSchema()
ratings_parquet.printSchema()

movies_genres_parquet.registerTempTable("movies_genres_parquet")
movies_data_parquet.registerTempTable("movies_parquet")
ratings_parquet.registerTempTable("ratings_parquet")

temp1 = spark.sql("select user_id,rating,movie_id,genre\
                    from\
                    (select _c0 as user_id, _c1 as movie_id, _c2 as rating\
                    from ratings_parquet)\
                    join\
                    (select _c1 as genre, _c0 as movie_id\
                    from movies_genres_parquet)\
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

temp3 = spark.sql(" select distinct(temp.genre) as genre, user_id, max_rating, min_rating, max_count\
                    from temp\
                    join\
                    temp2\
                   on temp.genre=temp2.genre and temp.count=temp2.max_count")

temp3.registerTempTable("temp3")

temp4 = spark.sql(" select *\
                    from\
                    (select _c0 as movie_id, _c1 as title\
                    from\
                    movies_parquet)\
                    join\
                    join_genres_rating\
                    using(movie_id)")

temp4.registerTempTable("join_genres_rating_movies")

temp5 = spark.sql(" select m.genre,m.user_id,movie_id as max_movie_id, title as max_movie_title,m.rating as max_rating, min_rating,max_count\
                    from join_genres_rating_movies as m\
                    join\
                    temp3\
                    on temp3.genre=m.genre and temp3.user_id=m.user_id and temp3.max_rating=m.rating")

temp5.registerTempTable("join_max_rating")

temp6 = spark.sql(" select m.genre,m.user_id,max_movie_id,max_movie_title,max_rating,movie_id as min_movie_id,title as min_movie_title,m.rating as min_rating,max_count\
                    from join_genres_rating_movies as m\
                    join\
                    join_max_rating as m1\
                    on m1.genre=m.genre and m1.user_id=m.user_id and m1.min_rating=m.rating")

temp6.registerTempTable("temp6")

temp7 = spark.sql(" select _c1 as movie_id, count(_c1) as movie_popularity\
                    from ratings_parquet\
                    group by _c1")

temp7.registerTempTable("popularity")

temp8 = spark.sql(" select genre, movie_id, user_id, movie_popularity as max_movie_popularity \
                    from temp6, popularity\
                    where max_movie_id=movie_id\
                    ")

temp8.registerTempTable("temp8")

temp9 = spark.sql(" select genre,user_id,max(max_movie_popularity) as max_popularity\
                    from temp8\
                    group by user_id,genre")

temp9.registerTempTable("temp9")

temp10 = spark.sql("select distinct(temp8.genre),temp8.user_id,movie_id\
                    from temp9,temp8\
                    where temp9.genre=temp8.genre and temp8.user_id=temp9.user_id and max_popularity=max_movie_popularity")

temp10.registerTempTable("max_movie_temp")


temp11 = spark.sql(" select genre, movie_id, user_id, movie_popularity as min_movie_popularity \
                    from temp6, popularity\
                    where min_movie_id=movie_id\
                    ")

temp11.registerTempTable("temp11")

temp12 = spark.sql(" select genre,user_id,max(min_movie_popularity) as max_popularity\
                    from temp11\
                    group by user_id,genre")

temp12.registerTempTable("temp12")

temp13 = spark.sql("select distinct(temp11.genre),temp11.user_id,movie_id\
                    from temp12,temp11\
                    where temp12.genre=temp11.genre and temp11.user_id=temp12.user_id and max_popularity=min_movie_popularity")

temp13.registerTempTable("min_movie_temp")

temp14 = spark.sql("select genre,user_id,max_movie_temp.movie_id as max_movie_id, min_movie_temp.movie_id as min_movie_id\
                    from \
                    max_movie_temp\
                    join\
                    min_movie_temp\
                    using (genre,user_id)")

temp14.registerTempTable("join_popularity")

final = spark.sql("select genre,user_id,max_movie_title,max_rating,min_movie_title,min_rating,max_count as number_of_reviews\
                    from\
                    temp6\
                    join\
                    join_popularity\
                    using(genre,user_id,max_movie_id,min_movie_id)\
                    order by genre ASC")
final.show(truncate = False)

# final.coalesce(1).write.format('csv').save("/home/user/src/q5_output_sql.txt",header='true')