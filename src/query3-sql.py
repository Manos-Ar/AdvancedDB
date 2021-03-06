#!/bin/python3

from pyspark.sql import SparkSession
from io import StringIO
import csv
spark = SparkSession.builder.appName("query3-sql").getOrCreate()

ratings_csv = spark.read.csv('hdfs://master:9000/movie_data/ratings.csv',inferSchema='true')
movies_genres_csv = spark.read.csv('hdfs://master:9000/movie_data/movie_genres.csv',inferSchema='true')

ratings_csv.registerTempTable("ratings_csv")
movies_genres_csv.registerTempTable("movies_genres_csv")

ratings_csv.printSchema()
movies_genres_csv.printSchema()

spark.sql(" select genres, count(distinct id) as total, avg(rating) as mean_rating\
             from \
            (select _c0 as id, _c1 as genres \
            from movies_genres_csv) \
            join \
            (select _c1 as id, _c2 as rating \
            from ratings_csv) \
            using(id)\
            group by genres").show()

ratings_parquet = spark.read.load('hdfs://master:9000/movie_data/ratings.parquet')
movies_genres_parquet = spark.read.load('hdfs://master:9000/movie_data/movie_genres.parquet')

ratings_csv.registerTempTable("ratings_parquet")
movies_genres_csv.registerTempTable("movies_genres_parquet")


spark.sql(" select genres, count(distinct id) as total, avg(rating) as mean_rating\
             from \
            (select _c0 as id, _c1 as genres \
            from movies_genres_parquet) \
            join \
            (select _c1 as id, _c2 as rating \
            from ratings_parquet) \
            using(id)\
            group by genres").show()
