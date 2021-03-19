#!/bin/python3

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("parquet_example").getOrCreate()
df = spark.read.csv('hdfs://master:9000/movie_data/movies.csv', header = False)
df.write.parquet('hdfs://master:9000/movie_data/movies.parquet')


df = spark.read.csv('hdfs://master:9000/movie_data/movie_genres.csv', header = False)
df.write.parquet('hdfs://master:9000/movie_data/movie_genres.parquet')

df = spark.read.csv('hdfs://master:9000/movie_data/ratings.csv', header = False)
df.write.parquet('hdfs://master:9000/movie_data/ratings.parquet')






