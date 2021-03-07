#!/bin/python3
#Kk9Q5BSLKx
from pyspark.sql import SparkSession
from io import StringIO
import csv
import sys


sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

spark = SparkSession.builder.appName("query2-sql").getOrCreate()

def count_review_words(x):
    if x is None:
        return -1
    else:
        return len(x.split(" "))


def get_quinquennium(x):
    if x is None:
        return "x"
    else:
        start_year = int(x)

        if(start_year<2000):
            return "1995-1996"
        if(start_year>=2000 and start_year<2005):
            return "2000-2004"
        elif(start_year>=2005 and start_year<2010):
            return "2005-2009"
        elif(start_year>=2010 and start_year<2015):
            return "2010-2014"
        else:
            return "2015-2019"

spark.udf.register("countWords", count_review_words)
spark.udf.register("quinquennium",get_quinquennium)


movies_genres_csv = spark.read.csv('hdfs://master:9000/movie_data/movie_genres.csv',inferSchema='true')
movies_data_csv = spark.read.csv('hdfs://master:9000/movie_data/movies.csv',inferSchema='true')

movies_genres_csv.printSchema()
movies_data_csv.printSchema()

movies_genres_csv.registerTempTable("movies_genres_csv")
movies_data_csv.registerTempTable("movies_csv")

spark.sql(" select quinq, avg(summary_words) as average_summary\
            from(\
            select title, genre, countWords(summary) as summary_words, quinquennium(year) as quinq \
            from (\
            (select _c1 as title, _c0 as id, _c2 as summary, year(_c3) as year\
            from movies_csv\
            ) as m1\
            join (select _c1 as genre, _c0 as id\
            from movies_genres_csv ) as m2 \
            on m1.id=m2.id)\
            where genre='Drama')\
            where quinq <> 'x'\
            group by quinq").show()



movies_genres_parquet = spark.read.load('hdfs://master:9000/movie_data/movie_genres.parquet')
movies_data_parquet = spark.read.load('hdfs://master:9000/movie_data/movies.parquet')

movies_genres_parquet.registerTempTable("movies_genres_parquet")
movies_data_parquet.registerTempTable("movies_data_parquet")


spark.sql(" select quinq, avg(summary_words) as average_summary\
            from(\
            select title, genre, countWords(summary) as summary_words, quinquennium(year) as quinq \
            from (\
            (select _c1 as title, _c0 as id, _c2 as summary, year(_c3) as year\
            from movies_data_parquet\
            ) as m1\
            join (select _c1 as genre, _c0 as id\
            from movies_genres_parquet ) as m2 \
            on m1.id=m2.id)\
            where genre='Drama')\
            where quinq <> 'x'\
            group by quinq").show()
