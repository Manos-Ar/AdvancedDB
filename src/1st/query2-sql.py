#!/bin/python3

from pyspark.sql import SparkSession
from io import StringIO
import csv
import time
import sys

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
times = open('times.txt', 'a+')
spark = SparkSession.builder.appName("query2-sql").getOrCreate()

df_csv = spark.read.csv('hdfs://master:9000/movie_data/ratings.csv',inferSchema='true')

df_csv.printSchema()

df_csv.registerTempTable("ratings_csv")
start_time = time.time()
spark.sql(" select count(distinct id)/(select count(distinct(_c0)) from ratings_csv) as percentage\
            from \
            (select _c0 as id, avg(_c2) as rating \
            from ratings_csv\
            group by id)\
            where rating>3.0").show()
end_time = time.time()

times.write("Query2-sql-csv: "+str((end_time-start_time)/60)+'\n')

df_parquet = spark.read.load('hdfs://master:9000/movie_data/ratings.parquet')

df_parquet.registerTempTable("ratings_parquet")
start_time = time.time()
spark.sql(" select count(distinct id)/(select count(distinct(_c0)) from ratings_parquet) as percentage\
            from \
            (select _c0 as id, avg(_c2) as rating \
            from ratings_parquet\
            group by id)\
            where rating>3.0").show()
end_time = time.time()

times.write("Query2-sql-parquet: "+str((end_time-start_time)/60)+'\n')
times.close()
