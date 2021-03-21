#!/bin/python3

from pyspark.sql import SparkSession
from io import StringIO
import csv
import time
import sys
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
spark = SparkSession.builder.appName("query1-sql").getOrCreate()
times = open('times2.txt', 'a+')

start_time = time.time()

df_csv = spark.read.csv('hdfs://master:9000/movie_data/movies.csv',inferSchema='true')

df_csv.registerTempTable("movies_csv")

df = spark.sql("select m1.year, title from \
            (select year(_c3) as year, max(((_c6-_c5)/_c5)*100) as profit from movies_csv where year(_c3) >= 2000 and (_c6 <> 0 and _c5 <> 0) group by year) as m1\
            join (select year(_c3) as year, _c1 as title, ((_c6-_c5)/_c5)*100 as profit from movies_csv) as m2\
            on m1.profit=m2.profit and m1.year=m2.year order by year")
end_time = time.time()
times.write("Query1-sql-csv: "+str((end_time-start_time)/60)+'\n')

df.show()
# df.savetxt('home/user/src/1_sql_csv.txt', sep='\t', index=False)
start_time = time.time()

df_parquet = spark.read.load('hdfs://master:9000/movie_data/movies.parquet')

df_parquet.registerTempTable("movies_parquet")
df = spark.sql("select m1.year, title from \
            (select year(_c3) as year, max(((_c6-_c5)/_c5)*100) as profit from movies_parquet where year(_c3) >= 2000 and (_c6 <> 0 and _c5 <> 0) group by year) as m1\
            join (select year(_c3) as year, _c1 as title, ((_c6-_c5)/_c5)*100 as profit from movies_parquet) as m2\
            on m1.profit=m2.profit and m1.year=m2.year order by year")
end_time = time.time()

df.show()

times.write("Query1-sql-parquet: "+str((end_time-start_time)/60)+'\n')

# df.savetxt('home/user/src/1_sql_par.txt', sep='\t', index=False)

times.close()