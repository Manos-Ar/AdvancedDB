#!/bin/python3
#Kk9Q5BSLKx
from pyspark.sql import SparkSession
from io import StringIO
import csv
import sys
import re
import time

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
times = open('times.txt', 'a+')
def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

def get_quinquennium(x):
    if x=='':
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

def mapper1(x):
    #tokens=x.split(",")
    # _id=int(tokens[0])
    # title = tokens[1]
    # summary_len=len(tokens[2].split(" "))
    _id = int(x[0])
    #title = x[1]
    summary_len = len(x[2].split(" "))
    year=x[3].split("-")[0]
    # year = year.replace(" ","")
    # year = ''.join(year.split())
    # year = int(year)
    year = re.sub(r"\s+", "", year, flags=re.UNICODE)
    # if year == '':
    #     year = 0
    # year = int(year)
    quinq = get_quinquennium(year)
    # return  (_id,(summary_len,quinq))
    return  (_id,(quinq,summary_len,1))

def mapper2(x):
    tokens=x.split(",")
    _id=int(tokens[0])
    genre = tokens[1]

    return (_id,genre)

def filter1(x):
    genre = x[1]
    if genre=='Drama':
        return True
    return False

spark = SparkSession.builder.appName("query4-rdd").getOrCreate()
sc = spark.sparkContext


movies = sc.textFile('hdfs://master:9000/movie_data/movies.csv')
genres = sc.textFile('hdfs://master:9000/movie_data/movie_genres.csv')
start_time = time.time()
movies_t = movies.map(split_complex).map(mapper1)
genres_t = genres.map(mapper2).filter(filter1)

#(16, (('2000-2004', 73, 1), 'Drama'))
#(id, (dekaetia,word_count,1), genre)
joined = movies_t.join(genres_t)

#meta to map (dekaetia,word_count_ana_tainia,1)
#meta to reduce (dekaetia,(sinoliko_word_count,sinolo_tainiwn))
output = joined.map(lambda x: (x[1][0][0],(x[1][0][1],x[1][0][2]))).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).map(lambda x: (x[0],x[1][0]/x[1][1]))
end_time = time.time()

times.write("Query4-rdd: "+str((end_time-start_time)/60)+'\n')

#joined.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]])).map(lambda x: x[0]/x[1])
# print(joined2.take(10))
output_file = open("4_rdd.txt", "w+")
output_file.write("Year_Range\tMean\n")

output_list = output.collect()

for line in output_list:
    for l in line:
        output_file.write("%s\t" %l)
    output_file.write("\n")

output_file.close()
times.close()
