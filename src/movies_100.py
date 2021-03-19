#!/bin/python3

import pandas

df = pd.read_csv('hdfs://master:9000/movie_data/movies.csv',nrows= 100)

df.to_csv('home/user/src/movies_100.csv',index=False)

