#!/bin/python3

import pandas as pd

df = pd.read_csv('movie_genres.csv',nrows= 99)

df.to_csv('movie_genres_100.csv',index=False)
