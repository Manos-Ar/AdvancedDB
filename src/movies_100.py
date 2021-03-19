#!/bin/python3

import pandas as pd

df = pd.read_csv('movies.csv',nrows= 99)

df.to_csv('movies_100.csv',index=False)
