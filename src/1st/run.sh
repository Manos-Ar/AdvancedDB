#!/bin/bash

if [ -d "outputs" ]; then
    rm -rf "outputs"
    mkdir "outputs"
else
    mkdir "outputs"
fi

spark-submit /home/user/src/1st/query1-rdd.py
spark-submit /home/user/src/1st/query1-sql.py
spark-submit /home/user/src/1st/query2-rdd.py
spark-submit /home/user/src/1st/query2-sql.py
spark-submit /home/user/src/1st/query3-rdd.py
spark-submit /home/user/src/1st/query3-sql.py
spark-submit /home/user/src/1st/query4-rdd.py
spark-submit /home/user/src/1st/query4-sql.py
spark-submit /home/user/src/1st/query5-rdd.py
spark-submit /home/user/src/1st/query5-sql-csv.py
spark-submit /home/user/src/1st/query5-sql-par.py

mv *.txt outputs

# python3 plots.py
