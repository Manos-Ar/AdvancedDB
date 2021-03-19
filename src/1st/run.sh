#!/bin/bash

if [ -d "outputs" ]; then
    rm -rf "outputs"
    mkdir "outputs"
else
    mkdir "outputs"
fi

# spark-submit /home/user/src/query1-rdd.py
# spark-submit /home/user/src/query1-sql.py
# spark-submit /home/user/src/query2-rdd.py
# spark-submit /home/user/src/query2-sql.py
# spark-submit /home/user/src/query3-rdd.py
# spark-submit /home/user/src/query3-sql.py
# spark-submit /home/user/src/query4-rdd.py
# spark-submit /home/user/src/query4-sql.py
# spark-submit /home/user/src/query5-rdd.py
# spark-submit /home/user/src/query5-sql-csv.py
# spark-submit /home/user/src/query5-sql-par.py

mv *.txt outputs

# python3 plots.py
