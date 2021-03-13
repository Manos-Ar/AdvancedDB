#!/bin/bash
#PROGRAMS = (query1-rdd.py)
rm -rf times.txt

#for program in ${PROGRAMS[@]}; do
#  spark-submit /home/user/src/${program}
spark-submit /home/user/src/query1-rdd.py
spark-submit /home/user/src/query1-sql.py
spark-submit /home/user/src/query2-rdd.py
spark-submit /home/user/src/query2-sql.py
spark-submit /home/user/src/query3-rdd.py
spark-submit /home/user/src/query3-sql.py
spark-submit /home/user/src/query4-rdd.py
spark-submit /home/user/src/query4-sql.py
spark-submit /home/user/src/query5-rdd.py
spark-submit /home/user/src/query5-sql.py
spark-submit /home/user/src/query5-sql-par.py
