#!/bin/bash

spark-submit /home/user/src/2nd/repartition.py
spark-submit /home/user/src/2nd/repartition_100.py
spark-submit /home/user/src/2nd/broadcast.py
spark-submit /home/user/src/2nd/broadcast_100.py
spark-submit /home/user/src/2nd/sql.py Y
spark-submit /home/user/src/2nd/sql.py N

mv *.txt /home/user/src/outputs