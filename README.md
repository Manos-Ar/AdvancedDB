# AdvancedDB

##Import Data To HDFS
```
wget http://www.cslab.ntua.gr/courses/atds/movie_data.tar.gz

tar -xvf movie_data.tar.gz

hadoop fs -put movie_data hdfs://master:9000/movie_data

```
