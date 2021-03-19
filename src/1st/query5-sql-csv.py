#!/bin/python3
#Kk9Q5BSLKx
from pyspark.sql import SparkSession
from io import StringIO
import csv
import sys
import time
times = open('times.txt', 'a+')


sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

spark = SparkSession.builder.appName("query5-sql").getOrCreate()

genres_csv = spark.read.csv('hdfs://master:9000/movie_data/movie_genres.csv',inferSchema='true')
movies_csv = spark.read.csv('hdfs://master:9000/movie_data/movies.csv',inferSchema='true')
ratings_csv = spark.read.csv('hdfs://master:9000/movie_data/ratings.csv',inferSchema='true')

# genres_csv.printSchema()
# movies_csv.printSchema()
# ratings_csv.printSchema()

genres_csv.registerTempTable("genres")
movies_csv.registerTempTable("movies")
ratings_csv.registerTempTable("ratings")
start_time = time.time()
join_genres_rating = spark.sql("""  select movie_id,genre,user_id,rating
                                    from
                                    (select _c0 as user_id, _c1 as movie_id, _c2 as rating
                                    from ratings)
                                    join
                                    (select _c1 as genre, _c0 as movie_id
                                    from genres)
                                    using (movie_id)""")

join_genres_rating.registerTempTable("join_genres_rating")

join_ratings_genres_movies = spark.sql( """ 
                                            select movie_id,genre,user_id,rating,popularity
                                            from
                                            join_genres_rating
                                            join
                                            (select _c0 as movie_id, _c7 as popularity
                                             from movies
                                            )
                                            using(movie_id)
                                        """)
# join_ratings_genres_movies.show()

join_ratings_genres_movies.registerTempTable("join_ratings_genres_movies")

count_users = spark.sql("""
                            select genre, user_id, count(rating) as count_u
                            from join_genres_rating
                            group by genre,user_id
                        """)

count_users.registerTempTable("count_users")

max_count = spark.sql(  """
                            select tmp.genre, user_id, tmp.max_count as count
                            from
                            (select genre, max(count_u) as max_count
                            from  count_users
                            group by genre) as tmp
                            join
                            count_users as cu
                            on tmp.genre==cu.genre and tmp.max_count==cu.count_u
                        """)

max_count = max_count.dropDuplicates(["genre"])
# max_count.show(30)

max_count.registerTempTable("max_count")

max_rating = spark.sql( """ 
                            select movie_id,genre,user_id,rating,popularity,count
                            from
                            max_count
                            join
                            join_ratings_genres_movies
                            using(genre,user_id) 
                        """)

max_rating.registerTempTable("max_rating")

max_rating_genre = spark.sql(   """
                                    select genre, max(rating) as rating
                                    from max_rating
                                    group by genre
                                """)
# max_rating_genre.show()

max_rating_genre.registerTempTable("max_rating_genre")

max_genre_movie_id_popularity = spark.sql(  """
                                            select genre, movie_id, popularity
                                            from max_rating_genre
                                            join
                                            (
                                            select genre, rating,movie_id, popularity
                                            from max_rating
                                            )
                                            using(genre,rating)
                                        """)

max_genre_movie_id_popularity.registerTempTable("max_genre_movie_id_popularity")
# max_rating_genre_movie_id.show()

max_genre_movie_id_popularity.registerTempTable("max_genre_movie_id_popularity")

max_genre_movie_id = spark.sql(   """
                                        select m.genre, movie_id 
                                        from   
                                        (select genre, max(popularity) as max_popularity
                                        from max_genre_movie_id_popularity
                                        group by genre) as t
                                        join
                                        max_genre_movie_id_popularity as m
                                        on
                                        t.genre==m.genre and t.max_popularity==m.popularity
                                    """)

max_genre_movie_id.registerTempTable("max_genre_movie_id")
# max_genre_movie_id.show()

max_rating_new = spark.sql( """
                            select genre,user_id,movie_id,rating,count
                            from
                            max_genre_movie_id
                            join
                            max_rating
                            using(genre,movie_id)
                        """)


max_rating_new.registerTempTable("max_rating_new")

max_rating_title = spark.sql(   """
                                    select genre,user_id,title,rating,count
                                    from
                                    max_rating_new
                                    join
                                    (
                                    select _c0 as movie_id , _c1 as title
                                    from movies 
                                    )
                                    using(movie_id)
                                """)

max_rating_title.registerTempTable("max_rating_title")                       


min_genre_rating = spark.sql( """
                        select genre, min(rating) as min_rating
                        from max_rating
                        group by genre
                        """)

min_genre_rating.registerTempTable("min_genre_rating")

min_genre_movie_id_popularity = spark.sql( """
                                    select m.genre, movie_id, popularity
                                    from
                                    min_genre_rating as mn
                                    join
                                    max_rating as m
                                    on mn.genre==m.genre and mn.min_rating==m.rating
                                """)

# min_genre_movie_id_popularity.show()
min_genre_movie_id_popularity.registerTempTable("min_genre_movie_id_popularity")

min_genre_movie_id = spark.sql( """
                                    select m.genre, movie_id
                                    from
                                    min_genre_movie_id_popularity as m
                                    join
                                    (
                                    select genre, max(popularity) as min_popularity
                                    from min_genre_movie_id_popularity
                                    group by genre
                                    ) as mn
                                    on mn.genre==m.genre and mn.min_popularity==m.popularity
                                """)

min_genre_movie_id.registerTempTable("min_genre_movie_id")

min_genre_movie_id_rating = spark.sql(  """
                            select genre, movie_id,rating
                            from
                            min_genre_movie_id
                            join
                            (
                            select genre, movie_id, rating
                            from max_rating
                            )
                            using(genre,movie_id)
                        """)

min_genre_movie_id_rating.registerTempTable("min_genre_movie_id_rating")

min_rating_title = spark.sql(    """
                                    select genre,title, rating
                                    from
                                    min_genre_movie_id_rating
                                    join
                                    (
                                    select _c0 as movie_id , _c1 as title
                                    from movies 
                                    )
                                    using(movie_id)
                                """)

min_rating_title.registerTempTable("min_rating_title")
# min_rating_title.write.format('csv').options("delimiter", "|").save("/home/user/src/min_rating_title.csv")
# min_rating_title.show(30)
# max_rating.show(30)
# max_rating_title.show(30) 

output = spark.sql( """
                        select genre,user_id,mx.title,mx.rating,mn.title,mn.rating,mx.count
                        from
                        min_rating_title as mn
                        join
                        max_rating_title as mx
                        using(genre)
                    """)
end_time = time.time()
output.show(30)

times.write("Query5-sql-csv: "+str((end_time-start_time)/60)+'\n')
times.close()


