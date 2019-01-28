# This program is to test the top movies accoriding ratings by users in pyspark frame work
# a public data set was taken and stored in hadoop distribution 

from pyspark import SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setAppName('test spark')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)


# take input data from hdfs file system
data = spark.textFile('hdfs:///user/maria_dev/ml-100k/u.data')

# perform some transformations on RDD 
out = data.map(lambda x:x.split()).map(lambda x:[int(x[0]),int(x[1]),float(x[2]),int(x[4])])

# convert RDD to data frame to assign column names 

spark_df = out.toDF(['id','movie_ID','rating','time_stamp'])

# movie names list is stored in another meta data file

items = spark.textFile('hdfs:///user/maria_dev/ml-100k/u.item')
movie_items = items.map(lambda x:x.split('|')).map(lambda x:[int(x[0]),str(x[1])])
movie_df = movie_items.toDF(['movie_ID','movie_name'])
# average ratings of each movie
moviewise_avg_rating = spark_df.groupBy('movie_ID').avg('rating')
# total count of each movie
movie_count = spark_df.groupBy('movie_ID').count()
final = moviewise_avg_rating.join(movie_count,'movie_ID')

print(final.orderBy('avg(rating)').take(10))


