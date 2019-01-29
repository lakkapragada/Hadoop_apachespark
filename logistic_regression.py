from pyspark import SparkContext
from pyspark.sql import SparkSession


sc = SparkContext('local[*]','spark_application')
spark = SparkSession(sc)

data = spark.read.csv('hdfs:///maria_dev/users/Downloads/telecom_churn.csv',header=True,inferSchema=True)
