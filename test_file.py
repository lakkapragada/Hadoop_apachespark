from pyspark import SparkConf,SparkContext
conf = SparkConf().setAppName('test spark')
sc = SparkContext(conf = conf)

data = sc.textFile('hdfs:///user/maria_dev/u.data')

splitted = data.map(lambda x: x.split())

elements = splitted.collect()

print(elements)
