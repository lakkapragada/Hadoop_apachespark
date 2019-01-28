from pyspark import SparkConf,SparkContext
conf = SparkConf().setAppName('test spark')
sc = SparkContext(conf = conf)

def convert(x):
    return [int(x[0]),int(x[1]),float(x[2]),int(x[4])]


data = sc.textFile('hdfs:///user/maria_dev/ml-100k/u.data')

out = data.map(lambda x:x.split())
out.map(convert)
output = out.collect()
print(output)
