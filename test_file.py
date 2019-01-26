from pyspark import SparkConf,SparkContext
conf = SparkConf().setAppName('test spark')
sc = SparkContext(conf = conf)

for i in sc.parallelize(xrange(0,10,1),9).glom().collect():
    print ('hello everyone this is a test program')
    print('count down {}'.format(i))
