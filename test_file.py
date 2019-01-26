from pyspark import SparkConf,SparkContext
conf = SparkConf().setAppName('test spark')
sc = SparkContext(conf = conf)

for i in range(10,0,-1):
    print ('hello everyone this is a test program')
    print('count down {}'.format(i))
