from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

sc = SparkContext('local[*]','spark_application')
spark = SparkSession(sc)

data = spark.read.csv('hdfs:///user/maria_dev/Hadoop_apachespark/telecom_churn.csv',header=True,inferSchema=True)

indexer = StringIndexer(inputCol = 'International plan'
                        ,outputCol = 'encoded_International plan').fit(data)
encoded_data = indexer.transform(data)

indexer_1 = StringIndexer(inputCol = 'Voice mail plan'
                        ,outputCol = 'encoded_Voice mail plan').fit(encoded_data)

encoded_data = indexer_1.transform(encoded_data)

final_data = encoded_data.withColumn('encoded_Churn',encoded_data['Churn'].cast('int'))

feature_columns = ['Account length','Area code','Number vmail messages',
 'Total day minutes',
 'Total day calls',
 'Total day charge',
 'Total eve minutes',
 'Total eve calls',
 'Total eve charge',
 'Total night minutes',
 'Total night calls',
 'Total night charge',
 'Total intl minutes',
 'Total intl calls',
 'Total intl charge',
 'Customer service calls','encoded_International plan',
 'encoded_Voice mail plan']

matrix = VectorAssembler(inputCols=feature_columns,outputCol='features')
final_matrix = matrix.transform(final_data)

learning_data = final_matrix.select('features','encoded_Churn')
train,test = learning_data.randomSplit([0.7,0.3])
model = LogisticRegression(labelCol='encoded_Churn')
model = model.fit(train)
summary = model.summary
predictions=model.evaluate(test)
print('The accuracy is :',predictions.accuracy)


