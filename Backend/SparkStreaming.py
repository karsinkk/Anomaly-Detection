import numpy as np
import datetime
import json

import pyspark
from pyspark.sql import *
import pyspark.sql.functions as Func
from pyspark.sql.types import *

from pyspark import SparkContext  

from pyspark.streaming import StreamingContext  
from pyspark.streaming.kafka import KafkaUtils  

import pyspark.mllib.clustering as clustering
from pyspark.mllib.linalg import Vectors

from kafka import KafkaProducer



sc = SparkContext(appName="PythonSparkStreamingKafka")  
sc.setLogLevel("WARN") 

spark = SparkSession \
    .builder \
    .appName("SparkSession") \
    .getOrCreate()

ssc = StreamingContext(sc, 5)  
print(sc)


model = clustering.KMeansModel.load(sc,"hdfs://localhost:9000/data/KMeansModel")

producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def distance(point):
    center = model.centers[model.predict(point)]
    return Vectors.dense(point).squared_distance(center)

def PreProcessData(rdd):
	rdd = rdd.map(lambda line : [x if x != ''else '0' for x in line ])
	rdd = rdd.map(lambda line : [float(x) for x in line])
	df = spark.createDataFrame(data=rdd,schema=DataSchema)
	return df.rdd

def ComputeDistances(rdd):
	df = spark.createDataFrame(data=rdd,schema=DataSchema)

	# Convert UTC timestamp to Hours
	data = df
	Hours = Func.udf(lambda x :datetime.datetime.utcfromtimestamp(float(x)/1000).strftime('%H'),StringType())
	data = data.withColumn('time',Hours(data.time))
	data = data.withColumn('time', data.time.cast(IntegerType()))
	# Calculate Activity.
	data = data.withColumn('sms_in',data.sms_in+data.sms_out+data.call_in+data.call_out+data.internet)
	data = data.withColumnRenamed('sms_in','total')
	data = data.drop('square_id','country','sms_out','call_out','internet','call_in')
	data = data.select(Func.array('total','time').alias("value")).rdd.map(lambda x: x.value)

	Distances = data.map(lambda point: distance(point))
	print("distances--- ",Distances.take(3))
	df = df.rdd.map(lambda x :[y for y in x])
	Result = Distances.zip(df)
	print("Result---",Result.take(2))
	return Distances.zipWithIndex().map(lambda e: (e[1], [e[0]]))


def SendResult(records):
	print(type(records))
	producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))
	for record in records:
		record = record[1]
		d0 = record[0][0]
		d1 = record[1][0].asDict()
		d1['error']=d0
		print(type(d1))
		print(d1)
		producer.send('result', d1)
	producer.flush()
	producer.close()	



# Read Data files with a Custom Schema - CDR Data was obtained from the Open Big Data project by Dandelion,
# It is availabe at https://dandelion.eu/datamine/open-big-data/
DataSchema = StructType([StructField("square_id", FloatType(), True), \
                    StructField("time", StringType(), True), \
                    StructField("country", FloatType(), True), \
                    StructField("sms_in", FloatType(), True), \
                    StructField("sms_out", FloatType(), True), \
                    StructField("call_in", FloatType(), True), \
                    StructField("call_out", FloatType(), True), \
                    StructField("internet", FloatType(), True)])


kvs = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'realtime':1})  
lines = kvs.map(lambda x: x[1])
rows = lines.map(lambda line:line.split("\n")[0])
rows = rows.map(lambda line: line.split("\t"))
Data  = rows.transform(lambda rdd : PreProcessData(rdd))
Result = Data.transform(lambda rdd: ComputeDistances(rdd))
Data = Data.transform(lambda rdd : rdd.zipWithIndex().map(lambda e: (e[1], [e[0]])))

Data.pprint()
Result.pprint()
Final = Result.join(Data)
Final.pprint()
Final.foreachRDD(lambda rdd: rdd.foreachPartition(SendResult))


ssc.start()  
ssc.awaitTermination() 
