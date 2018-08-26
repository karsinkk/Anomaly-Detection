
import datetime
import time

import pyspark
from pyspark.sql import *
import pyspark.sql.functions as Func
from pyspark.sql.types import *

import pyspark.mllib.clustering as clustering
from pyspark.ml.linalg import Vectors


sc = SparkContext(appName="Train-KMeans")  


#Initialize SparkSession
spark = SparkSession.builder \
    .master("spark://master:7077") \
    .appName("Spark") \
    .getOrCreate()
print(spark)

# Read Data files with a Custom Schema - CDR Data was obtained from the Open Big Data project by Dandelion,
# It is availabe at https://dandelion.eu/datamine/open-big-data/
DataSchema = StructType([StructField("square_id", IntegerType(), True), \
                    StructField("time", StringType(), True), \
                    StructField("country", IntegerType(), True), \
                    StructField("sms_in", FloatType(), True), \
                    StructField("sms_out", FloatType(), True), \
                    StructField("call_in", FloatType(), True), \
                    StructField("call_out", FloatType(), True), \
                    StructField("internet", FloatType(), True)])
df = spark.read.csv(path="hdfs://master:9000/data/sms-call-internet-mi-2013-12-03.txt",schema=DataSchema,sep='\t')

# Data preprocessing

# Fill Null Values with 0.0
df = df.fillna(0.0)

# Convert UTC timestamp to Hours
Hours = Func.udf(lambda x :datetime.datetime.utcfromtimestamp(float(x)/1000).strftime('%H'),StringType())
df = df.withColumn('time',Hours(df.time))
df = df.withColumn('time', df.time.cast(IntegerType()))

# Calculate Total Activity.
df = df.withColumn('sms_in',df.sms_in+df.sms_out+df.call_in+df.call_out+df.internet)
df = df.withColumnRenamed('sms_in','total')
df = df.drop('square_id','country','sms_out','call_out','internet','call_in')
df.take(3)

data = df.select(Func.array('total','time').alias("value")).rdd.map(lambda x: x.value)

# Build the model (cluster the data), cluster size was found by elbow method
clusters = clustering.KMeans.train(s1, 100, maxIterations=100, initializationMode="kmeans||")

# Save the  model
clusters.save(sc, "hdfs://master:9000/data/KMeansModel")

#Sum squared error of the model
WSSSE = model.computeCost(data)

#Calcuate euclidean distance between points and their closest centroids
def distance(point):
    center = clusters.centers[clusters.predict(point)]
    return Vectors.dense(point).squared_distance(center)
Distances = s1.map(lambda point: distance(point))
Distances.take(3)

#Take the largest distance as the threshold for anomaly detection
Threshold = Distances.max()