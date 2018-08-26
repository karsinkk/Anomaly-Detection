import pyspark
from pyspark.sql import *
import pyspark.sql.functions as Func
from pyspark.sql.types import *

import pyspark.mllib.clustering as clustering
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest


import datetime
from numpy import array
import numpy as np
from math import sqrt
import time

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
df = spark.read.csv(path="hdfs://master:9000/data/sms-call-internet-mi-2013-12-01.txt",schema=DataSchema,sep='\t')

# Data preprocessing
# Fill Null Values with 0.0
df = df.fillna(0.0)

# Convert UTC timestamp to Hours
Hours = Func.udf(lambda x :datetime.datetime.utcfromtimestamp(float(x)/1000).strftime('%H'),StringType())
df = df.withColumn('time',Hours(df.time))
df = df.withColumn('time', df.time.cast(IntegerType()))

# Calculate Activity.
df = df.withColumn('sms_in',df.sms_in+df.sms_out+df.call_in+df.call_out+df.internet)
df = df.withColumnRenamed('sms_in','total')
df = df.drop('square_id','country','sms_out','call_out','internet','call_in')
df.take(3)

print(df.first())
s1 = df.select(Func.array('total','time').alias("value"))
s2 = s1.rdd.map(lambda x: x.value)

# Unisampling the training data 
def unisample(df, fraction=1.0):
    columns = df.first()
    new_df = None
    for i in range(0, len(columns)):
        column = df.sample(withReplacement=True, fraction=fraction) \
            .map(lambda row: row[i]) \
            .zipWithIndex() \
            .map(lambda e: (e[1], [e[0]]))
        if new_df is None:
            new_df = column
        else:
            new_df = new_df.join(column)
            new_df = new_df.map(lambda e: (e[0], e[1][0] + e[1][1]))
    return new_df.map(lambda e: e[1])

# Building the model
def supervised2unsupervised(model):
    def run(df, *args, **kwargs):
        unisampled_df = unisample(df)
        labeled_data = df.map(lambda e: LabeledPoint(1, e))\
            .union(unisampled_df.map(lambda e: LabeledPoint(0, e)))
        return model(labeled_data, *args, **kwargs)
    return run


t0 = time.time()
# Training the classifier
unsupervised_forest = supervised2unsupervised(RandomForest.trainClassifier)
rf_model = unsupervised_forest(s2, numClasses=2, categoricalFeaturesInfo={},
                  numTrees=50, featureSubsetStrategy="auto",
                  impurity='gini', maxDepth=30, maxBins=10000)
t1 = time.time()
total_n = t1-t0
print(total_n)

#Testing the classifier
result = rf_model.predict(s2)
result.count()
#Display the number of Anomalies 
result.countByValue().items()