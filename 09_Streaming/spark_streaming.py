#!/bin/python
#
# without Kafka
#/naslx/projects/ug201/di57hah/students/software/spark-2.3.0-bin-hadoop2.7/bin/spark-submit spark_streaming.py 



import os
import sys
import time
import datetime
import logging
import numpy as np
import socket
import re
from subprocess import check_output

from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.clustering import StreamingKMeans
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType
from pyspark.ml.feature import FeatureHasher
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml import Pipeline
from pyspark.sql import functions as F
from pyspark.sql import Row, types
from pyspark.sql.types import StructType, StructField, StringType
#######################################################################################
# CONFIGURATIONS
# Get current cluster setup from work directory
STREAMING_WINDOW=60

# Initialize PySpark
SPARK_MASTER="local[1]"
#SPARK_MASTER="spark://mpp3r03c04s06.cos.lrz.de:7077"
APP_NAME = "PySpark Lecture"
os.environ["PYSPARK_PYTHON"] = "/naslx/projects/pn69si/mnmda001/software/anaconda3/bin/python"
os.environ["JAVA_HOME"] = "/lrz/sys/compilers/java/jdk1.8.0_112"
in_dir = "/naslx/projects/pn69si/mnmda001/students/data/nasa/"
#in_dir = "/naslx/projects/pn69si/mnmda001/students/mnmda001/exercise-students-2019/09_Streaming/in/"
out = "/naslx/projects/pn69si/mnmda001/students/mnmda001/exercise-students-2019/09_Streaming/out/"
in_dir
# If there is no SparkSession, create the environment
try:
    sc and spark
except NameError as e:
    import pyspark
    import pyspark.sql
    conf=pyspark.SparkConf().set("spark.cores.max", "8").set("spark.driver.memory", "16g")
    sc = pyspark.SparkContext(master=SPARK_MASTER, conf=conf)
    spark = pyspark.sql.SparkSession(sc).builder.appName(APP_NAME).getOrCreate()

print("PySpark initiated...") 

s = StructType([StructField("ip", StringType(), True),
                StructField("m1", StringType(), True),
                StructField("m2", StringType(), True),
                StructField("date", StringType(), True),
                StructField("tz", StringType(), True),
                StructField("method", StringType(), True),
                StructField("url", StringType(), True),
                StructField("code", StringType(), True),
                StructField("size", StringType(), True),
               ])

csv_sdf = spark.readStream.csv(in_dir, schema = s,sep = " ")
print(csv_sdf.isStreaming)
#a = csv_sdf.writeStream.outputMode("update").format('console').start()
csv_sdf.schema == s
stream = csv_sdf.writeStream.outputMode("update").format('console').trigger(processingTime='2 seconds').start()
r = stream.awaitTermination()