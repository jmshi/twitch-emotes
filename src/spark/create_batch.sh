#!/bin/bash
$SPARK_HOME/bin/spark-submit  --master spark://ec2-18-205-221-147.compute-1.amazonaws.com:7077 --packages com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.2,anguenot/pyspark-cassandra:0.9.0 --py-files /home/ubuntu/pyspark-cassandra-0.9.0.zip --conf spark.cassandra.connection.host=52.2.196.10  batch_job.py
