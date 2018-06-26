/usr/local/spark/bin/spark-submit --master spark://ec2-18-205-221-147.compute-1.amazonaws.com:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0,anguenot/pyspark-cassandra:0.9.0 --py-files /home/ubuntu/pyspark-cassandra-0.9.0.zip --conf spark.cassandra.connection.host=18.204.49.203 streaming.py

