from pyspark_cassandra import CassandraSparkContext, saveToCassandra
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.streaming import StreamingContext
#from pyspark.sql.types import StructType
from pyspark.sql.types import *
from pyspark.sql import functions as f
import time
#import redis
import config

#redis_server = "localhost"
#r = redis.StrictRedis(redis_server, port=6379, db=0)
#
#
#def write_redis(k, val):
#    """write into redis"""
#    redis_db.set(k, val)


def main():
	spark = SparkSession \
	    .builder \
	    .appName("Twitchatter") \
	    .getOrCreate()
	# Create DataFrame representing the stream of input lines from connection to localhost:9999
	# Subscribe to 1 topic
	df = spark \
	  .readStream \
	  .format("kafka") \
	  .option("kafka.bootstrap.servers", config.ip_address) \
	  .option("subscribe", config.topic) \
          .option("startingOffsets","earliest") \
	  .load()

        #Kafka streams from source are as "key":"value"..etc.
	df.printSchema()
        
        #Select key:value and discard others
        #value schema: {"username":"xxx","message":"xxx","channel":"xxx","time":"xxx"}
        schema = StructType().add("username",StringType()).add("message",StringType()).add("channel",StringType()).add("time",StringType())
        ds = df.selectExpr("CAST(value AS STRING)") \
               .select(f.from_json("value",schema).alias("message")) \
               .select("message.*")
        ds.printSchema()
        # uncomment to see data flowing in
        query = ds.writeStream.outputMode("append").format("console").start()
        query.awaitTermination()

        # write is not available for streaming data; we use create_table to create table and keyspaces
        #ds.write.format("org.apache.spark.sql.cassandra").options(table="rawtable",keyspace="test").save(mode="append")
        
        # again structured stream does not provide savetocassandra functionality
        #ds.saveToCassandra("test","rawtable")
        
        # dump data to parquet files
        #query = ds.writeStream \
        #  .format("parquet") \
        #  .option("startingOffsets", "earliest") \
        #  .option("checkpointLocation", "/home/ubuntu/twitchatter/test/check/") \
        #  .option("path", "s3a://mypqrquet/") \
        #  .start()
        #query.awaitTermination()
        	
        # Do some simple count: unique user count per channel
        user_count = ds.groupBy("channel","username").count()
        #user_count.write.format("org.apache.spark.sql.cassandra").options(table="protable",keyspace="test").save(mode="append")
        #user_count.saveToCassandra("test","protable")

        # write to cassandra 
        # https://docs.datastax.com/en/dse/6.0/dse-dev/datastax_enterprise/spark/structuredStreaming.html
        #query = user_count.writeStream\
        # .option("checkpointLocation", '/home/ubuntu/twitchatter/test/')\
        # .format("org.apache.spark.sql.cassandra")\
        # .option("keyspace", "analytics")\
        # .option("table", "test")\
        # .start()


if __name__ == '__main__':
    main()

