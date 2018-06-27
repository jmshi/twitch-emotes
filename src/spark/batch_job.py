import pyspark_cassandra
from pyspark_cassandra import streaming
from cassandra.cluster import Cluster

from pyspark.sql import SparkSession

from collections import Counter
import json
import time
import redis
import config
import time
import datetime

def updateTotalCount(currentState,countState):
    if countState is None:
        countState = 0
    return sum(currentState,countState)

def load_emotes():
    with open(config.data_dir+'emotes_set/global_id_code.json','r') as f:
        data = json.load(f)
    f.close
    return data

def load_subemotes():
    with open(config.data_dir+'emotes_set/subscriber_id_code.json','r') as f:
        data = json.load(f)
    f.close
    return data


def main():
    ss = SparkSession\
        .builder\
        .appName("Twitchatter-batch")\
        .getOrCreate()

    # broadcast the emotes set
    global_emotes = load_emotes()
    sub_emotes = load_subemotes()
    #print(global_emotes.value.keys())
    print(sub_emotes.keys()[:10])
    
    #load json files
    data_path = "s3a://twitchstorage/ninja/"
    loaded = ss.read.json(data_path+"v273374509.json",multiLine=True)\
            .select("comments.message")
    loaded.show(n=10,False)
    #loaded.printSchema()
    
    # count for each emoticons 
    def to_tuple(x):
        if 'emoticons' in x.keys():
            return x

    emotes = loaded.map(to_tuple)
             .

    ss.stop()
    
#
#with open('ninja/v268912511.json','r') as f:
#   data = json.load(f)
#f.close
#
#emotes = {}
#for item in data['comments']:
#    message = item['message']
#    if 'emoticons' in message.keys():
#      for emote in message['emoticons']:
#          eid = emote['_id']
#          if eid not in emotes.keys():
#              emotes[eid] = 1
#          else:
#              emotes[eid] +=1
#
#print sorted(((v,k) for k,v in emotes.iteritems()), reverse=True)
#
#
#
#
#
#
#
#        batch_duration = 5
#        ssc = StreamingContext(sc, 5) # every 3 seconds per batch
#        # set checkpoint directory:use default fs protocol in core-site.xml
#        ssc.checkpoint("hdfs://"+config.spark_ckpt)
#
#        zkQuorum = [config.zk_address]
#        topic = [config.topic]
#        print("{}{}".format(zkQuorum,topic))
#       
#        partition = 0
#        start = 0
#        topicpartition = TopicAndPartition(topic[0],partition)
#        
#        #kvs = KafkaUtils.createDirectStream(ssc,topic,{"metadata.broker.list": config.ip_address},
#        #        fromOffsets={topicpartition: int(start)})
#        kvs = KafkaUtils.createDirectStream(ssc,topic,{"metadata.broker.list": config.ip_address})
#        #kvs.pprint()
#        #kvs.checkpoint(600)
#        
#        parsed = kvs.map(lambda v: json.loads(v[1]))
#	parsed.pprint()
#        
        #### (1) searching for global emotes
        ###def get_emotes_count(x):
        ###    line = x.split(" ")
        ###    words = [item.encode('utf-8') for item in line]
        ###    emotes = [item for item in words if item in global_emotes.value.keys()]
        ###    return dict(Counter(emotes))
        ###
        ###def sum_dict(x,y):
        ###    return {k: x.get(k, 0) + y.get(k, 0) for k in set(x) | set(y)}
        ###def sub_dict(x,y):
        ###    return {k: x.get(k, 0) - y.get(k, 0) for k in set(x) | set(y)}

        ###window_duration,sliding_duration = 60,20
        ###channel_emotes_count_time = parsed.map(lambda v: (v[u'channel'],v[u'message']))\
        ###                            .mapValues(get_emotes_count)\
        ###                            .reduceByKeyAndWindow(sum_dict,sub_dict,window_duration,sliding_duration)\
        ###                            .map(lambda v: {"channel":v[0],"emote_list":v[1],"timestamp":datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
        ####channel_emotes_count_time.pprint()

        #### (2) searching for subscribed emotes
        ###def get_subemotes_count(x):
        ###    line = x.split(" ")
        ###    words = [item.encode('utf-8') for item in line]
        ###    emotes = [item for item in words if item in sub_emotes.value.keys()]
        ###    return dict(Counter(emotes))
        ###
        ###channel_subemotes_count_time = parsed.map(lambda v: (v[u'channel'],v[u'message']))\
        ###                          .mapValues(get_subemotes_count)\
        ###                          .reduceByKeyAndWindow(sum_dict,sub_dict,window_duration,sliding_duration)\
        ###                          .map(lambda v: {"channel":v[0],"subemote_list":v[1],"timestamp":datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
        ###channel_subemotes_count_time.pprint()


        ###emotes_count = parsed.map(lambda v: v[u'message'])\
        ###              .flatMap(lambda msg: msg.split(" "))\
        ###              .map(lambda x: x.encode('utf-8'))\
        ###              .filter(lambda x: x in global_emotes.value.keys())
        ####              .map(lambda emote: (emote,1))\
        ####              .reduceByKey(lambda x,y: x+y)
        ###emotes_count.pprint()

        #total_msg_counts = parsed.map(lambda v: (v[u'channel'],1)).updateStateByKey(updateTotalCount)
        #print(total_msg_counts)
        #total_msg_counts.pprint()

        ##total_user_counts = parsed.map(lambda v: (v[u'username'],1)).updateStateByKey(updateTotalCount)
        ##total_user_counts.pprint()

#        # 1) dump data to redis
#        total_msg_counts.foreachRDD(storeToRedis)
#
#        #total_msg_counts.foreachRDD(lambda rdd: rdd.foreachPartition(storeToRedis))
#        #parsed.foreachRDD(lambda rdd: rdd.foreachPartition(storeToRedis))
#
        # 2) dump data to cassandra
#        time_channel_user = parsed.map(lambda v: {"timestamp":v['time'],"channel":v[u'channel'],"username":v[u'username']})

#        # connect to cassandra cluster
#        cluster = Cluster([config.cass_seedip])
#        session = cluster.connect()
#
#        # create and set cassandra keyspace to work
#        # only once. for the future, set check existence conditions 
#        session.execute("CREATE KEYSPACE IF NOT EXISTS"+ config.cass_keyspace +" WITH replication = {'class':                 'SimpleStrategy', 'replication_factor': '3'};")
#        session.set_keyspace(config.cass_keyspace)
#
#        # create tables to insert data
#        session.execute("CREATE TABLE channel_emotes_count (timestamp text, channel text, username text, primary    key(timestamp,username));")
#
#        time_channel_user.saveToCassandra(config.cass_keyspace,"time_channel_user")



        ##Kafka streams from source are as "key":"value"..etc.
	#df.printSchema()
        #
        ##Select key:value and discard others
        ##value schema: {"username":"xxx","message":"xxx","channel":"xxx","time":"xxx"}
        #schema = StructType().add("username",StringType()).add("message",StringType()).add("channel",StringType()).add("time",StringType())
        #ds = df.selectExpr("CAST(value AS STRING)") \
        #       .select(f.from_json("value",schema).alias("message")) \
        #       .select("message.*")
        #ds.printSchema()
        ## uncomment to see data flowing in
        ##query = ds.writeStream.outputMode("append").format("console").start()
        ##query.awaitTermination()

        ## write is not available for streaming data; we use create_table to create table and keyspaces
        ##ds.write.format("org.apache.spark.sql.cassandra").options(table="rawtable",keyspace="test").save(mode="append")
        #
        ## again structured stream does not provide savetocassandra functionality
        ##ds.saveToCassandra("test","rawtable")
        #
        ## dump data to parquet files
        #query = ds.writeStream \
        #  .format("parquet") \
        #  .option("startingOffsets", "earliest") \
        #  .option("checkpointLocation", "/home/ubuntu/twitchatter/test/check/") \
        #  .option("path", "/home/ubuntu/Downloads") \
        #  .start()
        #query.awaitTermination()
        #	
        ## Do some simple count: unique user count per channel
        #user_count = ds.groupBy("channel","username").count()
        ##user_count.write.format("org.apache.spark.sql.cassandra").options(table="protable",keyspace="test").save(mode="append")
        ##user_count.saveToCassandra("test","protable")

        ## write to cassandra 
        ## https://docs.datastax.com/en/dse/6.0/dse-dev/datastax_enterprise/spark/structuredStreaming.html
        ##query = user_count.writeStream\
        ## .option("checkpointLocation", '/home/ubuntu/twitchatter/test/')\
        ## .format("org.apache.spark.sql.cassandra")\
        ## .option("keyspace", "analytics")\
        ## .option("table", "test")\
        ## .start()


if __name__ == '__main__':
    #r = redis.StrictRedis(host=config.redis_address, port=config.redis_port, db=0)
    #r.set('foo', 'bar')
    #r.get('foo')
    main()

