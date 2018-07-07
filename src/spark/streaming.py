import pyspark_cassandra
from pyspark_cassandra import streaming
from cassandra.cluster import Cluster

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.kafka import TopicAndPartition

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
    with open(config.data_dir+'global_emotes_count.json','r') as f:
    #with open(config.data_dir+'/emotes_set/global_code_id.json','r') as f:
        data = json.load(f)
    f.close
    return data

def load_subemotes():
    with open(config.data_dir+'sub_emotes_count.json','r') as f:
    #with open(config.data_dir+'/emotes_set/subscriber_code_id.json','r') as f:
        data = json.load(f)
    f.close
    return data


def main():
    sc = SparkContext(appName="Twitchatter")
    sc.setLogLevel('ERROR')
    # broadcast the emotes set
    global_emotes = sc.broadcast(load_emotes())
    #print(global_emotes.value.keys())
    sub_emotes = sc.broadcast(load_subemotes())
    #print(sub_emotes.value.keys()[:10])

    batch_duration = 6
    ssc = StreamingContext(sc, batch_duration) # every 3 seconds per batch
    # set checkpoint directory:use default fs protocol in core-site.xml
    ssc.checkpoint("hdfs://"+config.spark_ckpt)

    zkQuorum = [config.zk_address]
    topic = [config.topic]
    print("{}{}".format(zkQuorum,topic))
    
    partition = 0
    start = 0
    topicpartition = TopicAndPartition(topic[0],partition)
    
    kvs = KafkaUtils.createDirectStream(ssc,topic,{"metadata.broker.list": config.ip_address})
    # uncomment the following if running sum
    #kvs = KafkaUtils.createDirectStream(ssc,topic,{"metadata.broker.list": config.ip_address},
    #        fromOffsets={topicpartition: int(start)})
    #kvs.checkpoint(600)
    
    parsed = kvs.map(lambda v: json.loads(v[1]))
    
    window_duration,sliding_duration = 12,12
    # (1) total count of emotes for given channel
    def get_emotes_count(x):
        line = x.split(" ")
        words = [item.encode('utf-8') for item in line]
        emotes = [item for item in words if item in global_emotes.value]
        #emotes = [item for item in words if item in global_emotes.value.keys()]
        return dict(Counter(emotes))
    
    def sum_dict(x,y):
        return {k: x.get(k, 0) + y.get(k, 0) for k in set(x) | set(y)}
    def sub_dict(x,y):
        return {k: x.get(k, 0) - y.get(k, 0) for k in set(x) | set(y)}

    def get_count(x):
        line = x.split(" ")
        words = [item.encode('utf-8') for item in line]
        #emotes = [item for item in words if item in global_emotes.value.keys()]
        #subemotes = [item for item in words if item in sub_emotes.value.keys()]
        emotes = [item for item in words if item in global_emotes.value]
        subemotes = [item for item in words if item in sub_emotes.value]
        return [len(emotes),len(subemotes)]
    def sum_list(x,y):
        return [x[0]+y[0],x[1]+y[1]]
    def sub_list(x,y):
        return [x[0]-y[0],x[1]-y[1]]

    channel_count_time = parsed.map(lambda v: (v[u'channel'],v[u'message']))\
                               .mapValues(get_count)\
                               .reduceByKeyAndWindow(sum_list,sub_list,window_duration,sliding_duration)\
                               .map(lambda v: {"channel":v[0],\
                                               "global_emotes":v[1][0],\
                                               "subscriber_emotes":v[1][1],\
                                               "total_emotes":(v[1][0]+v[1][1]),\
                                               "timestamp":datetime.datetime.now()\
                                               .strftime("%Y-%m-%d %H:%M:%S")})
    #channel_count_time.pprint()

    # 2) get individual emotes count for given channel
    channel_message = parsed.map(lambda v: [(v[u'channel'],word) \
                                 for word in v[u'message'].split(" ")])\
                                 .flatMap(lambda x: x)
    #channel_message.pprint()

    def get_global(x):
        if x[1] in global_emotes.value:
        #if x[1] in global_emotes.value.keys():
            return True
        else:
            return False
    def get_sub(x):
        if x[1] in sub_emotes.value:
        #if x[1] in sub_emotes.value.keys():
            return True
        else:
            return False
    channel_emotes = channel_message.filter(get_global)\
                                    .map(lambda v: (v[0],v[1],True))
    #channel_emotes.pprint()
    channel_subemotes = channel_message.filter(get_sub)\
                                    .map(lambda v: (v[0],v[1],False))
    #channel_subemotes.pprint()

    time_channel_emotes_count = channel_emotes.union(channel_subemotes)\
                                              .map(lambda v: ((v[0],v[1],v[2]),1))\
                                              .reduceByKeyAndWindow(lambda x,y: x+y,lambda x,y:x-y,\
                                                                    window_duration, sliding_duration)\
                                              .map(lambda v: { "timestamp":datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),\
                                                               "channel":v[0][0],\
                                                               "emote_name":v[0][1],\
                                                               "is_free":v[0][2],\
                                                               "count":v[1],\
                                                               })
    #time_channel_emotes_count.pprint()
    
    channel_count_time.saveToCassandra(config.cass_keyspace,"channel_count_time")
    time_channel_emotes_count.saveToCassandra(config.cass_keyspace,"time_channel_emotes_count")


    ssc.start()
    ssc.awaitTermination()


if __name__ == '__main__':
    # connect to cassandra cluster
    cluster = Cluster([config.cass_seedip])
    session = cluster.connect()

    # create and set cassandra keyspace to work only once. 
    session.execute("CREATE KEYSPACE IF NOT EXISTS "+ config.cass_keyspace +\
            " WITH replication = {'class':'SimpleStrategy', 'replication_factor': '3'};")
    session.set_keyspace(config.cass_keyspace)

    # create tables to insert data
    session.execute("CREATE TABLE IF NOT EXISTS channel_count_time (channel text, global_emotes int, subscriber_emotes int, total_emotes int, timestamp text, primary key(channel,timestamp));")

    session.execute("CREATE TABLE IF NOT EXISTS time_channel_emotes_count (timestamp text, channel text, emote_name text, is_free boolean, count int, primary key(emote_name,timestamp));")

    main()
