#from pyspark_cassandra 
#from pyspark_cassandra import streaming
#from cassandra.cluster import Cluster

#from pyspark import SparkContext
#from pyspark.streaming import StreamingContext
#from pyspark.streaming.kafka import KafkaUtils

#import json
#import time
import redis
import config

#def track_trends(rdditer):
#        redis_session = redis.Redis(host=config.redis_address, port=config.redis_port, db=config.dbcount)
#        with redis_session.pipeline() as pipe:
#             for data in rdditer:
#                 pipe.zincrby("trends", str(data[2]), 1)
#                 pipe.execute()


if __name__ == '__main__':
    r = redis.StrictRedis(host=config.redis_address, port=config.redis_port, db=config.redis_dbcount)
    print(r.set('foo', 'bar'))
    print(r.get('foo'))
    #main()

