#!/usr/bin/python
from __future__ import print_function
from chat.chat import TwitchChatStream
import argparse
import time
import numpy as np
from kafka import KafkaProducer, KafkaClient
#from kafka import SimpleProducer as KafkaProducer, KafkaClient
import sys
import random
import datetime
import json

class Producer(object):

    def __init__(self, addr):
        #cluster = KafkaClient(addr)
        #self.producer = KafkaProducer(cluster,async=False)
        self.producer = KafkaProducer(bootstrap_servers=addr)

    def produce_msgs(self, key, channel,oauth):
        with TwitchChatStream(username=channel,
                              oauth=oauth,
                              verbose=True) as chatstream:
    
            # Send a message to this twitch stream
            #chatstream.send_chat_message("I'm reading this!")
    
            # Continuously check if messages are received (every ~10s)
            # This is necessary, if not, the chat stream will close itself
            # after a couple of minutes (due to ping messages from twitch)
            while True:
                received = chatstream.twitch_receive_messages()
                if received:
                    print("producer: {}".format(received[0]))
                    self.producer.send('my_topic', json.dumps(received[0]).encode('utf-8'))
                #time.sleep(1)
            self.producer.close()


if __name__ == "__main__":
    args = sys.argv
    if len(args) < 5:
      print("please specify ip_addr,key,channel,oauth")
      exit()

    ip_addr = str(args[1])
    partition_key = str(args[2])
    channel = str(args[3])
    oauth = str(args[4])

    prod = Producer(ip_addr)
    prod.produce_msgs(partition_key,channel,oauth) 
