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
import config

class Producer(object):

    def __init__(self, addr):
        #cluster = KafkaClient(addr)
        #self.producer = KafkaProducer(cluster,async=False)
        self.producer = KafkaProducer(bootstrap_servers=addr)

    def produce_msgs(self):
            print(config.chat_token)
            chatstream1 = TwitchChatStream(username='tsm_hamlinz',oauth=config.chat_token,verbose=True)
            chatstream2 = TwitchChatStream(username='summit1g',oauth=config.chat_token,verbose=True)
            # Send a message to this twitch stream
            chatstream1.send_chat_message("I'm reading this!")
    
            # Continuously check if messages are received (every ~10s)
            # This is necessary, if not, the chat stream will close itself
            # after a couple of minutes (due to ping messages from twitch)
            while True:
              received = chatstream1.twitch_receive_messages()
              if received:
                print("producer: {}".format(received[0]))
                self.producer.send('topic', json.dumps(received[0]).encode('utf-8'))

              received = chatstream2.twitch_receive_messages()
              if received:
                print("producer: {}".format(received[0]))
                self.producer.send('topic', json.dumps(received[0]).encode('utf-8'))
              #  #time.sleep(1)
            self.producer.close()


if __name__ == "__main__":

    ip_addr = config.ip_address

    prod = Producer(ip_addr)
    prod.produce_msgs() 
