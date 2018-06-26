#!/usr/bin/python
from __future__ import print_function
import time
import numpy as np
from kafka import KafkaConsumer, KafkaClient
import sys
import random
import datetime
import json
import producer
import config

class Consumer(object):

    def __init__(self, addr):
        self.consumer = KafkaConsumer(bootstrap_servers=addr) #,auto_offset_reset='earliest',consumer_timeout_ms=5000)
        self.consumer.subscribe([config.topic])
        print("reach consumer init")
   
    def receive_msgs(self):
        print("reach consumer recv")
        for msg in self.consumer:
            print("consumer: {}".format(msg))

        self.consumer.close()



if __name__ == "__main__":
    #args = sys.argv
    #if len(args) < 2:
    #  print("please specify ip_addr")
    #  exit()

    #ip_addr = str(args[1])
    #partition_key = str(args[2])
    #channel = str(args[3])
    #oauth = str(args[4])

    ip_addr = config.broker_address
    cons = Consumer(ip_addr)
    cons.receive_msgs()


