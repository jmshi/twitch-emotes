#!/usr/bin/env python
from __future__ import print_function
from chat.chat import TwitchChatStream
import config
import datetime
import json
from kafka import KafkaProducer
import sys
import threading, time
from twitch import TwitchClient
import multiprocessing as mp


def get_active_channel(offset=0,limit=100):
    bots = TwitchClient(client_id=config.channel_client_id)
    stream  = bots.streams.get_live_streams(offset=offset,limit=limit)
    
    channel_list = [item[u'channel'][u'name'].encode('ascii','ignore').lower() for item in stream]
    return channel_list



class Producer(threading.Thread):
    def __init__(self,name):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.channel = name 

    def stop(self):
        self.stop_event.set()

    def run(self):
        self.producer = KafkaProducer(bootstrap_servers=config.ip_address)
        chatstream = TwitchChatStream(username=self.channel,
                                      oauth=config.chat_token,
                                      verbose=False)
    
        # Continuously check if messages are received (every ~10s)
        # This is necessary, if not, the chat stream will close itself
        # after a couple of minutes (due to ping messages from twitch)
        while True:
            received = chatstream.twitch_receive_messages()
            if received:
                #print("producer: {}{}".format(self.channel,received[0]))
                self.producer.send(config.topic, json.dumps(received[0]).encode('utf-8'))
        self.producer.close()


def listen_to_channel(channel):
    prod = Producer(channel)
    prod.run()
    return

if __name__ == "__main__":

    #get 1000 live channels
    channel_list = []
    num,limit = 5,100 #targeting 10,100
    for i in range(num):
        channel_list += get_active_channel(i*limit,limit)
    print(channel_list)
    
    #  create thread for each channel..
    for item in channel_list:
        thread = threading.Thread(target=listen_to_channel,args=(item,))
        thread.start()



    

  
