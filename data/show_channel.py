from twitch import TwitchClient
import config

client = TwitchClient(client_id=config.channel_client_id)
#channel = client.channels.get_by_id(44322889)
#print(channel.id)
#print(channel.name)
#print(channel.display_name)


#all_emoticons = client.chat.get_all_emoticons()
#set_emoticons = client.chat.get_emoticons_by_set()
#print(set_emoticons)

# test the functionality of twitch api-5.0
for i in range(1):
  stream  = client.streams.get_live_streams(offset=i*100,limit=100)
  print(stream)
  #active_channel_list = [item[u'channel'][u'display_name'].encode('ascii','replace').lower() for item in stream]
  #active_channel_list = [item[u'channel'][u'display_name'].lower() for item in stream]
  active_channel_list = [item[u'channel'][u'name'].lower() for item in stream]
  print(active_channel_list)
  active_channel_list = [item[u'channel'][u'name'].decode('utf-8').lower() for item in stream]
  print(active_channel_list)
print(client.streams.get_summary())

