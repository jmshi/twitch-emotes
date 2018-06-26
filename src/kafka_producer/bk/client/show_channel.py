from twitch import TwitchClient

client = TwitchClient(client_id='v11nmjkij546e998o54vamwkjknxvr')
channel = client.channels.get_by_id(44322889)
stream  = client.streams.get_live_streams(limit=100)


print(channel.id)
print(channel.name)
print(channel.display_name)

active_channel_list = [item[u'channel'][u'display_name'].encode('ascii','ignore').lower() for item in stream]
print(active_channel_list)

