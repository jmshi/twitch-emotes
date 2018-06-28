import threading
from twitch import TwitchClient
import config

def get_top_videos(offset=0,limit=100, game=None, period='all',broadcast_type=''):
    client = TwitchClient(client_id=config.client_id)
    vid  = client.videos.get_top(offset=offset,limit=limit,game=game,period=period,broadcast_type=broadcast_type)
    #print(vid)
    vid_list = [item['id'] for item in vid]
    return vid_list


if __name__ == "__main__":

    #form video list
    vid_list = []
    num,limit = 10,100 #targeting 10,100
    for i in range(num):
        vid_list += get_top_videos(1100+i*limit,limit)
    #print(vid_list)
    vid_file = open('vid_list_1.txt','w')
    for item in vid_list:
        vid_file.write("{}\n".format(item))

