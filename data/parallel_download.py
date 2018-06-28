import threading
import app

def get_single_video(vid,form):
    app.arguments.video = vid
    app.arguments.format = form
    app.download(app.arguments.video,app.arguments.format)
    return

if __name__ == "__main__":

    #read in the vid list
    f = open('vid_list.txt','r')
    vid_list = f.readlines()
    #print(vid_list[0:3])
    vid_list = [item[1:-1] for item in vid_list]
    for vid in vid_list:
        print(vid)
        thread = threading.Thread(target=get_single_video,args=(vid,'json'))
        thread.start()



##all_emoticons = client.chat.get_all_emoticons()
##set_emoticons = client.chat.get_emoticons_by_set()
##print(set_emoticons)
#
