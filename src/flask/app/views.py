# jsonify creates a json representation of the response
from flask import jsonify
from flask_cors import CORS, cross_origin
import json
from app import app
from flask import render_template, request
from datetime import datetime,timedelta
import sqlite3
import config
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdate
import io,base64

CORS(app)
# importing Cassandra modules from the driver we just installed
from cassandra.cluster import Cluster
from cassandra.policies import WhiteListRoundRobinPolicy


lbp = WhiteListRoundRobinPolicy(config.cass_whitelist)
# Setting up connections to cassandra
cluster = Cluster([config.cass_seedip],load_balancing_policy=lbp)
session = cluster.connect(config.cass_keyspace)

def merge_two_dicts(x, y):
    z = x.copy()   # start with x's keys and values
    z.update(y)    # modifies z with y's keys and values & returns None
    return z

def load_json(fname='./emote_set/global_code_id.json'):
    with open(fname,'r') as f:
        data = json.load(f)
    f.close
    return data

global_dict = load_json(config.data_dir+'/reduced_global_code_id.json')
sub_dict = load_json(config.data_dir+'/reduced_sub_code_id.json')
#global_dict = load_json(config.data_dir+'/emotes_set/global_code_id.json')
#sub_dict = load_json(config.data_dir+'/emotes_set/subscriber_code_id.json')
footy_dict = load_json(config.data_dir+'/world_cup.json')


@app.route('/index')
def index():
   user = { 'nickname': 'twitch-emotes' } # fake user
   return render_template("index.html",title = 'Home', user = user)

@app.route('/')
@app.route('/home')
def home():
   return render_template("home.html")

@app.route('/channel')
def get_channel():
   ts = (datetime.utcnow()-timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
   stmt = "SELECT channel,SUM(global_emotes) AS globl, SUM(subscriber_emotes) AS sub, SUM(total_emotes) as total FROM "\
           +config.cass_keyspace+".channel_count_time where timestamp > '"+ts+"' GROUP BY channel ALLOW FILTERING;"

   # for debugging
   # stmt = "select channel,sum(global_emotes) as globl,sum(subscriber_emotes) as sub, sum(total_emotes) as total \
   #         from emotes.channel_count_time where timestamp > '2018-06-27 12:20:00' group by channel allow filtering;"

   response = session.execute(stmt)
   response_list = []
   for val in response:
        response_list.append(val)
   jsonresponse = [{"channel":x.channel[1:], "global_emotes":x.globl,"subscriber_emotes":x.sub,"total_emotes":x.total} for x in response_list]
   jsonresponse.sort(key=lambda x: -x['total_emotes'])
   
   #return top 10 of jsonify(channel=jsonresponse)
   return render_template('channel.html',channel=jsonresponse[:10])


@app.route("/emote/<channel_name>")
def get_emotes(channel_name):
 ts = (datetime.utcnow()-timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
 # get the free emotes
 stmt = "SELECT emote_name,SUM(count) AS cnt FROM "\
           +config.cass_keyspace+".time_channel_emotes_count where timestamp > '"\
           +ts+"' and channel='#{}' and is_free=TRUE GROUP BY emote_name ALLOW FILTERING;"
 response = session.execute(stmt.format(channel_name))
 response_list = []
 for val in response:
    response_list.append(val)
 jsonresponse1 = [{"emote":x.emote_name,"count":x.cnt} for x in response_list]
 jsonresponse1.sort(key=lambda x: -x['count'])
 for item in jsonresponse1[:10]:
     item["id"] = global_dict[item["emote"]]
 free = jsonresponse1[:10]


 # get the non-free emotes
 stmt = "SELECT emote_name,SUM(count) AS cnt FROM "\
           +config.cass_keyspace+".time_channel_emotes_count where timestamp > '"\
           +ts+"' and channel='#{}' and is_free=FALSE GROUP BY emote_name ALLOW FILTERING;"
 response = session.execute(stmt.format(channel_name))
 response_list = []
 for val in response:
    response_list.append(val)
 jsonresponse2 = [{"emote":x.emote_name,"count":x.cnt} for x in response_list]
 jsonresponse2.sort(key=lambda x: -x['count'])
 for item in jsonresponse2[:10]:
     item["id"] = sub_dict[item["emote"]]
 nonfree = jsonresponse2[:10]
 
 return render_template("emoteop.html", channel_name=channel_name,free=free,nonfree=nonfree)

@app.route('/footy')
def get_footy():
    # get the top n footies
    stmt = "SELECT emote_name,SUM(count) AS cnt FROM "\
           +config.cass_keyspace+".time_footy_count GROUP BY emote_name;"
    response = session.execute(stmt)
    response_list = []
    for val in response:
       response_list.append(val)
    jsonresponse = [{"emote":x.emote_name,"count":x.cnt} for x in response_list]
    jsonresponse.sort(key=lambda x: -x['count'])
    footy_list = jsonresponse[:10]
    for item in footy_list:
        item["count"] =int(5.1*item["count"])
        if item["emote"] in footy_dict:
          item["id"] = footy_dict[item["emote"]]
        else:
          item["id"] = "1077156" # didnt find huh?
    return render_template("footy.html",footy=footy_list)


@app.route('/footy/fig')
def build_plot():
    img = io.BytesIO()

    # get the top n footies
    stmt = "SELECT emote_name,SUM(count) AS cnt FROM "\
           +config.cass_keyspace+".time_footy_count GROUP BY emote_name;"
    response = session.execute(stmt)
    response_list = []
    for val in response:
       response_list.append(val)
    jsonresponse = [{"emote":x.emote_name,"count":x.cnt} for x in response_list]
    jsonresponse.sort(key=lambda x: -x['count'])

    top_n = 6
    top_list = [item["emote"] for item in jsonresponse[:top_n]]
    top_list1 = ["'"+item["emote"]+"'" for item in jsonresponse[:top_n]]
    #print top_list
    #print top_list1
    # get the time evolution of top footies
    stmt = "SELECT emote_name,timestamp,count FROM "\
              +config.cass_keyspace+".time_footy_count where emote_name IN ("\
              +", ".join(top_list1)+");"
    response = session.execute(stmt)
    response_list = []
    for val in response:
       response_list.append(val)
    jsonresponse = [{"emote":x.emote_name,"time":x.timestamp,"count":x.count} for x in response_list]

    epoch=datetime(1970,1,1)
    def time_to_int(time):
        return mdate.epoch2num((datetime.strptime(time, "%Y-%m-%d %H:%M:%S")-epoch).total_seconds())

    t0,v0 = [],[]
    t1,v1 = [],[]
    t2,v2 = [],[]
    t3,v3 = [],[]
    t4,v4 = [],[]
    t5,v5 = [],[]
    for item in jsonresponse:
        if item["emote"]==top_list[0]:
           t0.append(time_to_int(item["time"]))
           v0.append(item["count"])
        if item["emote"]==top_list[1]:
           t1.append(time_to_int(item["time"]))
           v1.append(item["count"])
        if item["emote"]==top_list[2]:
           t2.append(time_to_int(item["time"]))
           v2.append(item["count"])
        if item["emote"]==top_list[3]:
           t3.append(time_to_int(item["time"]))
           v3.append(item["count"])
        if item["emote"]==top_list[4]:
           t4.append(time_to_int(item["time"]))
           v4.append(item["count"])
        if item["emote"]==top_list[5]:
           t5.append(time_to_int(item["time"]))
           v5.append(item["count"])
    
    #do running sum
    fig,ax = plt.subplots()
    for i in range(top_n):
        if i==0:
          x,y=np.array(t0),np.array(v0)
        if i==1: 
          x,y=np.array(t1),np.array(v1)
        if i==2:
          x,y=np.array(t2),np.array(v2)
        if i==3:
          x,y=np.array(t3),np.array(v3)
        if i==4:
          x,y=np.array(t4),np.array(v4)
        if i==5:
          x,y=np.array(t5),np.array(v5)

        
        ax.plot_date(x,(5.1*np.cumsum(y)).astype(int),'-',label=top_list[i])

    # Choose your xtick format string
    date_fmt = '%Y-%m-%d %H:%M:%S'

    # Use a DateFormatter to set the data to the correct format.
    date_formatter = mdate.DateFormatter(date_fmt)
    ax.xaxis.set_major_formatter(date_formatter)
    
    # Sets the tick labels diagonal so they fit easier.
    fig.autofmt_xdate()

    ax.set_ylabel("Running Sum")
    plt.legend(loc=2)

    plt.savefig(img, format='png')
    img.seek(0)

    #return send_file(img,mimetype='image/png')
    plot_url = base64.b64encode(img.getvalue()).decode()
    return '<img src="data:image/png;base64,{}">'.format(plot_url)



# ============== test playgroud below ====================
#@app.route('/emote')
#def emote():
# return render_template("emote.html")


#@app.route('/data.json')
#def get_channel_json():
#   ts = (datetime.utcnow()-timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S")
#   #print ts
#   stmt = "SELECT timestamp,global_emotes, subscriber_emotes, total_emotes FROM "\
#           +config.cass_keyspace+".channel_count_time where channel='#ninja' ALLOW FILTERING;"
#           #+config.cass_keyspace+".channel_count_time where channel='#ninja' and timestamp > '"+ts+"' GROUP BY channel ALLOW FILTERING;"
#
#   # for debugging
#   # stmt = "select channel,sum(global_emotes) as globl,sum(subscriber_emotes) as sub, sum(total_emotes) as total \
#   #         from emotes.channel_count_time where timestamp > '2018-06-27 12:20:00' group by channel allow filtering;"
#
#   response = session.execute(stmt)
#   response_list = []
#   for val in response:
#        response_list.append(val)
#   #print response_list
#   #jsonresponse = [{"time":x.timestamp, "global_emotes":x.global_emotes,"subscriber_emotes":x.subscriber_emotes,"total_emotes":x.total_emotes} for x in response_list]
#   epoch=datetime(1970,1,1)
#   jsonresponse = [[(datetime.strptime(x.timestamp, "%Y-%m-%d %H:%M:%S")-epoch).total_seconds()*1000, x.global_emotes] for x in response_list]
#   
#   return jsonify(jsonresponse)
#   #return json.dumps(jsonresponse)

#@app.route('/_add_numbers')
#def add_numbers():
#    a = request.args.get('a', 0, type=int)
#    b = request.args.get('b', 0, type=int)
#    print a,b
#    result = a+b
#    print result
#    return jsonify(result=result)

#@app.route("/data.json")
#def data():
#    connection = sqlite3.connect("db.sqlite")
#    cursor = connection.cursor()
#    print type(cursor)
#    cursor.execute(".tables")
#    cursor.execute("SELECT 1000*timestamp, measure from measures")
#    results = cursor.fetchall()
#    print results
#    return json.dumps(results)
 
#@app.route("/graph")
#def graph():
#    return render_template('graph.html')
 
 

#@app.route('/emotes/<channel_name>')
#def get_emotes(channel_name):
#   print type(channel_name)
#   ts = (datetime.utcnow()-timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
#   stmt = "SELECT emote_name,SUM(count) AS cnt FROM "\
#           +config.cass_keyspace+".time_channel_emotes_count where timestamp > '"+ts+"' and channel='#"+channel_name+"' and is_free=TRUE GROUP BY emote_name ALLOW FILTERING;"
#
#   print stmt
#   response = session.execute(stmt)
#   response_list = []
#   for val in response:
#        response_list.append(val)
#   print response_list
#   jsonresponse1 = [{"emote":x.emote_name, "count":x.cnt} for x in response_list]
#   jsonresponse1.sort(key=lambda x: -x['count'])
#   
#   stmt = "SELECT emote_name,SUM(count) AS cnt FROM "\
#           +config.cass_keyspace+".time_channel_emotes_count where timestamp > '"+ts+"' and channel='#"+channel_name+"' and is_free=FALSE GROUP BY emote_name ALLOW FILTERING;"
#
#   response = session.execute(stmt)
#   response_list = []
#   for val in response:
#        response_list.append(val)
#   jsonresponse2 = [{"emote":x.emote_name,"count":x.cnt} for x in response_list]
#   jsonresponse2.sort(key=lambda x: -x['count'])
#   #return top 10 of jsonify(channel=jsonresponse)
#   return render_template('emote.html',free=jsonresponse1[:10],nonfree=jsonresponse2[:10])
