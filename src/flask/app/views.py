# jsonify creates a json representation of the response
from flask import jsonify
from flask_cors import CORS, cross_origin
import json
from app import app
from flask import render_template, request
import config
from datetime import datetime,timedelta

CORS(app)
# importing Cassandra modules from the driver we just installed
from cassandra.cluster import Cluster
from cassandra.policies import WhiteListRoundRobinPolicy


@app.route('/')
@app.route('/index')
def index():
   user = { 'nickname': 'twitch-emotes' } # fake user
   return render_template("index.html",title = 'Home', user = user)


lbp = WhiteListRoundRobinPolicy(config.cass_whitelist)
# Setting up connections to cassandra
cluster = Cluster([config.cass_seedip],load_balancing_policy=lbp)
session = cluster.connect(config.cass_keyspace)

@app.route('/channel')
def get_channel():
   ts = (datetime.utcnow()-timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
   #print ts
   stmt = "SELECT channel,SUM(global_emotes) AS globl, SUM(subscriber_emotes) AS sub, SUM(total_emotes) as total FROM "\
           +config.cass_keyspace+".channel_count_time where timestamp > '"+ts+"' GROUP BY channel ALLOW FILTERING;"

   # for debugging
   # stmt = "select channel,sum(global_emotes) as globl,sum(subscriber_emotes) as sub, sum(total_emotes) as total \
   #         from emotes.channel_count_time where timestamp > '2018-06-27 12:20:00' group by channel allow filtering;"

   response = session.execute(stmt)
   response_list = []
   for val in response:
        response_list.append(val)
   #print response_list
   jsonresponse = [{"channel":x.channel, "global_emotes":x.globl,"subscriber_emotes":x.sub,"total_emotes":x.total} for x in response_list]
   jsonresponse.sort(key=lambda x: -x['total_emotes'])
   #print jsonresponse
   
   #return top 10 of jsonify(channel=jsonresponse)
   return render_template('index.html',channel=jsonresponse[:10])


@app.route('/emote')
def emote():
 return render_template("emote.html")

@app.route("/emote", methods=['POST'])
def emote_post():
 channel_name = request.form["channel_name"]
 print type(channel_name),channel_name
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

 return render_template("emoteop.html", free=jsonresponse1[:10],nonfree=jsonresponse2[:10])



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
