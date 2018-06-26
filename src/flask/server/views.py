# jsonify creates a json representation of the response
from flask import jsonify
from flask_cors import CORS, cross_origin
import json
from server import app
from flask import render_template
#import config

#from app import app
#CORS(app)
# importing Cassandra modules from the driver we just installed
#from cassandra.cluster import Cluster
#from cassandra.policies import WhiteListRoundRobinPolicy


@app.route('/')
@app.route('/index')
def index():
   user = { 'nickname': 'twitch-emotes' } # fake user
   return render_template("index.html",title = 'Home', user = user)


#lbp = WhiteListRoundRobinPolicy(config['CASSANDRA_WHITELIST'])
## Setting up connections to cassandra
#
## Change the bolded text to your seed node public dns (no < or > symbols but keep quotations. Be careful to copy quotations as it might copy it as a special character and throw an error. Just delete the quotations and type them in and it should be fine. Also delete this comment line
#cluster = Cluster([config['CASSANDRA_DNS']],load_balancing_policy=lbp)
#
## Change the bolded text to the keyspace which has the table you want to query. Same as above for < or > and quotations. Also delete this comment line
#session = cluster.connect('twitchly')
#
#redisConnection = redis.StrictRedis(host=config['REDIS_IP'], port=6379, db=0)


#@app.route('/api/<channel>/<date>')
#def get_channel_data(channel, date):
#       stmt = "SELECT * FROM streamStatus WHERE channel = %s AND date = %s"
#       response = session.execute(stmt, parameters=[channel, date])
#       response_list = []
#       for val in response:
#            response_list.append(val)
#       jsonresponse = [{"date":x.date, "count": x.count, "time": x.time, "viewers":x.viewers, "messageLength":x.messagelength} for x in response_list]
#       return jsonify(channelName=channel,channelViews=jsonresponse)

