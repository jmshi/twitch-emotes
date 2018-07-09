import dash
from dash.dependencies import Output, Input, Event
import dash_core_components as dcc
import dash_html_components as html
import plotly
import random
import plotly.graph_objs as go
from collections import deque
from datetime import datetime,timedelta
import numpy as np
import config
# importing Cassandra modules from the driver we just installed
from cassandra.cluster import Cluster
from cassandra.policies import WhiteListRoundRobinPolicy


lbp = WhiteListRoundRobinPolicy(config.cass_whitelist)
# Setting up connections to cassandra
cluster = Cluster([config.cass_seedip],load_balancing_policy=lbp)
session = cluster.connect(config.cass_keyspace)


app = dash.Dash(__name__)

def get_top_channel():
   print "call this func"
   ts = (datetime.utcnow()-timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
   stmt = "SELECT channel,SUM(global_emotes) AS globl, SUM(subscriber_emotes) AS sub, SUM(total_emotes) as total FROM "\
           +config.cass_keyspace+".channel_count_time where timestamp > '"+ts+"' GROUP BY channel ALLOW FILTERING;"
   response = session.execute(stmt)
   response_list = []
   for val in response:
        response_list.append(val)
   jsonresponse = [{"channel":x.channel[1:], "global_emotes":x.globl,"subscriber_emotes":x.sub,"total_emotes":x.total} for x in response_list]
   jsonresponse.sort(key=lambda x: -x['total_emotes'])
   #return top 10 of jsonify(channel=jsonresponse)
   return [item["channel"] for item in jsonresponse[:10]]

def serve_layout():
  layout = html.Div([
           html.Div([
           dcc.Dropdown(id='yaxis-column',
               options=[{'label': i, 'value': i} for i in get_top_channel()],
               value='blank'
               ),
           dcc.Dropdown(id='xaxis-column',
               options=[{'label':'1 min','value': 1},
                        {'label':'10 min','value': 10},
                        {'label':'1 hour','value': 60}],
               value=1
               )
           ],style={'width': '30%', 'display': 'inline-block'}),
 
           html.Div([
           dcc.Graph(id='live-graph', animate=True),
           dcc.Interval(
               id='graph-update',
               interval=5*1000
           ),])
           ])
  return layout

# this is crucial serve_layout instead of serve_layout()
app.layout = serve_layout


def get_data(channel_name='ninja',toffset_min=10):
   #ts = (datetime.utcnow()-timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
   ts = (datetime.utcnow()-timedelta(minutes=toffset_min)).strftime("%Y-%m-%d %H:%M:%S")
   stmt = "SELECT timestamp AS time,global_emotes AS free, subscriber_emotes AS paid, total_emotes as total FROM "\
           +config.cass_keyspace+".channel_count_time where channel='#"+channel_name+"' and timestamp > '"+ts+"' ALLOW FILTERING;"

   response = session.execute(stmt)
   response_list = []
   for val in response:
        response_list.append(val)
   result = [(x.time,x.free,x.paid,x.total) for x in response_list]
   
   return result 

@app.callback(Output('live-graph', 'figure'),
              [Input('yaxis-column','value'),Input('xaxis-column','value')],
              events=[Event('graph-update', 'interval')])
def update_graph_scatter(yaxis_column_name,xaxis_column_name):
    result = get_data(yaxis_column_name,xaxis_column_name)
    X = np.array([item[0] for item in result])
    Y = np.array([item[1] for item in result])
    Z = np.array([item[2] for item in result])
    #print X
    #print Y
    #print Z
    trace1 = plotly.graph_objs.Scatter(
             x=list(X),
             y=list(Y),
             name='free',
             mode= 'lines+markers'
             )
    trace2 = plotly.graph_objs.Scatter(
             x=list(X),
             y=list(Z),
             name='paid',
             mode= 'lines+markers'
             )
    data = [trace1,trace2]
    max_y = max([max(Y),max(Z)])
    min_y = min([min(Y),min(Z)])
    return {'data': [trace1,trace2],'layout' : go.Layout(title='Emotes count in channel '+yaxis_column_name+' (every 10 seconds)',xaxis=dict(title='time(UTC)',range=[min(X),max(X)]),
                                              yaxis=dict(range=[min_y,max_y]),)}
    #return {'data': data,'layout' : go.Layout(xaxis=dict(range=[min(X),max(X)]),
    #                                          yaxis=dict(range=[min_y,max_y]),
    #                                          yaxis2=dict(range=[min_y,max_y]),)}
          #       layout = {'height': 620, 'yaxis': {'title': "Average Proximity (m)", 'side': "left"},
          #                             'yaxis2': {'title': 'Within 10 Meters (count)', 'side': "right", 'overlaying': "y"}}
    #return Figure(data=data, layout=layout)




if __name__ == '__main__':
    app.run_server(debug=True,host='0.0.0.0')
