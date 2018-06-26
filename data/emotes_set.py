# use this script to genenrate {name:id} for global/subscriber emote set (id:code or code:id)

import json 
from pprint import pprint
import re


def global_set():
  with open('emotes_general.json','r') as f:
     data = json.load(f)
  f.close
  emotes = {}
  for k,v in data.iteritems():
      emotes[int(v[u'id'])]=k
  
  with open('global_id_code.json','w') as f:
      json.dump(emotes,f)
  f.close

  e2 = {}
  for k,v in emotes.iteritems():
      e2[v] = k
  with open('global_code_id.json','w') as f:
      json.dump(e2,f)
  f.close

  return


def subscriber_set():
  with open('subscriber.json','r') as f:
     data = json.load(f)
  f.close
  print len(data)
  emotes = {}
  for channel,value in data.iteritems():
    for item in value['emotes']:
      emotes[unicode(item['id'])] = item['code']

  print len(emotes)
  # it turns out the set we get include global ones
  # so we need to remove them first
  with open('global_id_code.json','r') as fglobal:
      emotes_global = json.load(fglobal)
  fglobal.close

  for key in emotes_global.keys():
      if key in emotes.keys():
         del emotes[key]
  
  rmk_list = []
  for k,v in emotes.iteritems():
      if not re.match("^[A-Za-z0-9_-]*$",v):
          rmk_list.append(k)

  for k in rmk_list:
      del emotes[k]


  with open('subscriber_id_code.json','w') as f:
      json.dump(emotes,f)
  f.close

  e2 = {}
  for k,v in emotes.iteritems():
      e2[v] = k
  with open('global_code_id.json','w') as f:
      json.dump(e2,f)
  f.close
  return

if __name__ == '__main__':
  global_set()
  subscriber_set()

