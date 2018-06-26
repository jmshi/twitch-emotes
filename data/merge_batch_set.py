
import json 
from pprint import pprint
import re
from emotes_set import load_global_set
from emotes_set import load_subscriber_set


# script to merge batch job with the whole set to get a list of
# valuale emotes (both global and subscriber's) with count >100

def read_batch_count(fname='./data/batch_emotes_count.json'):
  with open(fname,'r') as f:
     data = json.load(f)
  f.close
  return data

global_set = load_global_set()

sub_set = load_subscriber_set()

id_count = read_batch_count()

# now try to merge the set with the batch count ==> code:count pair
global_emotes_count = {}
sub_emotes_count = {}
for k,v in id_count.iteritems():
    if v > 100: # throw away the trivials 
      if k in sub_set.keys():
        #print 'sub:',k,sub_set[k],v
        sub_emotes_count[sub_set[k]]=v
      if k in global_set.keys():
        global_emotes_count[global_set[k]]=v
        #print 'global:',k,global_set[k],v


with open('./data/global_emotes_count.json','w') as f:
   json.dump(global_emotes_count,f)
f.close
with open('./data/sub_emotes_count.json','w') as f:
   data = json.dump(sub_emotes_count,f)
f.close


   





