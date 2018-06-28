import json 
from pprint import pprint


def load_json(fname='./emote_set/global_code_id.json'):
    with open(fname,'r') as f:
        data = json.load(f)
    f.close
    return data

global_code_id = load_json('./emotes_set/global_code_id.json')
sub_code_id = load_json('./emotes_set/subscriber_code_id.json')
global_emotes_count = load_json('global_emotes_count.json')
sub_emotes_count = load_json('sub_emotes_count.json')


#compare them with emotes_count set
reduced_global = {k:global_code_id[k]  for k in global_emotes_count.keys() if k in global_code_id.keys()}
reduced_sub = {k:sub_code_id[k]  for k in sub_emotes_count.keys() if k in sub_code_id.keys()}

with open('reduced_global_code_id.json','w') as f:
   json.dump(reduced_global,f)
f.close

with open('reduced_sub_code_id.json','w') as f:
   json.dump(reduced_sub,f)
f.close
