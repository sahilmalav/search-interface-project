DATABASE = 'sahildb'

from pymongo import MongoClient
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, parallel_bulk
from collections import deque
from tqdm import tqdm
import time

client = Elasticsearch()
mgclient = MongoClient()
db = mgclient[DATABASE]
col = db['sslog']

# Pull from mongo and dump into ES using bulk API
actions = []
for data in tqdm(col.find(), total=col.count()):
    data.pop('_id')
    action = {
        "_index": DATABASE,
        "_type": "sslog",
        "_source": data
    }
    actions.append(action)

    # Dump x number of objects at a time
    if len(actions) >= 100:
        deque(parallel_bulk(client, actions), maxlen=0)
        actions = []
    time.sleep(.01)