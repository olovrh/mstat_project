 # -*- coding: utf-8 -*-

#requests to talk easily with API's
import requests

#to use strip to remove spaces in textfiles
import sys

#needed to generate uris
import time
from hashlib import sha1
import random
import string

# json dependencies
import json as simplejson

# to flatten json and export to csv
import pandas as pd

# API credentials
key = "Y0bQcAQ3W9E0iPFUQ1gGWfdZV898rtDJWhxUtdPb"
callerId = "22690student"

# Create empty list to store reqests
data_frame=[]

with open('original_data\kommuner.txt', 'r', encoding='utf-8') as f: #open file with input strings (Swedish municipalities)
    lines = iter(f)

    for stad in lines:
        offset = iter(range(0,90000,500))
#generating unique hash string for each request
        for step in offset:
            timestamp = str(int(time.time()))
            unique = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(16))
            hashstr = sha1(callerId.encode('utf-8')+timestamp.encode('utf-8')+key.encode('utf-8')+unique.encode('utf-8')).hexdigest()
            auth = {'callerId': callerId, 'time': timestamp, 'unique': unique, 'hash': hashstr}
#make request
            batch = requests.get('http://api.booli.se/sold?q='+stad+'&limit=500&offset=%d'%step, params = auth).json()
            data_frame.append(batch['sold']) # save each request to data_frame
            print(batch['count'], stad, end = '')
            if batch['count'] == 0:
                data_frame.append(batch['sold']) #if number of hits for current offset is 0, start next iteration
                break


list_of_pandas = [] #create empty list
range_of_frame = range(0,len(data_frame),1)
for i in range_of_frame:
    list_of_pandas.append(pd.io.json.json_normalize(data_frame[i])) #flatten each JSON request to panda format

df_out = pd.concat(list_of_pandas) #merge all panda-lists to one frame

df_out.to_csv('original_data\data_booli_out.csv', sep='\t', encoding='utf-8') #export retrieved booli data to csv
