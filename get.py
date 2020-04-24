# -*- coding: utf-8 -*-
"""

This scripts accesses https://data.ontario.ca/dataset API to call a given dataset
"""

import requests
import pandas as pd
import time
import pyodbc
import sqlalchemy
import os
import pandas as pd
import numpy as np
import datetime

## Define server parameters ###
server = ''
database = ''
username = ''
password = ''
driver= '{ODBC Driver 13 for SQL Server}'
cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+'; \
                      PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
engine = sqlalchemy.create_engine('',echo=False)


## Define table label ##

label = 'ON_covidcases'

## Set call limit/rate ###
request = '&limit='
limit='10000'
rid = '455fd63b-603d-4608-8216-7d8647f43350'

### Define url request 
url = 'https://data.ontario.ca/api/3/action/datastore_search?resource_id='+rid+request+limit


def get_obj(url): 
    response = requests.get(url)
    obj = response.json()
    return response, obj

def get_item(obj):
    dicts = obj.get('result', {}).get('records',{})
    return dicts

def out_df(dicts):
    df = pd.DataFrame(dicts)
    return df

def push(engine, label):
    df.to_sql(label,con=engine,if_exists='replace',index=False)


    
if __name__ == '__main__':
    
    response, obj = get_obj(url)
    dicts = get_item(obj)
    df = out_df(dicts)
    push(engine, label)
