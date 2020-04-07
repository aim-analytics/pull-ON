# -*- coding: utf-8 -*-
"""
Created on Tue Apr  7 11:03:01 2020

@author: csun
"""

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

label = 'ON_healthcareProv'

## Set call limit/rate ###


### Define url request 
url = 'https://opendata.arcgis.com/datasets/68853b605c844c6ebe041003b4c71b56_26.geojson'


def get_obj(url): 
    response = requests.get(url)
    obj = response.json()
    return response, obj


def get_item(obj):
    dicts = obj.get('features', {})
    return dicts

def out_df(dicts):
    df = pd.io.json.json_normalize(dicts)
    return df

def clean_df(df):
    df['geometry.coordinates'] = df['geometry.coordinates'].astype(str)
    df['geometry.coordinates'] = df['geometry.coordinates'].replace('\[|\]', '', regex=True)
    df[['X','Y']] = df['geometry.coordinates'].str.split(',',expand=True)
    df.columns = ['Coordinates',
              'GeoType',
              'Descriptor',
              'AddressLine1',
              'AddressLine2',
              'Community',
              'UpdatedAt',
              'Name',
              'AltName',
              'FrenchName',
              'FrenchNameAlt',
              'GeoUpdateAt',
              'ServiceProvider',
              'ObjectId',
              'OGFId',
              'PostalCode',
              'ServiceType',
              'ServiceDetail',
              'SystemDateTime',
              'Type',
              'X',
              'Y']
    
    return df

def push(engine, label):
    df.to_sql(label,con=engine,if_exists='replace',index=False)


response, obj = get_obj(url)
dicts = get_item(obj)
df = out_df(dicts)
push(engine, label)
