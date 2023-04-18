from os import environ
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from datetime import datetime
from time import time
import pandas as pd
import numpy as np


print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgresSQL successful.')

# Write the solution here


def calc_dist(lat1, lon1, lat2, lon2, to_radians=True, earth_radius=6371):

    if to_radians:
        lat1, lon1, lat2, lon2 = np.radians([lat1, lon1, lat2, lon2])

    a = np.sin((lat2-lat1)/2.0)**2 + \
        np.cos(lat1) * np.cos(lat2) * np.sin((lon2-lon1)/2.0)**2

    return earth_radius * 2 * np.arcsin(np.sqrt(a))

while True:
    try:
        mysql_engine = create_engine(environ["MYSQL_CS"])
        break
    except OperationalError:
        sleep(0.1)
print('Connection to MYSQL successful.')

psqlcon = psql_engine.connect()
mysqlcon = mysql_engine.connect()

df = pd.read_sql("select * from devices", psqlcon)

print(df.head(n=10).to_string(index=False))

df['time'] = pd.to_datetime(df['time'], unit='s').astype(str).str[:-6]
df1 = df.groupby(['device_id','time'])['temperature'].max().reset_index(name='max_temp')

df2 = df.groupby(['device_id','time'])['device_id'].count().reset_index(name='counts')


df['location']=df['location'].str.replace('{"latitude": "', '').str.replace(' "longitude": "', '').str.replace('"}','').str.replace('"','')
df['lat']=df['location'].str.split(',').str[0].astype(float)
df['long']=df['location'].str.split(',').str[1].astype(float)


df['lagged_lat'] = df.groupby(['device_id'])['lat'].shift(1)
df['lagged_long'] = df.groupby(['device_id'])['long'].shift(1)
df.dropna(inplace=True)


df['dist'] =calc_dist(df['lat'], df['long'],df['lagged_lat'], df['lagged_long'])

df3 = df.groupby(['device_id'])['dist'].sum().reset_index(name='total_dist')


try:
    df1.to_sql("maxtemperature", mysqlcon, if_exists="replace")
    df2.to_sql("devicecount", mysqlcon, if_exists="replace")
    df3.to_sql("totaldistance", mysqlcon, if_exists="replace")

except Exception as ex:
    print(ex)

print("tables created successfully")

df1 = pd.read_sql(
    "SELECT * FROM maxtemperature",
    con=mysqlcon
)

df2 = pd.read_sql(
    "SELECT * FROM devicecount",
    con=mysqlcon
)

df3 = pd.read_sql(
    "SELECT * FROM totaldistance",
    con=mysqlcon
)


print(df1.head(n=10).to_string(index=False))
print(df2.head(n=10).to_string(index=False))
print(df3.head(n=10).to_string(index=False))

