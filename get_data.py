from sqlalchemy import create_engine
from sqlalchemy import Column, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.sqltypes import FLOAT, Integer
from sqlalchemy.dialects import postgresql
import requests
import json
import pandas as pd
from decimal import Decimal
import numpy as np

from psycopg2.extensions import register_adapter, AsIs
def adapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
def adapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)
register_adapter(np.float64, adapt_numpy_float64)
register_adapter(np.int64, adapt_numpy_int64)

# Import data from BLS website
series_names = ['CUUR0000SA0','WPUFD4']

headers = {'Content-type': 'application/json'}
data = json.dumps({"seriesid": series_names,"startyear":"2011", 
                   "endyear":"2020"})
p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
json_data = json.loads(p.text)

d = {}

for series in json_data['Results']['series']:
    
    csv_data = []
    
    seriesId = series['seriesID']
    for item in series['data']:
        year = item['year']
        period = item['period']
        value = item['value']
        
        if 'M01' <= period <= 'M12':
            csv_data.append([seriesId,year,period,value])
    
    d[series['seriesID']] = pd.DataFrame({
        "idx":[x for x in range(0,len(csv_data))],
        "Series":[x[0] for x in csv_data],
        "Year":[x[1] for x in csv_data],
        "Period":[x[2] for x in csv_data],
        "Value":[float(x[3]) for x in csv_data]
    })

# SQL

from config_create import sql_host, sql_database, sql_user, sql_password

db_string = "postgresql://" + sql_user + ":" + sql_password + "@" + sql_host + "/" + sql_database

db = create_engine(db_string)
base = declarative_base()

db.execute('DROP TABLE IF EXISTS cpi_' + series_names[0])
db.execute('DROP TABLE IF EXISTS ppi_' + series_names[1])

class ConPriceIndex(base):
    __tablename__ = 'cpi_' + series_names[0]
    idx = Column(Integer, primary_key=True)
    year=Column(Integer)
    period = Column(String)
    value = Column(FLOAT)

class ProdPriceIndex(base):
    __tablename__ = 'ppi_' + series_names[1]
    idx = Column(Integer, primary_key=True)
    year=Column(Integer)
    period = Column(String)
    value = Column(FLOAT)

Session = sessionmaker(db)
session = Session()

base.metadata.create_all(db)

# Create 
for i in range(0,len(d[series_names[0]])):
    session.add(ConPriceIndex(idx=d[series_names[0]].idx[i],
                            year=d[series_names[0]].Year[i],
                            period=d[series_names[0]].Period[i],
                            value=d[series_names[0]].Value[i]))

for i in range(0,len(d[series_names[1]])):
    session.add(ProdPriceIndex(idx=d[series_names[1]].idx[i],
                            year=d[series_names[1]].Year[i],
                            period=d[series_names[1]].Period[i],
                            value=d[series_names[1]].Value[i]))

session.commit()