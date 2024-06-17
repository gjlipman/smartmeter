from myutils.utils import getConnection, load_bmrs_data, cronlog, email_script, loadDataFromDb
from myutils.keys import metkey


cronlog()
import pandas as pd
import numpy as np
import requests
import pickle
from io import StringIO
import datetime
import traceback

errstr = ''


# Solar
try:
    url = 'https://api.nationalgrideso.com/api/3/action/datastore_search?resource_id=db6c038f-98af-4570-ab60-24d71ebd0ae5&limit=500'
    r = requests.get(url)
    df = pd.DataFrame(r.json()['result']['records'])[['DATE_GMT','TIME_GMT','EMBEDDED_SOLAR_FORECAST']]
    df.index = pd.DatetimeIndex(df.DATE_GMT.str[:11] + df.TIME_GMT) - pd.offsets.Minute(30)
    df.drop(columns=["DATE_GMT","TIME_GMT"], inplace=True)
    df = df.resample("1H").mean()
    df['forecast_for'] = df.index.strftime('%Y-%m-%dT%H:%M')
    df['forecast_at'] = pd.Timestamp.utcnow().strftime('%Y-%m-%dT%H:%M')
    df = df.reset_index(drop=True).rename(columns={'EMBEDDED_SOLAR_FORECAST':'forecast'})

    s = """
    INSERT INTO forecast_solar (forecast_for, forecast_at, forecast)
    VALUES """
    for i, j in df.iterrows():
        s += "('{}', '{}', {}),".format(j.forecast_for, j.forecast_at, j.forecast)
    s = s[:-1] + ';'

    #print(s)
    conn, cur = getConnection()
    cur.execute(s)
    conn.commit()


except Exception as err:  
    errstr +=  str(err) 
    errstr += traceback.format_exc() + '\n'



# Wind
try:
    url = 'https://api.nationalgrideso.com/api/3/action/datastore_search?resource_id=93c3048e-1dab-4057-a2a9-417540583929&limit=500'
    r = requests.get(url)
    df = pd.DataFrame(r.json()['result']['records'])[['Datetime','Wind_Forecast']]
    df.index = pd.DatetimeIndex(df.Datetime)-pd.offsets.Minute(30)
    df.drop(columns=["Datetime"], inplace=True)
    df = df.resample('1H').mean()
    df['forecast_for'] = df.index.strftime('%Y-%m-%dT%H:%M')
    df['forecast_at'] = pd.Timestamp.utcnow().strftime('%Y-%m-%dT%H:%M')
    df = df.reset_index(drop=True).rename(columns={'Wind_Forecast':'forecast'})

    s = """
    INSERT INTO forecast_wind (forecast_for, forecast_at, forecast)
    VALUES """
    for i, j in df.iterrows():
        s += "('{}', '{}', {}),".format(j.forecast_for, j.forecast_at, j.forecast)
    s = s[:-1] + ';'

    #print(s)
    conn, cur = getConnection()
    cur.execute(s)
    conn.commit()


except Exception as err:  
    errstr +=  str(err) 
    errstr += traceback.format_exc() + '\n'




# Demand
try:
    url = 'https://api.nationalgrideso.com/api/3/action/datastore_search?resource_id=7c0411cd-2714-4bb5-a408-adb065edf34d&limit=500'
    r = requests.get(url)
    df = pd.DataFrame(r.json()['result']['records'])[['GDATETIME','NATIONALDEMAND']]
    df.index = pd.DatetimeIndex(df.GDATETIME)-pd.offsets.Minute(30)
    df.drop(columns=['GDATETIME'], inplace=True)
    df = df.resample("1H").mean()
    df['forecast_for'] = df.index.strftime("%Y-%m-%dT%H:%M")
    df['forecast_at'] = pd.Timestamp.utcnow().strftime("%Y-%m-%dT%H:%M")
    df = df.reset_index(drop=True).rename(columns=dict(NATIONALDEMAND="forecast"))

    s = """
    INSERT INTO forecast_demand (forecast_for, forecast_at, forecast)
    VALUES """
    for i, j in df.iterrows():
        s += "('{}', '{}', {}),".format(j.forecast_for, j.forecast_at, j.forecast)
    s = s[:-1] + ';'

    #print(s)
    conn, cur = getConnection()
    cur.execute(s)
    conn.commit()


except Exception as err:  
    errstr +=  str(err) 
    errstr += traceback.format_exc() + '\n'


try:
    before =  pd.Timestamp.now().isoformat()

    s = f'''
    with d1 as (select forecast_for, max(forecast_at) as forecast_at from forecast_demand 
    where forecast_for>=date_trunc('day', TIMESTAMP '{before}') and forecast_at<'{before}'
    group by forecast_for order by 1 limit 168)
    , d as (select d1.forecast_for, d1.forecast_at as demand_at, forecast_demand.forecast as demand
    from d1 inner join forecast_demand on d1.forecast_for=forecast_demand.forecast_for and d1.forecast_at=forecast_demand.forecast_at)
    select * from d 
    '''
    data_d = loadDataFromDb(s, returndf=True)
    s = f'''
    with d1 as (select forecast_for, max(forecast_at) as forecast_at from forecast_wind 
    where forecast_for>=date_trunc('day', TIMESTAMP '{before}') and forecast_at<'{before}'
    group by forecast_for order by 1 limit 168)
    , w as (select d1.forecast_for, d1.forecast_at as wind_at, forecast_wind.forecast as wind
    from d1 inner join forecast_wind on d1.forecast_for=forecast_wind.forecast_for and d1.forecast_at=forecast_wind.forecast_at)
    
    select * from w 
    '''
    data_w = loadDataFromDb(s, returndf=True)
    s = f'''
    with d1 as (select forecast_for, max(forecast_at) as forecast_at from forecast_solar
    where forecast_for>=date_trunc('day', TIMESTAMP '{before}') and forecast_at<'{before}'
    group by forecast_for order by 1 limit 168)
    , s as (select d1.forecast_for, d1.forecast_at as solar_at, forecast_solar.forecast as solar
    from d1 inner join forecast_solar on d1.forecast_for=forecast_solar.forecast_for and d1.forecast_at=forecast_solar.forecast_at)

    select * from s 
    '''
    data_s = loadDataFromDb(s, returndf=True)
    d = pd.concat([x.set_index('forecast_for') for x in [data_d, data_w, data_s]], axis=1)
    d = d.iloc[:168]
    d['solar'] = d.solar.fillna(0)
    d['demand'] = d.demand + d.solar      
    d['created_on'] = max(d.demand_at.max(), d.solar_at.max(), d.wind_at.max())
    data = d.reset_index().rename(columns={'forecast_for': 'datetime'})
    with open("//home/django/django_project/forecasts/cache.pkl", "wb") as writer:
        pickle.dump(data, writer)
except Exception as err:  
    errstr +=  str(err) 
    errstr += traceback.format_exc() + '\n'


email_script(errstr, 'priceforecast.py', 1)
if len(errstr):
    print(errstr)