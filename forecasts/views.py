from django.http import HttpResponse, HttpRequest
from django.shortcuts import render
import os
import sys
import pandas as pd
import numpy as np
import json
from myutils.utils import getConnection, loadDataFromDb



def get_prices(region, product, after_date):
    s = f'''
    select concat(local_date, ' ', local_time) dt, value from sm_hh_variable_vals v 
    inner join sm_periods on v.period_id=sm_periods.period_id
    inner join sm_tariffs on sm_tariffs.var_id=v.var_id
    where product='{product}' and region='{region}' 
    and concat(sm_periods.local_date, ' ', sm_periods.local_time)>='{after_date.strftime('%Y-%m-%d %H:%M')}'
    order by dt
    '''
    df2 = loadDataFromDb(s, returndf=True)

    #raise Exception(df2.iloc[40:60])
    df2 = pd.Series(0.5*(df2['value'].iloc[0::2].values + df2['value'].iloc[1::2].values), index=df2['dt'].iloc[0::2])    
    return df2


def inner(request):
    region = request.GET.get('region', 'W').upper()
    before = request.GET.get('before', pd.Timestamp.now().isoformat())   
    ph = ('ph' in request.GET)


    s = f'''
    with latest as (select datetime, max(created_on) created_on from price_forecast 
    where datetime>=date_trunc('day', TIMESTAMP '{before}') and created_on<'{before}'
    group by datetime) 
        
        select latest.datetime, demand, solar, wind, price, latest.created_on 
        from price_forecast inner join latest on price_forecast.datetime=latest.datetime 
        and price_forecast.created_on=latest.created_on
      order by latest.datetime;'''

    data = loadDataFromDb(s, returndf=True)

    
    if region=='W':
        df2 = get_prices('C','AGILE-18-02-21', data['datetime'].iloc[0])
        df3 = get_prices('C','AGILE-OUTGOING-19-05-13', data['datetime'].iloc[0])
        df2 = df2 - np.where(df2.index.str[-5:-3].isin(['16','17','18']), 12, 0)
        df2 = df2*10/2
        df3 = df3 - np.where(df3.index.str[-5:-3].isin(['16','17','18']), 5.93+1.3, 1.3)
        df3 = df3*10/0.95
        df2 = pd.Series(np.where(df3>df2, df3, df2), index=df2.index)

        

    else:
        df2 = get_prices(region,'AGILE-18-02-21', data['datetime'].iloc[0])
        df2 *= 1.05

    if ph:
        hrs = data['datetime'].astype(str).str[11:13]
        data = data[(hrs>='08')&(hrs<'20')]
        #raise Exception(data['datetime'].astype(str).str[:16], df2.index)
        df2 = df2[df2.index.isin(data['datetime'].astype(str).str[:16])]
        
        #raise Exception(data['datetime'].astype(str).str[11:13])
        #     

    with open(os.path.dirname(os.path.realpath(__file__)) + "/template.html", "r") as f:
        template = f.read()


    if region=='W':

        prices =  str(['{:.2f}'.format(d.price) for _, d in data.iterrows()])
        
        retail = "The prices are essentially estimates of day ahead hourly auction results in £/MWh."
        retail2 = "Estimates of hourly day ahead prices in £/MWh"
    else:
        multipliers = {'A': 2.1, 'B': 2, 'C': 2, 'D':2.2, 'E': 2.1, 'F': 2.1, 'G': 2.1, 'H': 2.1, 'J': 2.2, 'K': 2.2, 'L': 2.3, 'M': 2, 'N': 2.1, 'P': 2.4}
        adders = {'A': 13, 'B': 14, 'C': 12, 'D':13, 'E': 12, 'F': 12, 'G': 12, 'H': 12, 'J': 12, 'K': 12, 'L': 11, 'M': 13, 'N': 13, 'P': 12}
        multiplier = multipliers[region]
        adder = adders[region]
        
        data.price *= multiplier/10
        data.price += np.where(data['datetime'].astype(str).str[-8:-6].isin(['16','17','18']), adder, 0)
        data.price *= 1.05
        prices = str(['{:.2f}'.format(d.price) for _, d in data.iterrows()])
        retail = f"The prices are essentially estimates of the day ahead hourly auction results in p/kwh, converted into retail prices for region {region} by multiplying by {multiplier}, and adding {adder} from 4-7pm. Prices include 5% VAT."
        retail = retail.format(region, multiplier, adder)
        retail2 = f"Estimates of Octopus retail prices for region {region} in p/kwh"

    url = request.build_absolute_uri()
    if 'before' not in url:
        if "?" in url:
            url = url + '&before=2020-12-31T12:00'
        else:
            url = url + '?before=2020-12-31T12:00'
        retail += f'''<P>If you want to know how historic forecasts performed, you can see the latest forecast before a past datetime, eg {url}</P>'''
    if 'json' in request.GET:
        myobj = []
        for _, j in data.iterrows():
            myobj.append({'datetime': j['datetime'].strftime('%Y-%m-%dT%H:%M'),
                          'demand': j['demand']/1000,
                          'solar': j['solar']/1000,
                          'wind': j['wind']/1000,
                          'price': j['price'] })
        return json.dumps(myobj)

    kwargs = {'asof': data['created_on'].iloc[-1].isoformat()[:16],
            'datetimes': str([d['datetime'].strftime('%a %Hh') for _, d in data.iterrows()]),
            'demand': str(['{:.2f}'.format(d['demand']/1000) for _, d in data.iterrows()]),
            'solar': str(['{:.2f}'.format(d['solar']/1000) for _, d in data.iterrows()]),
            'wind': str([ '{:.2f}'.format(d['wind']/1000) for _, d in data.iterrows()]),
            'prices': prices ,
            'actual': str(['{:.2f}'.format(d) for d in df2.values]),
            'retail': retail,
            'retail2': retail2}

    for k, v in kwargs.items():
        template = template.replace('{' + k + '}', v)

    return template


def index(request):
    try:
        template = inner(request)
        return HttpResponse(template)
    except Exception as err:    
        import traceback
        errstr = str(err) + '<BR>'
        errstr += '<BR>'.join([x for x in traceback.format_exc().splitlines()])
        return HttpResponse(errstr)
