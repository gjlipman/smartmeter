from myutils.utils import getConnection, load_bmrs_data, cronlog
from myutils.keys import metkey


cronlog()
import pandas as pd
import numpy as np
import requests
from io import StringIO
import datetime



t = datetime.datetime.today()

# This section works out maximum solar for each hour
datalist = []
for d in range(14):
    kwargs = {'report': 'B1630'} 
    kwargs['dates'] = ("SettlementDate=" + 
                       (t-pd.offsets.Day(d+1)).strftime('%Y-%m-%d') + 
                       '&Period=*&')
    r = load_bmrs_data(**kwargs)
    data = pd.read_csv(StringIO(r), 
                       header=None, 
                       skiprows=5)
    data = data[data[2]=='Solar']
    datalist.append(data[[3,4,5]])
    
data = pd.concat(datalist)
data[4] = ((data[4]-1)/2).astype(int)
data.rename(columns={3: 'date', 4: 'time', 5: 'solar'}, inplace=True)
data['date'] = (data['date'].str[:4] + data['date'].str[5:7] + 
                data['date'].str[8:]).astype(int)
fullsolar = data.groupby(['time']).max()['solar']



# This section calculates gross demand (demand + solar) for the last 7 days
kwargs['report'] = 'FORDAYDEM'
kwargs['dates'] = ('FromDate=' + (t-pd.offsets.Day(7)).strftime('%Y-%m-%d') +
                   '&ToDate=' + (t-pd.offsets.Day(1)).strftime('%Y-%m-%d') + 
                   '&')
r = load_bmrs_data(**kwargs)
data = pd.read_csv(StringIO(r), header=None, skiprows=1)
data = data[data[0]=='DANF']
data[6] = ((data[2]-1)/2).astype(int)
data.rename(columns={1: 'date', 6: 'time', 5: 'demand'}, inplace=True)
demand = data[['date','time','demand']].groupby(['date','time']).mean()

datalist = []
for d in range(7):
    kwargs['dates'] = ("SettlementDate=" + 
                       (t-pd.offsets.Day(d+1)).strftime('%Y-%m-%d') + 
                       '&Period=*&')
    kwargs['report'] = 'B1630'
    r = load_bmrs_data(**kwargs)
    data = pd.read_csv(StringIO(r), 
                       header=None, 
                       skiprows=5)
    data = data[data[2]=='Solar']
    datalist.append(data[[3,4,5]])
    
data = pd.concat(datalist)
data[4] = ((data[4]-1)/2).astype(int)

data.rename(columns={3: 'date', 4: 'time', 5: 'solar'}, inplace=True)
data['date'] = (data['date'].str[:4] + data['date'].str[5:7] + 
                data['date'].str[8:]).astype(int)
solar = data.groupby(['date', 'time']).mean()

demand = pd.concat([demand, solar], axis=1)
demand.reset_index(inplace=True)
demand['grossdemand'] = demand['demand']+demand['solar']
demand.interpolate(inplace=True)


# Gets wind forecasts
dates = ('FromDate=' + t.strftime('%Y-%m-%d') + '&ToDate=' +
             (t+pd.offsets.Day(4)).strftime('%Y-%m-%d') + '&')
kwargs = {'report': 'WINDFORFUELHH', 
          'dates': dates} 
r = load_bmrs_data(**kwargs)
data = pd.read_csv(StringIO(r), header=None, skiprows=1)
data = data[data[5].isnull()==0]
data1 = data[[1,2,6]].copy()
data1.columns = ['date','time','peakMW']
data1.loc[:,'time'] = (data1.loc[:,'time'].values-1)/2

kwargs['dates'] = ""
kwargs['report'] = 'FOU2T14D'
r = load_bmrs_data(**kwargs)
data = pd.read_csv(StringIO(r), header=None, skiprows=1)
data = data[data[1]=='WIND']
data2 = data.loc[:,4:].copy()
data2.reset_index(drop=True, inplace=True)
data2.columns=['date','peakMW']
data2['time']=-1

for i in range(1,len(data2)-1):
    x = data2.peakMW.values[(i-1):(i+2)]
    if x[1]<=max(x[0], x[2]):
        if x[0]>x[2]:
            t = 0
        elif x[0]<x[2]:
            t = 23
        else:
            t = 11
    else:
        t = round(23*(x[1]-x[0])/(2*x[1]-x[0]-x[2]))
    data2.loc[i,'time'] = t        
i = len(data2)-1
x = data2.peakMW.values[(i-1):]
data2.loc[i,'time'] = 0
data2.loc[i,'peakMW'] = min(x)
    
data = pd.concat([data1, data2.iloc[1:]], ignore_index=True, sort=False)
temp = data['date'].astype(str) + 'T' + data['time'].astype(str)
temp = [datetime.datetime.strptime(x, '%Y%m%d.0T%H.0') for x in temp.values]
data = pd.Series(data.peakMW.values, index=temp)

idx = pd.date_range(start=data.index[0], end=data.index[-1], freq='H')
d2 = data[idx].interpolate()
d2 = d2.iloc[24:8*24]

data= pd.DataFrame(index=d2.index)
data['grossdemand'] = np.hstack([demand.grossdemand.values[24:],demand.grossdemand.values[:24]])
data['wind'] = d2.values
data['fullsolar'] = fullsolar.values.tolist()*7




resource = 'val/wxfcs/all/json/3772/?res=daily&'
meturl = 'http://datapoint.metoffice.gov.uk/public/data/{}key={}'

r = requests.get(meturl.format(resource, metkey))


days = r.json()['SiteRep']['DV']['Location']['Period']
UVs = [float(x['Rep'][0]['U']) for x in days]
UVs = UVs + UVs[-1:]*2
UVs = [[x/7.0]*24 for x in UVs]
UVs = np.array(UVs).reshape(-1)
data['solar'] = data['fullsolar']*UVs
data['demand'] = data.grossdemand-data.solar
data['netdemand'] = data.demand-data.wind

conn, cur = getConnection()
s = 'select slope, intercept from price_function order by date desc, created_on desc limit 1;'
cur.execute(s)
slope, intercept = cur.fetchone()

data['price'] = np.log(data.netdemand.values)*slope+intercept


if True:
    timestamp = datetime.datetime.now().isoformat()[:16]
    s = """
    INSERT INTO price_forecast (datetime, demand, solar, wind, price, created_on)
    VALUES """
    for i, j in data.iterrows():
        s += "('{}', {}, {}, {}, {}, '{}'),".format(i, j.grossdemand, j.solar, j.wind, j.price, timestamp)
    s = s[:-1] + ';'

    #print(s)
    cur.execute(s)
    conn.commit()

conn.close()

