
import pandas as pd
import numpy as np
from scipy import stats
import requests
from io import StringIO
import datetime
from myutils.utils import getConnection, load_bmrs_data, cronlog, email_script
import traceback

cronlog()
numdays = 14    
errstr = ''
try:
    t = datetime.datetime.today() - pd.offsets.Day(0)

    # Get day ahead prices
    r = requests.get('https://www.nordpoolgroup.com/globalassets/marketdata-excel-files/n2ex-day-ahead-auction-prices_2020_hourly_gbp.xls')
    a = pd.read_html(StringIO(r.text))[0]
    a.columns=['date','time1','time2','price']
    a = a[a.price.notna()].copy()
    a.price = a.price/100
    a['date'] = a['date'].str[-4:] + a['date'].str[3:5] + a['date'].str[:2]
    a['time1'] = a['time1'].str[:2]
    a.drop(columns=['time2'], inplace=True)
    dates = a.date.unique()
    dates2 = {k: v for k, v in zip(dates[1:], dates[:-1])}
    a['date2'] = np.where(a.time1=='23', a.date.map(dates2), a.date)
    lastdate = datetime.datetime.strptime(a['date'].iloc[-1], '%Y%m%d')
    dates = [(lastdate-pd.offsets.Day(d)).strftime('%Y%m%d') for d in range(numdays)]
    a = a[a.date2.isin(dates)].copy()
    a['date'] = a['date2'].astype(int)
    a['time'] = a['time1'].astype(int)
    prices = a[['date','time','price']].groupby(['date','time']).mean()



    # Get Historic demand (net of solar)
    datalist = []
    for d in range(numdays):
        date = (lastdate-pd.offsets.Day(d)).strftime('%Y-%m-%d')
        dates = 'FromDate=' + date + '&ToDate=' + date + '&'
        kwargs = {'report': 'FORDAYDEM', 'dates': dates} 
        r = load_bmrs_data(**kwargs)
        data = pd.read_csv(StringIO(r), header=None, skiprows=1)
        data = data[data[0]=='DANF']
        data[6] = ((data[2]-1)/2).astype(int)
        datalist.append(data)
    data = pd.concat(datalist)    
    data.rename(columns={1: 'date', 6: 'time', 5: 'demand'}, inplace=True)
    demand = data[['date','time','demand']].groupby(['date','time']).mean()

    # Get historic wind
    datalist = []
    for d in range(numdays):
        date = (lastdate-pd.offsets.Day(d)).strftime('%Y-%m-%d')
        dates = ('FromDate=' + date + '&ToDate=' + date + '&')
        kwargs = {'report': 'WINDFORFUELHH', 
                'dates': dates} 
        r = load_bmrs_data(**kwargs)
        data = pd.read_csv(StringIO(r), header=None, skiprows=1)
        data = data[data[5].isnull()==0]
        datalist.append(data)
    data = pd.concat(datalist)
    data1 = data[[1,2,6]].copy()
    data1.columns = ['date','time','wind']
    data1['time'] = (data1['time'].values-1)/2
    wind = data1.groupby(['date','time']).mean()


    df = pd.DataFrame(demand)
    df['wind'] = wind
    df['netdemand'] = df.demand-df.wind
    df['price'] = prices
    df.reset_index(inplace=True)
    df = df[df.netdemand.notna()]
    df = df[df.price.notna()].copy()


    slope, intercept, r_value, _, _ =  stats.linregress(np.log(df.netdemand.values), df.price.values )
    df['predictprice'] = np.log(df.netdemand.values)*slope + intercept
    df['error'] = df.price-df.predictprice

    #print('slope: {}, intercept: {}, r: {}'.format(slope, intercept, r_value))




    s = """
    INSERT INTO price_function (date, slope, intercept, r, created_on)
    VALUES 
    ('{date}', {slope}, {intercept}, {r}, CURRENT_TIMESTAMP);
    """
    #('2020-07-04', 2., 2.9, 0.7, CURRENT_TIMESTAMP);
    s = s.format(date=t.strftime('%Y-%m-%d'), slope=slope, intercept=intercept, r=r_value)
    conn, cur = getConnection()
    cur.execute(s)
    conn.commit()

    conn.close()
except Exception as err:  
    errstr +=  str(err) 
    errstr += traceback.format_exc() + '\n'

email_script(errstr, 'pricefit.py', 0)
if len(errstr):
    print(errstr)