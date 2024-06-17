import pandas as pd
import numpy as np
import requests
from io import StringIO
import datetime
import traceback
import time
from myutils.utils import cronlog, loadDataFromDb, email_script

cronlog()

START='201901010000'
errstr = ''
def getintensity(dt):
    url = "https://api.carbonintensity.org.uk/intensity/"
    print(url + dt + "/fw48h")
    r = requests.get(url + dt + "/fw48h")
    if r.status_code != 200:
        r = requests.get(url + dt + "/fw48h")
    if r.status_code != 200:
        return None, None

    j = r.json()['data']
    if dt<'2021-12-27T11:00Z':
        vals = [x['intensity']['actual'] if x['intensity']['actual'] is not None else x['intensity']['forecast']  for x in j ]
    else:
        vals = [x['intensity']['actual'] for x in j ]

    return j[-1]['to'],  pd.DataFrame(vals, index=[x['from'] for x in j])


try:
    t = (pd.Timestamp.now() - pd.offsets.Day(2)).strftime('%Y-%m-%d')
    s = f'''
        delete from sm_hh_variable_vals where id in 
        (select v.id  
        from sm_periods p inner join sm_hh_variable_vals v on p.period_id=v.period_id
        inner join sm_variables t on t.var_id=v.var_id and t.product='CO2_National'
        where  p.period> '{t}')   
    '''
    df = loadDataFromDb(s, returndf=True)
    


    s = """
    select s.var_id, s.product, s.region, max(sm_periods.period_id) as period_id, max(period) as period 
    from sm_variables s
    left join sm_hh_variable_vals on s.var_id=sm_hh_variable_vals.var_id  
    left join sm_periods on sm_periods.period_id=sm_hh_variable_vals.period_id
    where s.product='CO2_National'
    group by s.var_id, s.product, s.region;
    """
    data = loadDataFromDb(s)[0]
    latest = data[4]
    var_id = data[0]
    idx = pd.date_range(START, '202409010000', freq='30T')  
    df = pd.DataFrame()
    df['timestamp'] = idx
    df = pd.DataFrame(idx, columns=['timestamp'])
    df = df.iloc[:-1].copy()

    dt = latest.replace(' ', 'T')+ 'Z'
    intensities = []

    for d in range(500):
        new_dt, intensity = getintensity(dt)
        if intensity is None:
            #errstr += "Could not load carbon intensity for " + dt + "\n"
            break
        intensities.append( intensity)
        if intensity[0].isna()[-1]:
            break
        dt = new_dt
    if len(intensities):
        d = pd.concat(intensities)
        d.columns=['intensity']
        last = d[d.intensity.notna()].index.max()
        d = d.loc[:last].copy()

        for i in range(len(d)):
            if np.isnan(d.intensity.iloc[i]):
                if i<48:
                    d.intensity.iloc[i] = d.intensity.iloc[i-1]
                else:
                    d.intensity.iloc[i] = d.intensity.iloc[i-48]
            
        d['timestamp'] = pd.DatetimeIndex(d.index.str[:16])
        d = d.iloc[2:]
        d = df.merge(d, how='left', on='timestamp' )
        d = d[d.intensity.notna()]   
        s = "insert into sm_hh_variable_vals (var_id, period_id, value) values "
        for i, j in d.iterrows():
            s+= " ({}, {}, {}),".format(var_id, i, j.intensity)
        s = s[:-1] + ';'
        loadDataFromDb(s)
except Exception as err:  
    errstr += 'Problem with carbon intensity import \n'
    errstr +=  str(err) 
    errstr += traceback.format_exc() + '\n'

if False:
    try:
        t = (pd.Timestamp.now() - pd.offsets.Day(2)).strftime('%Y-%m-%d')
        s = f"""
        delete from sm_d_variable_vals where local_date>'{t}'
        """
        loadDataFromDb(s)

        s = """
        select sm_variables.var_id, product, region, max(sm_d_variable_vals.local_date) as latest_date 
        from sm_variables 
        inner join sm_d_variable_vals on sm_variables.var_id=sm_d_variable_vals.var_id 
        where sm_variables.granularity_id=1 and sm_variables.type_id=0
        group by sm_variables.var_id, product, region;
        """
        mins = loadDataFromDb(s, returndf=True)

        for i, j in mins.iterrows():
            if j['product'] not in ['SILVER-2017-1']:
                continue 
            r = requests.get(f'https://octopus.energy/api/v1/tracker/E-1R-SILVER-2017-1-{j.region}/daily/past/90/1/')
            dates = [x['date'] for x in r.json()['periods']]
            prices = [x['unit_rate'] for x in r.json()['periods']]
            d = pd.Series(prices, index=dates)
            d = d[j.latest_date.strftime('%Y-%m-%d'):datetime.date.today().strftime('%Y-%m-%d')]
            d = d.iloc[1:]
            d = d/1.05

            if len(d)>0:
                s = """
                INSERT INTO sm_d_variable_vals (var_id, local_date, value) values
                """
                for a, b in d.iteritems():
                    s+= f" ({j.var_id}, '{a}', {b}),"
                s = s[:-1] + ';'
                loadDataFromDb(s)
    except Exception as err:  
        errstr += 'Problem with tracker electricity price import \n'
        errstr +=  str(err) 
        errstr += traceback.format_exc() + '\n'


try:
    if False:
        s = """
        select sm_variables.var_id, product, region, max(sm_d_variable_vals.local_date) as latest_date 
        from sm_variables 
        inner join sm_d_variable_vals on sm_variables.var_id=sm_d_variable_vals.var_id 
        where sm_variables.granularity_id=1 and sm_variables.type_id=1
        group by sm_variables.var_id, product, region;
        """
        mins = loadDataFromDb(s, returndf=True)

        for i, j in mins.iterrows():
            if j['product'] not in ['SILVER-2017-1']:
                continue 

            r = requests.get(f'https://octopus.energy/api/v1/tracker/G-1R-SILVER-2017-1-{j.region}/daily/past/90/1/')
            dates = [x['date'] for x in r.json()['periods']]
            prices = [x['unit_rate'] for x in r.json()['periods']]
            d = pd.Series(prices, index=dates)
            d = d[j.latest_date.strftime('%Y-%m-%d'):datetime.date.today().strftime('%Y-%m-%d')]
            d = d.iloc[1:]
            d = d/1.05

            if len(d)>0:
                s = """
                INSERT INTO sm_d_variable_vals (var_id, local_date, value) values
                """
                for a, b in d.iteritems():
                    s+= f" ({j.var_id}, '{a}', {b}),"
                s = s[:-1] + ';'
                loadDataFromDb(s)
except Exception as err:  
    errstr += 'Problem with tracker gas price import \n'
    errstr +=  str(err) 
    errstr += traceback.format_exc() + '\n'


try:
    idx = pd.date_range(START, '202409010000', freq='30T')  
    df = pd.DataFrame()
    df['timestamp'] = idx
    df = pd.DataFrame(idx, columns=['timestamp'])

    s = """
    select sm_variables.var_id, product, region, max(sm_periods.period_id) as period_id, max(period) as period 
    from sm_variables
    inner join sm_hh_variable_vals on sm_variables.var_id=sm_hh_variable_vals.var_id and sm_variables.granularity_id=0 
    and sm_variables.region!='Z'
    inner join sm_periods on sm_periods.period_id=sm_hh_variable_vals.period_id
    group by sm_variables.var_id, product, region;
    """
    mins = loadDataFromDb(s, returndf=True)

    for i, j in mins.iterrows():

        if j['product'] not in ['AGILE-18-02-21','GO-18-06-12', 'AGILE-OUTGOING-19-05-13','AGILE-22-07-22', 'AGILE-FLEX-22-11-25']:
            continue 

        start = j.period.replace(' ','T')
        end = '2024-09-01T00:00'
        url = ('https://api.octopus.energy/v1/products/{}/' + 
            'electricity-tariffs/E-1R-{}-{}/standard-unit-rates/' + 
                '?period_from={}Z&period_to={}Z&page_size=15000')
  
        url = url.format(j['product'], j['product'], j.region, start, end)
        for attempt in range(3):
            r = requests.get(url)
            print(r.status_code)
            if 200 <= r.status_code < 300:
                r = r.json().get('results',[])
                break
            else:
                print(r.text)
                print(r.status_code)
                time.sleep(10)
        else:
            errstr += f'Unable to load {url} \n'
            errstr += f'{r.text} \n'
            continue

        if len(r)==0:
            errstr += f'No data retrieved for {url} \n'
            continue

        dfs = pd.DataFrame(r)[['valid_from','valid_to','value_exc_vat']]

        dfs.drop_duplicates(keep='last',inplace=True)


        dfs.index = pd.DatetimeIndex(dfs.valid_from.str[:16])
        dfs.sort_index(inplace=True)
        dfs.loc[pd.Timestamp(dfs.valid_to[-1][:16])] = dfs.iloc[-1]
        dfs = dfs['value_exc_vat']
        
        dfs = dfs.resample('30T').ffill()
        dfs = dfs[dfs.index>j.period]


        dfs = dfs.iloc[:-1].copy() 

        dfs = pd.merge(left=df, right=dfs, left_on='timestamp', right_index=True, how='left')
        dfs = dfs[dfs.value_exc_vat.notna()]


        if len(dfs):
            s = """
            INSERT INTO sm_hh_variable_vals (var_id, period_id, value) values
            """
            for a, b in dfs.iterrows():
                s+= " ({}, {}, {}),".format(j.var_id, a, b.value_exc_vat)
            s = s[:-1] + ';'
            loadDataFromDb(s)        
except Exception as err:  
    errstr += 'Problem with Agile price import \n'
    errstr +=  str(err) 
    errstr += traceback.format_exc() + '\n'

try:
    cutoff = (datetime.datetime.now()-pd.offsets.Day(2)).isoformat()
    s = f'''
    update sm_accounts set active='0' where last_updated<'{cutoff}' and session_id != 'e4280c7d-9d06-4bbe-87b4-f9e106ede788'
    '''
    loadDataFromDb(s)
    s = '''
    delete from sm_quantity where account_id not in (select account_id from sm_accounts where active='1')
    '''
    loadDataFromDb(s)
except Exception as err:  
    errstr += 'Problem with deleting historical data \n'
    errstr +=  str(err) 
    errstr += traceback.format_exc() + '\n'

email_script(errstr, 'smloads.py', 1)
if len(errstr):
    print(errstr)