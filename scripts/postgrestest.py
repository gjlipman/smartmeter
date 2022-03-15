
from myutils.utils import getConnection, cronlog
import pandas as pd
import numpy as np
import datetime
import requests

class TestRequest:
    def __init__(self, url, method='GET', META=None, postdata=None):
        self.method = method
        u = url.split('?')
        self.path_info = u[0]
        self.META = META or {}
        self.GET = {}
        if len(u)>1:
            for x in u[1].split('&'):
                y = x.split('=')
                if len(y)==1:
                    self.GET[x] = ''
                else:
                    self.GET[y[0]] = y[1]
        self.PUT = postdata
    
    def get_full_path(self):
        return url


conn, cur = getConnection()


if False:
    s = """
    DROP TABLE IF EXISTS price_function;
    CREATE TABLE price_function (
    id smallserial PRIMARY KEY
    , date DATE NOT NULL 
    , slope FLOAT(8) NOT NULL
    , intercept FLOAT(8) NOT NULL
    , r FLOAT(8) NOT NULL
    , created_on TIMESTAMP NOT NULL
    ); 
    """
    cur.execute(s)
    conn.commit()

if False:
    s = """
    INSERT INTO price_function (date, slope, intercept, r, created_on)
    VALUES 
    ('2020-07-05', 3, 2.8, 0.9, CURRENT_TIMESTAMP),
    ('2020-07-04', 2., 2.9, 0.7, CURRENT_TIMESTAMP);
    """
    cur.execute(s)
    conn.commit()

    s = 'select * from price_function;'
    cur.execute(s)
    list_tables = cur.fetchall()
    print(list_tables)


if False:
    conn.commit()
    s = """
    DROP TABLE IF EXISTS price_forecast;
    CREATE TABLE price_forecast (
    id serial PRIMARY KEY
    , datetime TIMESTAMP NOT NULL 
    , demand Float(8) NOT NULL
    , solar Float(8) NOT NULL
    , wind Float(8) NOT NULL
    , price Float(4) NOT NULL 
    , created_on TIMESTAMP NOT NULL
    ); 
    """
    cur.execute(s)
    conn.commit()

if False:
    s = """
    DROP TABLE IF EXISTS testing;
    CREATE TABLE testing (
        id serial PRIMARY KEY
        , created_on TIMESTAMP NOT NULL
    ); """
    cur.execute(s)
    conn.commit()


if False:
    conn.commit()
    s = """
    DROP TABLE IF EXISTS sm_periods;
    CREATE TABLE sm_periods (
    period_id serial PRIMARY KEY
    , period Char(16) not null
    , local_date Date not null
    , local_time char(5) not null
    , timezone_adj smallint not null
    ); 
    """
    cur.execute(s)
    conn.commit()

    df_idx = pd.date_range(datetime.datetime(2019,1,1), datetime.datetime(2020,10,1), freq='30min')
    df_idx_local = df_idx.tz_localize('UTC').tz_convert('Europe/London')
    df = pd.DataFrame(index=df_idx)
    df['period'] = df_idx.strftime('%Y-%m-%d %H:%M')
    df['local_date'] = df_idx_local.strftime('%Y-%m-%d')
    df['local_time'] = df_idx_local.strftime('%H:%M')
    df['timezone_adj'] = df_idx_local.strftime('%z').str[0:3].astype(int)
    df.reset_index(inplace=True)
    start = """
    INSERT INTO sm_periods (period_id, period, local_date, local_time, timezone_adj)
    VALUES 
    """
    s=""
    for i, j in df.iterrows():
        s+= "({},'{}', '{}', '{}', {}),".format(i, j['period'], j['local_date'],j['local_time'], j['timezone_adj'])
        if (i+1)%1000==0:
            print('done: {}'.format(i+1))
            cur.execute(start + s[:-1] + ';')
            conn.commit()
            s=""
            
    print('done: {}'.format(i+1))
    cur.execute(start + s[:-1] + ';')
    conn.commit()
    s=""



if False:
    conn.commit() 

    s = """
    DROP TABLE IF EXISTS sm_accounts;
    CREATE TABLE sm_accounts (
    account_id serial PRIMARY KEY
    , type_id smallint not null
    , first_period varChar(16) not null
    , last_period varChar(16) not null
    , last_updated TIMESTAMP not null
    , hash varChar(64) not null
    , region varChar(1) 
    , source_id smallint not null 
    ); 
    """
    cur.execute(s)
    conn.commit()

if False:
    conn.commit()
    s = """
    DROP TABLE IF EXISTS sm_quantity;
    CREATE TABLE sm_quantity (
    id serial PRIMARY KEY
    , account_id integer not null
    , period_id integer not null
    , quantity float(8) not null
    ); 
    """
    cur.execute(s)
    conn.commit()

if False:
    conn.commit()
    s = """
    DROP TABLE IF EXISTS sm_hh_variables;
    CREATE TABLE sm_hh_variables (
    var_id serial PRIMARY KEY
    , var_name varchar(32) not null
    , var_type varchar(32));
    """
    cur.execute(s)
    conn.commit() 



if False:
    conn.commit()
    s = """
    DROP TABLE IF EXISTS sm_d_variables;
    CREATE TABLE sm_d_variables (
    var_id serial PRIMARY KEY
    , var_name varchar(32) not null
    , var_type varchar(32));
    """
    cur.execute(s)
    conn.commit() 


if False:   # Creates new hh tariff variables in sm_hh_variables and sm_tariffs
    product = 'AGILE-OUTGOING-19-05-13'
    type_id=2

    s = f"""
    delete from sm_hh_variables where var_name like '{product}%';
    delete from sm_tariffs where product='{product}';
    """

    cur.execute(s)
    conn.commit() 
    
    for region in ['A','B','C','D','E','F','G','H','J','K','L','M','N','P']:    
        s = f"""
        INSERT INTO sm_hh_variables (var_name) values ('{product}-{region}');
        """
        cur.execute(s)
        conn.commit()  
        s = f"select var_id from sm_hh_variables where var_name='{product}-{region}';"
        cur.execute(s)
        var_id = cur.fetchone()[0]
        conn.commit()  

        s = f"""
        INSERT INTO sm_tariffs (type_id, product, region, granularity_id, var_id) values
        ({type_id}, '{product}', '{region}', 0, {var_id});
        """
        cur.execute(s)
        conn.commit()  

START='201901010000'
if False: #Inserts initial prices into hh tariff variables

    import requests
    idx = pd.date_range(START, '202101010000', freq='30T')  
    df = pd.DataFrame()
    df['timestamp'] = idx
    df = pd.DataFrame(idx, columns=['timestamp'])

    for region in ['B','C','D','E','F','G','H','J','K','L','M','N','P']:
        tariff = 'AGILE-OUTGOING-19-05-13'
        url = ('https://api.octopus.energy/v1/products/{}/' + 
            'electricity-tariffs/E-1R-{}-{}/standard-unit-rates/' + 
                '?period_from={}Z&period_to={}Z&page_size=15000')
        url = url.format(tariff, tariff, region,
                        df.timestamp.iloc[0].strftime('%Y-%m-%dT%H:%M'),
                        df.timestamp.iloc[-1].strftime('%Y-%m-%dT%H:%M'))

        r = requests.get(url)
        dfs = []
        dfs.append(pd.DataFrame(r.json()['results'])[['valid_from','value_exc_vat']])

        while r.json()['next'] is not None:
            r = requests.get(r.json()['next'])
            dfs.append(pd.DataFrame(r.json()['results'])[['valid_from','value_exc_vat']])
            if len(dfs)>30:
                raise Exception        

        dfs = pd.concat(dfs)
        dfs['timestamp'] = pd.DatetimeIndex(dfs.valid_from.str[:-1])
        dfs = df.merge(right=dfs, how='left', on='timestamp')
        dfs = dfs[dfs.value_exc_vat.notna()]


        s = f"select var_id from sm_hh_variables where var_name='{tariff}-{region}';"
        cur.execute(s)
        var_id = cur.fetchone()[0]
        conn.commit()  
        print(f'{var_id} {tariff} {region}' )
        s = """
        delete from sm_hh_variable_vals where var_id={}; 
        """
        s = s.format(var_id)        
        cur.execute(s)
        conn.commit()  
  
        s = """
        INSERT INTO sm_hh_variable_vals (var_id, period_id, value) values
        """
        s = s.format(var_id)
        for i, j in dfs.iterrows():
            s+= " ({}, {}, {}),".format(var_id, i, j.value_exc_vat)
        s = s[:-1] + ';'
        
        cur.execute(s)
        conn.commit()  

        


if False:
    conn.commit()
    s = """
    DROP TABLE IF EXISTS sm_hh_variable_vals;
    CREATE TABLE sm_hh_variable_vals (
    id serial primary key
    , var_id integer not null
    , period_id integer not null
    , value float(8) not null);
    """
    cur.execute(s)
    conn.commit() 

if False:
    conn.commit()
    s = """
    DROP TABLE IF EXISTS sm_d_variable_vals;
    CREATE TABLE sm_d_variable_vals (
    id serial primary key
    , var_id integer not null
    , local_date date not null
    , value float(8) not null);
    """
    cur.execute(s)
    conn.commit() 


from myutils.utils import loadDataFromDb

if False:  #Creates daily tracker variables
    product = 'SILVER-2017-1'

    for region in ['A','B','C','D','E','F','G','H','J','K','L','M','N','P']:
        s = f"""
        insert into sm_d_variables (var_name) values ('{product}-{region}') returning var_id; """
        var_id = loadDataFromDb(s)[0][0]
        print(var_id)
        s = f"""
        insert into sm_tariffs (product, region, var_id, type_id, granularity_id) values
          ('{product}', '{region}', {var_id}, 1, 1); """
        loadDataFromDb(s) 

if False:
    product = 'SILVER-2017-1'

    for region in ['A','B','C','D','E','F','G','H','J','K','L','M','N','P']:
        s = f"select var_id from sm_variables where product='{product}' and region='{region}' ;"
        var_id = loadDataFromDb(s)[0][0]   

        r = requests.get(f'https://octopus.energy/api/v1/tracker/G-1R-SILVER-2017-1-{region}/daily/past/540/1/')    
        dates = [x['date'] for x in r.json()['periods']]
        prices = [x['unit_rate'] for x in r.json()['periods']]
        d = pd.Series(prices, index=dates)
        d = d[:datetime.date.today().strftime('%Y-%m-%d')]
        d = d/1.05 
        d = d.round(2)
        s = 'insert into sm_d_variable_vals (var_id, local_date, value) values '
        for i, j in d.iteritems():
            s+= f"({var_id}, '{i}', {j}),"
        s = s[:-1]+';'
        loadDataFromDb(s)
        print(region)


if False:
    conn.commit()
    import requests
    idx = pd.date_range(START, '202101010000', freq='30T')  
    df = pd.DataFrame()
    df['timestamp'] = idx
    df = pd.DataFrame(idx, columns=['timestamp'])

    s = """
    select sm_hh_variables.var_id, var_name, min(sm_periods.period_id) as period_id, min(period) as period 
    from sm_periods cross join sm_hh_variables  
    left outer join sm_hh_variable_vals on sm_periods.period_id=sm_hh_variable_vals.period_id 
     and sm_hh_variable_vals.var_id=sm_hh_variables.var_id
    where sm_hh_variable_vals.id is null 
    group by sm_hh_variables.var_id, var_name;
    """
    mins = loadDataFromDb(s, returndf=True)

    for i, j in mins.iterrows():
        tariff = j.var_name
        if 'AGILE-18-02-21' not in tariff:
            continue 
        print(tariff)
        start = j.period.replace(' ','T')
        end = '2021-01-01T00:00'
        url = ('https://api.octopus.energy/v1/products/AGILE-18-02-21/' + 
            'electricity-tariffs/E-1R-{}/standard-unit-rates/' + 
                '?period_from={}Z&period_to={}Z&page_size=15000')
  
        url = url.format(tariff, start, end)
        r = requests.get(url)
        r = r.json()['results']
        if len(r)==0:
            continue
        dfs = pd.DataFrame(r)[['valid_from','value_exc_vat']]
        dfs['timestamp'] = pd.DatetimeIndex(dfs.valid_from.str[:-1])
        dfs = df.merge(right=dfs, how='left', on='timestamp')   
        dfs = dfs[dfs.valid_from.notna()]

        print(dfs)
   
        s = """
        INSERT INTO sm_hh_variable_vals (var_id, period_id, value) values
        """
        for a, b in dfs.iterrows():
            s+= " ({}, {}, {}),".format(j.var_id, a, b.value_exc_vat)
        s = s[:-1] + ';'
        
        cur.execute(s)
        print(cur.statusmessage)
        conn.commit()   




if False:    
    s = 'select account_id, code from sm_accounts;'
    a = loadDataFromDb(s, returndf=True)
    s = ''
    for i, j in a.iterrows():    
        s+= "update sm_accounts set hash='{}' where account_id={};\n".format(encode(j.code), j.account_id)

    loadDataFromDb(s)

if False:
    # Checks for gaps
    s = """ 
    select sm_accounts.account_id, sm_accounts.type_id, sm_periods.period from sm_accounts 
        left join sm_periods on sm_periods.period between sm_accounts.first_period and sm_accounts.last_period 
        left join sm_quantity on sm_quantity.period_id=sm_periods.period_id and sm_quantity.account_id= sm_accounts.account_id
        where sm_quantity.quantity is null 
    """

    df = loadDataFromDb(s, returndf=True)
    print(df) 

if False:
    s = """
    DROP TABLE IF EXISTS sm_tariffs;
    CREATE TABLE sm_tariffs (
    tariff_id serial primary key
    , type_id integer not null
    , product varchar not null
    , region char(1) not null
    , granularity_id integer not null
    , var_id integer not null);
    """

    loadDataFromDb(s)

if False:
    s = """
    insert into sm_tariffs (type_id, product, region, granularity_id, var_id)
    select 
    0 as type_id, left(var_name, 14) as product, right(var_name, 1) as region, 0 as granularity_id, var_id 
    from sm_hh_variables;
    """
    loadDataFromDb(s)
    print(loadDataFromDb('select * from sm_tariffs', returndf=True))

if False:
    s = """
    select period from sm_periods 
    left join sm_quantity on sm_quantity.period_id=sm_periods.period_id 
    where sm_quantity.quantity is null and sm_periods.local_date between '2020/07/01' and '2020/07/30' 
        and sm_quantity.account_id in (select account_id from sm_accounts where hash LIKE 
        'c6e81194c0aa3d65d0522d41171e4d07301457dc1cb26f7b05f60a70227be1f3%' and type_id=0);
    """

    s = """
    with p as (select period, period_id from sm_periods where local_date between '2020/07/01' and '2020/07/30' ), 
    q as (select quantity, period_id from sm_quantity where sm_quantity.account_id in (select account_id from sm_accounts where hash LIKE 
        'c6e81194c0aa3d65d0522d41171e4d07301457dc1cb26f7b05f60a70227be1f3%' and type_id=0))
    select count(p.period) from p 
    left join q on q.period_id=p.period_id
    where q.quantity is null;
    """
    print(loadDataFromDb(s, returndf=True))



if False:
    s = "insert into sm_hh_variables (var_name) Values ('Profile_1'), ('Profile_2');"
    #loadDataFromDb(s)
    # 

    for pc in [1,2]:
        idx = pd.date_range(START, '202203312300', freq='30T')  
        df = pd.DataFrame()
        df['timestamp'] = idx
        df = pd.DataFrame(idx, columns=['timestamp'])
        df = df.iloc[:-1].copy()
        f = '/home/django/django_project/scripts/Default_Period_Profile_Class_Coefficient_309.csv'
        d = pd.read_csv(f)
        d.columns = ['class','d1','period','coeff']
        d = d[d['class']==pc]

        d['date'] = d.d1.str[6:] + d.d1.str[2:6] + d.d1.str[:2]
        d = d[d.date>=(START[:4] + '/' + START[4:6] + '/' + START[6:8])]
        df = df[df.timestamp>='2021-03-31 23:00']


        #d = d[d.date<'2021/04/01']
        d = d.iloc[:len(df)]
        assert(len(d)==len(df))


        df['coeff'] = d.coeff.tolist()


        s = "select var_id from sm_hh_variables where var_name='{}';".format('Profile_{}'.format(pc))
        var_id = loadDataFromDb(s)[0][0]
        s = "insert into sm_hh_variable_vals (var_id, period_id, value) values "
        for i, j in df.iterrows():
            s+= " ({}, {}, {}),".format(var_id, i, j.coeff)
        s = s[:-1] + ';'
        loadDataFromDb(s)


if False: #Gets latest carbon intensity
    s = "insert into sm_hh_variables (var_name) Values ('CO2_National');"
    #loadDataFromDb(s) 

    s = """
    select s.var_id, s.var_name, max(sm_periods.period_id) as period_id, max(period) as period 
    from sm_hh_variables s
    left join sm_hh_variable_vals on s.var_id=sm_hh_variable_vals.var_id  
    left join sm_periods on sm_periods.period_id=sm_hh_variable_vals.period_id
    where s.var_name='CO2_National'
    group by s.var_id, s.var_name;
    """
    data = loadDataFromDb(s)[0]
    latest = data[3]
    var_id = data[0]
    print(latest)

    idx = pd.date_range(START, '202101010000', freq='30T')  
    df = pd.DataFrame()
    df['timestamp'] = idx
    df = pd.DataFrame(idx, columns=['timestamp'])
    df = df.iloc[:-1].copy()

  

    def getintensity(dt):
        url = "https://api.carbonintensity.org.uk/intensity/"
        r = requests.get(url + dt + "/fw48h")
        j = r.json()['data']
        return j[-1]['to'],  pd.DataFrame([x['intensity']['actual'] for x in j], index=[x['from'] for x in j])

    dt = latest.replace(' ', 'T')+ 'Z'
    intensities = []

    for d in range(500):
        dt, intensity = getintensity(dt)
        intensities.append( intensity)
        if intensity[0].isna()[-1]:
            break

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
    print(d)    

    s = "insert into sm_hh_variable_vals (var_id, period_id, value) values "
    for i, j in d.iterrows():
        s+= " ({}, {}, {}),".format(var_id, i, j.intensity)
    s = s[:-1] + ';'
    loadDataFromDb(s)

if False:


    s = """
    select sm_periods.local_date, sm_periods.local_time, emis.value emissions, prof.value profile 
        from sm_hh_variable_vals emis
    inner join sm_hh_variable_vals prof on emis.period_id=prof.period_id and emis.var_id=31 and prof.var_id=29
    inner join sm_periods on sm_periods.period_id=emis.period_id;
    """

    s = """
    select sm_periods.local_date, sm_periods.local_time, emis.value emissions, prof.value profile, COALESCE(qty.quantity,0) quantity 
    from sm_hh_variable_vals emis
    inner join sm_hh_variable_vals prof on emis.period_id=prof.period_id and emis.var_id=31 and prof.var_id=29
    inner join sm_periods on sm_periods.period_id=emis.period_id
    left outer join sm_quantity qty on qty.period_id=emis.period_id and qty.account_id=21
    ;
    """
    s = """
    select sm_periods.local_date, sm_periods.local_time, emis.value emissions, prof.value profile, COALESCE(qty.quantity,0) quantity 
    from sm_hh_variable_vals emis
    inner join sm_hh_variable_vals prof on emis.period_id=prof.period_id and emis.var_id=31 and prof.var_id=29
    inner join sm_periods on sm_periods.period_id=emis.period_id
    left outer join sm_quantity qty on qty.period_id=emis.period_id and qty.account_id=21
    where local_date='2020-07-25'
    ;
    """

    df = loadDataFromDb(s, returndf=True)
    print(df)

    
if False:
    s = """
    DROP TABLE IF EXISTS sm_log;
    CREATE TABLE sm_log (
    id serial primary key
    , datetime timestamp not null
     , mode integer not null
     , url varchar(124) not null
   , hash varchar(64) not null);
    """
    loadDataFromDb(s)



if False:
    for region in ['A','B','C','D','E','F','G','H','J','K','L','M','N','P']:
        tariff = 'GO-18-06-12'
        s = '''
        insert into sm_hh_variables (var_name) values ('{t}-{r}');
        insert into sm_tariffs (type_id, product, region, granularity_id, var_id) 
        select 0 as type_id, '{t}' as product, '{r}' as region, 0 as granularity_id, var_id 
        from sm_hh_variables where var_name='{t}-{r}';
        '''
        loadDataFromDb(s.format(t=tariff, r=region))

if False:

    idx = pd.date_range(START, '202101010000', freq='30T')  
    df = pd.DataFrame()
    df['timestamp'] = idx
    df = pd.DataFrame(idx, columns=['timestamp'])
    for region in ['A','B','C','D','E','F','G','H','J','K','L','M','N','P']:
        tariff = 'GO-18-06-12'
        print(region)
        url = ('https://api.octopus.energy/v1/products/{}/' + 
            'electricity-tariffs/E-1R-{}-{}/standard-unit-rates/' + 
                '?period_from={}Z&period_to={}Z&page_size=15000')
        url = url.format(tariff, tariff, region,
                        df.timestamp.iloc[0].strftime('%Y-%m-%dT%H:%M'),
                        df.timestamp.iloc[-1].strftime('%Y-%m-%dT%H:%M'))
        
        r = requests.get(url)
        
        dfs = pd.DataFrame(r.json()['results'])
        
        dfs.index = pd.DatetimeIndex(dfs.valid_from.str[:16])
        dfs.sort_index(inplace=True)
        dfs.loc[pd.Timestamp(dfs.valid_to[-1][:16])] = dfs.iloc[-1]
        dfs = dfs['value_exc_vat']
        dfs = dfs.resample('30T').ffill()
        
        dfs = pd.merge(left=df, right=dfs, left_on='timestamp', right_index=True, how='left')
        dfs = dfs[dfs.value_exc_vat.notna()]

        var_id = loadDataFromDb("select var_id from sm_tariffs where product='{}' and region='{}'".format(tariff, region))[0][0]
        s = '''
        insert into sm_hh_variable_vals (var_id, period_id, value) values 
        '''
        for i, j in dfs.iterrows():
            s += "({}, {}, {}),".format(var_id, i, j.value_exc_vat)
        s = s[:-1] + ';'
        loadDataFromDb(s)

        
if False:
    from myutils.utils import encode
    keys = ['ea873c14b0626673', 'd307ca43950726cf','db1da1932e528e06']
    source_id=1
    region='C'
    multipliers = [1.2, 0.8, 0.7]
    newhash = encode('demo')

    s = """
    delete from sm_accounts where hash='{}';
    """
    loadDataFromDb(s.format(newhash))

    for i in range(3):
        s = """
        insert into sm_accounts (type_id, first_period, last_period, last_updated, hash, region, source_id )
            select {}, first_period, last_period, CURRENT_TIMESTAMP, '{}', '{}', {}  
            from sm_accounts where hash LIKE '{}%' and type_id={} returning account_id;
        """
        s = s.format(i, newhash, region, source_id, keys[i], i)
        account_id = loadDataFromDb(s)[0][0]

        s = '''
        insert into sm_quantity (account_id, period_id, quantity)
           select {} as account_id, period_id, {}*quantity as quantity from sm_quantity 
           inner join sm_accounts on sm_accounts.account_id=sm_quantity.account_id 
           where hash LIKE '{}%' and type_id={};
        '''
        s = s.format(account_id, multipliers[i], keys[i], i)
        print(s)
        loadDataFromDb(s)
        print(i)
 
if False:
    s = 'delete from sm_quantity where account_id not in (select account_id from sm_accounts)'
    loadDataFromDb(s)
    s = '''
    select distinct account_id from sm_quantity order by account_id;
    '''
    print(loadDataFromDb(s, returndf=True))
    s = '''
    select distinct account_id from sm_accounts order by account_id;
    '''
    print(loadDataFromDb(s, returndf=True))
    s = 'select count(id) from sm_quantity;'
    print(loadDataFromDb(s, returndf=True))

if False: #Inserts latest daily gas tracker prices
    conn.commit()
    import requests
    import datetime

    s = """
    select sm_tariffs.var_id, sm_tariffs.tariff_id, product, region, max(sm_d_variable_vals.local_date) as latest_date 
    from sm_tariffs 
    inner join sm_d_variable_vals on sm_tariffs.var_id=sm_d_variable_vals.var_id and sm_tariffs.granularity_id=1
    group by sm_tariffs.var_id, sm_tariffs.tariff_id, product, region;
    """
    mins = loadDataFromDb(s, returndf=True)
    print(mins)

    for i, j in mins.iterrows():
        if j['product'] not in ['SILVER-2017-1']:
            continue 
        print(f"{j['product']}_{j.region}")
        r = requests.get(f'https://octopus.energy/api/v1/tracker/G-1R-SILVER-2017-1-{j.region}/daily/past/90/1/')
        dates = [x['date'] for x in r.json()['periods']]
        prices = [x['unit_rate'] for x in r.json()['periods']]
        d = pd.Series(prices, index=dates)
        d = d[j.latest_date.strftime('%Y-%m-%d'):datetime.date.today().strftime('%Y-%m-%d')]
        d = d.iloc[1:]

        if len(d)==0:
            continue

        print(d)

        s = """
        INSERT INTO sm_d_variable_vals (var_id, local_date, value) values
        """
        for a, b in d.iteritems():
            s+= f" ({j.var_id}, '{a}', {b}),"
        s = s[:-1] + ';'
        cur.execute(s)
        print(cur.statusmessage)
        conn.commit()   



if False: #Inserts latest hh prices
    conn.commit()
    import requests
    idx = pd.date_range(START, '202101010000', freq='30T')  
    df = pd.DataFrame()
    df['timestamp'] = idx
    df = pd.DataFrame(idx, columns=['timestamp'])

    s = """
    select sm_tariffs.var_id, sm_tariffs.tariff_id, product, region, max(sm_periods.period_id) as period_id, max(period) as period 
    from sm_tariffs 
    inner join sm_hh_variable_vals on sm_tariffs.var_id=sm_hh_variable_vals.var_id and sm_tariffs.granularity_id=0
    inner join sm_periods on sm_periods.period_id=sm_hh_variable_vals.period_id
    group by sm_tariffs.var_id, sm_tariffs.tariff_id, product, region;
    """
    mins = loadDataFromDb(s, returndf=True)
    print(mins)
    for i, j in mins.iterrows():

        if j['product'] not in ['AGILE-18-02-21','GO-18-06-12', 'AGILE-OUTGOING-19-05-13']:
            continue 
        print(f"{j['product']}_{j.region}")
        start = j.period.replace(' ','T')
        end = '2021-01-01T00:00'
        url = ('https://api.octopus.energy/v1/products/{}/' + 
            'electricity-tariffs/E-1R-{}-{}/standard-unit-rates/' + 
                '?period_from={}Z&period_to={}Z&page_size=15000')
  
        url = url.format(j['product'], j['product'], j.region, start, end)
        r = requests.get(url)

        r = r.json().get('results',[])
        if len(r)==0:
            continue

        dfs = pd.DataFrame(r)[['valid_from','valid_to','value_exc_vat']]

        dfs.index = pd.DatetimeIndex(dfs.valid_from.str[:16])
        dfs.sort_index(inplace=True)
        dfs.loc[pd.Timestamp(dfs.valid_to[-1][:16])] = dfs.iloc[-1]
        dfs = dfs.iloc[1:]
        dfs = dfs['value_exc_vat']
        dfs = dfs.resample('30T').ffill()
        dfs = dfs.iloc[:-1].copy()
                      
        dfs = pd.merge(left=df, right=dfs, left_on='timestamp', right_index=True, how='left')
        dfs = dfs[dfs.value_exc_vat.notna()]

        print(url)
        #print(len(dfs))
        print(dfs)
        if len(dfs):
            s = """
            INSERT INTO sm_hh_variable_vals (var_id, period_id, value) values
            """
            for a, b in dfs.iterrows():
                s+= " ({}, {}, {}),".format(j.var_id, a, b.value_exc_vat)
            s = s[:-1] + ';'
            
            cur.execute(s)
            print(cur.statusmessage)
            conn.commit()   

if False:

    s = """
    select var_id, period_id, max(id) m from sm_hh_variable_vals 
    group by var_id, period_id
    having count(id)>1);
    """

    s = """
    delete from sm_hh_variable_vals s where id in (
    select max(id) m from sm_hh_variable_vals 
    group by var_id, period_id
    having count(id)>1);
    """

    s = """
    select count(id) from sm_hh_variable_vals s where
    var_id not in (select var_id from sm_hh_variables);
    """

    s = """
    select count(id) from sm_quantity where
    account_id not in (select account_id from sm_accounts);
    """

    s = "select count(id) from sm_quantity;"
    s = "select count(account_id) from sm_accounts;"

    s = """
    select s.var_id, s.var_name, max(sm_periods.period_id) as period_id, max(period) as period 
    from sm_hh_variables s
    left join sm_hh_variable_vals on s.var_id=sm_hh_variable_vals.var_id  
    left join sm_periods on sm_periods.period_id=sm_hh_variable_vals.period_id
    where s.var_name='CO2_National'
    group by s.var_id, s.var_name;
    """

    d = loadDataFromDb(s)
    print(d)
s = 3, 31
s = """
    select min(period_id) from sm_hh_variable_vals where var_id=47;
    """
#print(loadDataFromDb(s, returndf=True))
s = """
with accounts as (select distinct hash, source_id from sm_accounts) 
select mode, source_id, left(sm_log.hash, 6), count(id) 
from sm_log left outer join accounts on accounts.hash=sm_log.hash
where datetime>'2020-07-23 17:00' and datetime<'2020-07-24 17:00'
group by mode, source_id, sm_log.hash;
"""

if False:
    exportparams = {
        'A': (0.95, 1.09, 7.04), 
        'B': (0.94, 0.78, 6.27), 
        'C': (0.95, 1.3, 5.93), 
        'D': (0.97, 1.26, 5.97), 
        'E': (0.94, 0.77, 6.5), 
        'F': (0.95, 0.87, 4.88), 
        'G': (0.96, 1.1, 5.89), 
        'H': (0.94, 0.93, 7.05), 
        'J': (0.94, 1.09, 7.41), 
        'K': (0.94, 0.97, 5.46), 
        'L': (0.93, 0.83, 7.14), 
        'M': (0.96, 0.72, 5.78), 
        'N': (0.97, 0.9, 3.85), 
        'P': (0.96, 1.36, 2.68), }
    product = 'AGILE-OUTGOING-19-05-13'

    for region in ['A','B','C','D','E','F','G','H','J','K','L','M','N','P']:

        s = f"select var_id from sm_tariffs where product='{product}' and region='{region}'"
        var_id = loadDataFromDb(s)[0][0]
        print(f'{region}, {var_id}')
        p = exportparams[region]

        s = f"""
            insert into sm_hh_variable_vals (var_id, period_id, value) 
            select {var_id} var_id, v.period_id, ({p[2]} + {p[1]} + {p[0]}*(v.value-12)/2.0) as value from sm_hh_variable_vals v
            inner join sm_periods p on v.period_id=p.period_id
            where var_id=31 and p.period_id<6478 and local_time between'16:00' and '18:30';
            """
        print(loadDataFromDb(s, returndf=True))
        s = f"""
            insert into sm_hh_variable_vals (var_id, period_id, value) 
            select {var_id} var_id, v.period_id, ({p[1]}+{p[0]}*v.value/2.0) as value from sm_hh_variable_vals v
            inner join sm_periods p on v.period_id=p.period_id
            where var_id=31 and p.period_id<6478 and local_time not between'16:00' and '18:30';
            """
        print(loadDataFromDb(s, returndf=True))




if False:
    s = '''
    select local_date, local_time as local_time_start, timezone_adj, quantity as total_quantity, value as price, quantity*value as total_cost  
    from sm_quantity inner join sm_periods on sm_quantity.period_id=sm_periods.period_id
    inner join sm_hh_variable_vals on sm_hh_variable_vals.period_id=sm_quantity.period_id and sm_hh_variable_vals.var_id={} 
    where local_date BETWEEN '{}' AND '{}' and 
    sm_quantity.account_id in ({}) order by period;
    '''
    s = s.format(15, '2020-06-01', '2020-06-30', 58)

    s = '''
    select sm_periods.local_date, local_time as local_time_start, timezone_adj, quantity as total_quantity, value as price, quantity*value as total_cost  
    from sm_quantity inner join sm_periods on sm_quantity.period_id=sm_periods.period_id
    inner join sm_d_variable_vals on sm_d_variable_vals.local_date=sm_periods.local_date and sm_d_variable_vals.var_id={} 
    where sm_periods.local_date BETWEEN '{}' AND '{}' and 
    sm_quantity.account_id in ({}) order by period;
    '''
    s = s.format(1, '2020-06-01', '2020-06-30', 58)


    s = '''
                    select sm_periods.local_date as day, count(sm_quantity.id) as numperiods, sum(quantity) as total_quantity, value as price, sum(quantity*value) as total_cost  
                    from sm_quantity inner join sm_periods on sm_quantity.period_id=sm_periods.period_id 
                    inner join sm_d_variable_vals on sm_d_variable_vals.local_date=sm_periods.local_date and sm_d_variable_vals.var_id={} 
                    where sm_periods.local_date between '{}' and '{}' and date_trunc('month', sm_periods.local_date)='{}' and 
                    sm_quantity.account_id in ({}) group by sm_periods.local_date, value order by sm_periods.local_date;
                    '''   
               
    s = s.format(1, '2020-06-01', '2020-06-30', '2020-06' + '-01', 58)    

    s = '''
    select date_trunc('month', sm_periods.local_date) as month, count(sm_quantity.id) as numperiods, sum(value)/count(value) as price, sum(quantity) as total_quantity, sum(quantity*value) as total_cost  
    from sm_quantity inner join sm_periods on sm_quantity.period_id=sm_periods.period_id 
    inner join sm_d_variable_vals on sm_d_variable_vals.local_date=sm_periods.local_date and sm_d_variable_vals.var_id={} 
    where sm_periods.local_date between '{}' and '{}' and
    sm_quantity.account_id in ({}) group by month order by month;
    '''
    s = s.format(1, '2020-01-01', '2020-06-30', 58)       

    #print(loadDataFromDb(s, returndf=True))

if False: #Clean out old account data
    from myutils.utils import encode
    demohash = encode('A-EB5A2015sk_live_BXmPhoj6LwhwwfYvosRMePtm')   
    print(loadDataFromDb('select count(id) from sm_quantity;'))
    s = f"delete from sm_accounts where last_updated<'2020-07-26 09:00' and hash!='{demohash}';"
    loadDataFromDb(s)
    s = f"delete from sm_quantity where account_id not in (select account_id from sm_accounts);"
    loadDataFromDb(s)
    print(loadDataFromDb('select count(id) from sm_quantity;'))





conn.close()

if False:
    s = '''
    with hhvars as (
    select t.var_id, t.var_name, min(v.period_id) min, max(v.period_id) max from sm_hh_variables t
    left outer join sm_hh_variable_vals v on t.var_id=v.var_id 
    group by t.var_name, t.var_id order by var_name)

    select hhvars.var_id, hhvars.var_name, pmin.period min, pmax.period max from hhvars 
    inner join sm_periods pmin on pmin.period_id=hhvars.min
    inner join sm_periods pmax on pmax.period_id=hhvars.max
    ;
    '''
    print(loadDataFromDb(s, returndf=True))
   
    s = '''
    with hhvars as (
    select t.var_id, t.var_name, min(v.period_id) min, max(v.period_id) max from sm_hh_variables t
    left outer join sm_hh_variable_vals v on t.var_id=v.var_id 
    group by t.var_name, t.var_id order by var_name)

    select hhvars.var_id, hhvars.var_name, p.period, p.period_id
    from hhvars 
    inner join sm_periods p on p.period_id between hhvars.min and hhvars.max
    left outer join sm_hh_variable_vals v on v.period_id=p.period_id and hhvars.var_id=v.var_id
    where v.period_id is null
    ;
    '''
    print(loadDataFromDb(s, returndf=True))

if False:
    s = '''
    with dvars as (
    select t.var_id, t.var_name, min(v.local_date) min, max(v.local_date) max from sm_d_variables t
    left outer join sm_d_variable_vals v on t.var_id=v.var_id 
    group by t.var_name, t.var_id order by var_name)

    select dvars.var_id, dvars.var_name, dvars.min, dvars.max
    from dvars;
    '''
    print(loadDataFromDb(s, returndf=True))
   
    s = '''
    with dvars as (
    select t.var_id, t.var_name, min(v.local_date) min, max(v.local_date) max from sm_d_variables t
    left outer join sm_d_variable_vals v on t.var_id=v.var_id 
    group by t.var_name, t.var_id order by var_name)

    select distinct dvars.var_id, dvars.var_name, p.local_date
    from dvars 
    inner join sm_periods p on p.local_date between dvars.min and dvars.max
    left outer join sm_d_variable_vals v on v.local_date=p.local_date and dvars.var_id=v.var_id
    where v.local_date is null
    ;
    '''
    print(loadDataFromDb(s, returndf=True))

if False:
    s = '''
        select v.var_id, t.var_name, v.local_date, min(id) min, max(id) max, count(id)  
        from sm_d_variable_vals v 
        inner join sm_d_variables t on t.var_id=v.var_id
        group by v.var_id, t.var_name, v.local_date
        having count(id)>1;
    '''
    print(loadDataFromDb(s, returndf=True))


if False:
    s = '''
    with log as (select left(hash,8) shorthash, date(datetime-Interval '3 hours') myday, * from sm_log
            where url not LIKE '%debug%' and mode=0    ),
         firstlog as (select shorthash, min(myday) firstday from log where
         url LIKE '%task=load%' group by shorthash ),
         dailylog as (select shorthash, myday, count(id) numhits from log group by shorthash, myday ),
         sumlog as (select shorthash, max(myday) lastday, count(myday) as numdays, sum(numhits) numhits from dailylog 
         group by shorthash),
         sources as (select distinct shorthash, CASE WHEN url LIKE '%octopus%' THEN 'octopus' WHEN url LIKE '%n3rgy%' THEN 'n3rgy' else 'none' end as source 
                     from log where (url LIKE '%octopus%' or url LIKE '%n3rgy%') ) 


    select firstlog.shorthash, sources.source, sumlog.lastday, firstlog.firstday, sumlog.numdays, sumlog.numhits
    from firstlog inner join sumlog on firstlog.shorthash = sumlog.shorthash 
    left outer join sources on sources.shorthash=firstlog.shorthash  
    order by lastday desc, firstday desc, numdays desc 
    ;

    '''
    print(loadDataFromDb(s, returndf=True))

#CASE WHEN url LIKE '%?%' THEN left(url, position('?' in url)-1) 

if False:
    s = '''
    select account_id, period_id, count(id) from sm_quantity group by period_id, account_id
    having count(id)>1 order by account_id, period_id 
    '''
    df = loadDataFromDb(s, returndf=True)
    print(df)
    if len(df):
        s = ''' delete from sm_quantity where id in (
        select min(id) id from sm_quantity group by period_id, account_id
        having count(id)>1 order by account_id, period_id)
        '''
        loadDataFromDb(s)

        s = '''
        select account_id, period_id, count(id) from sm_quantity group by period_id, account_id
        having count(id)>1 order by account_id, period_id 
        '''
        df = loadDataFromDb(s, returndf=True)
        print(df)

if False:
    s = '''
    select  datetime, url from sm_log order by datetime desc limit 5;
    '''
    df = loadDataFromDb(s, returndf=True)
    print(df)



if False:
    s = '''
    Alter Table sm_log 
    add column session_id uuid,
    add column choice varchar(64);
    '''
    #loadDataFromDb(s)

    s = '''
    update sm_accounts
    set active = True
    '''
    #loadDataFromDb(s)

    s = '''
    alter table sm_log
    drop column mode,
    drop column hash
    '''
    #loadDataFromDb(s)

    s = "select * from sm_log order by datetime desc limit 5"
    s = '''
    update sm_log
    set choice= right(split_part(url, '?',1),-4)
    where choice is Null
            '''
    #loadDataFromDb(s)

    s = "select account_id, type_id, last_updated, region, session_id, active from sm_accounts order by last_updated"
    s = "select * from sm_log order by datetime"

    s = '''
    with log as 
               (select date(datetime-Interval '3 hours') myday, 
                right(split_part(url, '?',1),-4) as choice, 
                * from sm_log where url not LIKE '%debug%')
    select myday, count(id) from log where url like '%load%' group by myday order by myday


    '''
    print(loadDataFromDb(s, returndf=True))

    s = '''
    with log as 
               (select date(datetime-Interval '3 hours') myday, 
                right(split_part(url, '?',1),-4) as choice, 
                * from sm_log where url not LIKE '%debug%')
    select myday, count(id) from log group by myday order by myday


    '''


    print(loadDataFromDb(s, returndf=True))

    s = '''
    with log as 
               (select date(datetime-Interval '3 hours') myday, 
                * from sm_log where url not LIKE '%debug%'),
        log2 as (
    select 
    CASE WHEN POSITION('octopus' in url)>0 then 'octopus' WHEN POSITION ('n3rgy' in url)>0 then 'n3rgy' else 'unknown' end as source,  
    CASE when POSITION('loadgas' in url)>0 then 'gas' WHEN POSITION('loadexport' in url)>0 then 'export' else 'electricity' end as type,
    *
       from log where url like '%load%')
    
    select 
    myday, source, type, count(id) count from log2 group by myday, source, type
    order by myday desc, source, type

    '''
    print(loadDataFromDb(s, returndf=True))


if False:
    s = '''
        with sessions as (    
            select sm_accounts.account_id, sm_accounts.session_id, sm_accounts.type_id, last_updated, max(datetime) as last_called
            from sm_accounts 
            left outer join sm_log on sm_accounts.session_id=sm_log.session_id 
            where sm_accounts.session_id != 'e4280c7d-9d06-4bbe-87b4-f9e106ede788' and sm_accounts.active='1' 
            group by account_id, last_updated)
        update sm_accounts set active='0' where account_id in     
        (
        select account_id from sessions where last_updated<CURRENT_TIMESTAMP-Interval '6 hours' or last_called<CURRENT_TIMESTAMP-Interval '3 hours' )
  
        '''

    #print(loadDataFromDb(s, returndf=True))

    s = "delete from sm_quantity where account_id not in (select account_id from sm_accounts where active='1') "
    #print(loadDataFromDb(s, returndf=True))

    s = '''
        select count(id), count(session_id) from sm_quantity left outer join sm_accounts on sm_quantity.account_id=sm_accounts.account_id and sm_accounts.active='1'
        '''
    #print(loadDataFromDb(s, returndf=True))
    s = "select session_id, count(account_id) from sm_accounts where active='1' group by session_id having count(account_id)>0"
    
    s = "select count(id) from sm_quantity"
    print(loadDataFromDb(s, returndf=True))    
    
if False:
    s = '''Update sm_log set session_id=Null where session_id='e4280c7d-9d06-4bbe-87b4-f9e106ede788' '''
    #'e4280c7d-9d06-4bbe-87b4-f9e106ede788'
    print(loadDataFromDb(s))
    s = '''select * from sm_log where session_id='e4280c7d-9d06-4bbe-87b4-f9e106ede788' limit 5'''

    print(loadDataFromDb(s, returndf=True))   

if False:
    s = f'''
    select concat(local_date, ' ', local_time) dt, value from sm_hh_variable_vals v 
    inner join sm_periods on v.period_id=sm_periods.period_id
    inner join sm_tariffs on sm_tariffs.var_id=v.var_id
    where product='AGILE-18-02-21' and region='C' 
    order by dt desc limit 10
    '''
    print(loadDataFromDb(s, returndf=True))  
    s = f'''
    select concat(local_date, ' ', local_time) dt, value from sm_hh_variable_vals v 
    inner join sm_periods on v.period_id=sm_periods.period_id
    inner join sm_tariffs on sm_tariffs.var_id=v.var_id
    where product='AGILE-18-02-21' and region='C' 
    and concat(sm_periods.local_date, ' ', sm_periods.local_time)>='2020-10-01T00:00'
    order by dt
    '''
    print(loadDataFromDb(s, returndf=True))  

    idx = pd.date_range(START, '202101010000', freq='30T')  
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
    print(mins)

    for i, j in mins.iterrows():

        if j['product'] not in ['AGILE-18-02-21','GO-18-06-12', 'AGILE-OUTGOING-19-05-13']:
            continue 

        start = j.period.replace(' ','T')
        end = '2021-01-01T00:00'
        url = ('https://api.octopus.energy/v1/products/{}/' + 
            'electricity-tariffs/E-1R-{}-{}/standard-unit-rates/' + 
                '?period_from={}Z&period_to={}Z&page_size=15000')
  
        url = url.format(j['product'], j['product'], j.region, start, end)
        r = requests.get(url)

        r = r.json().get('results',[])
        if len(r)==0:
            continue

        dfs = pd.DataFrame(r)[['valid_from','valid_to','value_exc_vat']]
        dfs.index = pd.DatetimeIndex(dfs.valid_from.str[:16])
        dfs.sort_index(inplace=True)
        dfs.loc[pd.Timestamp(dfs.valid_to[-1][:16])] = dfs.iloc[-1]
        dfs = dfs.iloc[1:]
        dfs = dfs['value_exc_vat']
        dfs = dfs.resample('30T').ffill()
        dfs = dfs.iloc[:-1].copy()               
        dfs = pd.merge(left=df, right=dfs, left_on='timestamp', right_index=True, how='left')
        dfs = dfs[dfs.value_exc_vat.notna()]
        print(dfs)
        if len(dfs):
            s = """
            INSERT INTO sm_hh_variable_vals (var_id, period_id, value) values
            """
            for a, b in dfs.iterrows():
                s+= " ({}, {}, {}),".format(j.var_id, a, b.value_exc_vat)
            s = s[:-1] + ';'
            print(loadDataFromDb(s) )       


if False:
    conn, cur = getConnection()
    df_idx = pd.date_range(datetime.datetime(2019,1,1), datetime.datetime(2022,7,1), freq='30min')
    df_idx_local = df_idx.tz_localize('UTC').tz_convert('Europe/London')
    df = pd.DataFrame(index=df_idx)
    df['period'] = df_idx.strftime('%Y-%m-%d %H:%M')
    df['local_date'] = df_idx_local.strftime('%Y-%m-%d')
    df['local_time'] = df_idx_local.strftime('%H:%M')
    df['timezone_adj'] = df_idx_local.strftime('%z').str[0:3].astype(int)
    df.reset_index(inplace=True)
    df = df.loc[43777:]
    print(df)

    start = """
    INSERT INTO sm_periods (period_id, period, local_date, local_time, timezone_adj)
    VALUES 
    """
    s=""
    for i, j in df.iterrows():
        s+= "({},'{}', '{}', '{}', {}),".format(i, j['period'], j['local_date'],j['local_time'], j['timezone_adj'])
        if (i+1)%1000==0:
            print('done: {}'.format(i+1))
            cur.execute(start + s[:-1] + ';')
            conn.commit()
            s=""
            
    print('done: {}'.format(i+1))
    cur.execute(start + s[:-1] + ';')
    conn.commit()
    s=""
    
if False:
    s = '''
    with latest as (select max(id) m, var_id, period_id from sm_hh_variable_vals group by var_id, period_id)
    , remove as (select v.id from sm_hh_variable_vals v inner join latest on v.var_id=latest.var_id and v.period_id=latest.period_id 
    where v.id<latest.m 
    order by latest.var_id, latest.period_id)
    delete from sm_hh_variable_vals where id in (select id from remove)
    '''
    print(loadDataFromDb(s, returndf=True))


if False:
    s = '''
    with periods as (select * from sm_periods where local_date between '2020-08-01' and '2020-10-01' )
    , quantities1 as (select period_id, quantity 
    from sm_quantity
    inner join sm_accounts on sm_quantity.account_id=sm_accounts.account_id
    where session_id='39b76afc-118d-40e1-8368-c395fa0926e4' and type_id=0 and active='1')
    , quantities2 as (select period_id, quantity from sm_quantity
    inner join sm_accounts on sm_quantity.account_id=sm_accounts.account_id
    where session_id='39b76afc-118d-40e1-8368-c395fa0926e4' and type_id=2 and active='1')
    , fulldata as
    (select periods.*, coalesce(quantities1.quantity,0) as import, coalesce(quantities2.quantity, 0) as export
    from periods inner join quantities2 on periods.period_id=quantities2.period_id
    left outer join quantities1 on periods.period_id=quantities1.period_id)

    select date_trunc('month',local_date) as month, count(period_id) as numperiods,
    sum(import) as total_import,
    sum(export) as total_export
    from fulldata group by month order by month
    '''
    import time
    a = time.time()
    #print(loadDataFromDb(s, returndf=True))
    print(time.time()-a)



    s = '''
    with periods as (select * from sm_periods where local_date between '2020-08-01' and '2020-10-01' )
    , quantities1 as (select period_id, quantity 
    from sm_quantity
    inner join sm_accounts on sm_quantity.account_id=sm_accounts.account_id
    where session_id='39b76afc-118d-40e1-8368-c395fa0926e4' and type_id=0 and active='1')
    , quantities2 as (select period_id, quantity from sm_quantity
    inner join sm_accounts on sm_quantity.account_id=sm_accounts.account_id
    where session_id='39b76afc-118d-40e1-8368-c395fa0926e4' and type_id=2 and active='1')
    , full1 as 
     (select periods.*, quantities1.quantity 
    from periods inner join quantities1 on periods.period_id=quantities1.period_id)
    , full2 as 
     (select periods.*, quantities2.quantity
    from periods inner join quantities2 on periods.period_id=quantities2.period_id)
    , fulldata as
    (select full2.*, coalesce(full1.quantity,0) as import, full2.quantity as export
    from full2 full outer join full1 on full2.period_id=full1.period_id)

    select date_trunc('month',local_date) as month, count(period_id) as numperiods,
    sum(import) as total_import,
    sum(export) as total_export
    from fulldata group by month order by month
    '''

    import time
    a = time.time()
    print(loadDataFromDb(s, returndf=True))
    print(time.time()-a)