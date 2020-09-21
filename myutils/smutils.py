import pandas as pd
import numpy as np
import datetime
import requests
import os

from myutils.utils import loadDataFromDb

START = '201901010000'    

def get_type_id(choice):
    endpoints = [choice in [mi[0] for mi in m[1]] for m in menuitems]
    if sum(endpoints)==0:
        return -1, 'Invalid Page'
    type_id = endpoints.index(True)
    type_label = ['Electricity', 'Gas', 'Electricity Export'][menuitems[type_id][0]]
    return type_id, type_label


def getmode(smid):
        s = f"select type_id from sm_accounts where session_id = '{smid}' and active='1'"
        type_ids = loadDataFromDb(s)    
        mode = ['0']*3
        for j in type_ids:
            mode[j[0]] = '1'
        mode[0] = '1'    
        return ''.join(mode)

def create_sm_page(request, content, heading, title=None):
    with open(os.path.dirname(os.path.realpath(__file__)) + "/base_template.html", "r") as f:
        template = f.read()
    if title is not None:
        template = template.replace('<title>Smart Meter Reports</title>','<title>{}</title>'.format(title))
    
    s = """
    <div class="inner">
    <main role="main" class="col-md-9 ml-sm-auto col-lg-10 px-md-4">
    <div class="d-flex justify-content-between flex-wrap flex-md-nowrap align-items-center pt-3 pb-2 mb-3 border-bottom">            
            <H1 class="h2">{}</H1>
    </div>
    """
    s = s.format(heading)
    homeurl, sdbar = sidebarhtml(request)
    output = template.replace('{content}', s+content).replace('{homeurl}',homeurl).replace('{sidebarhtml}', sdbar)
    return output



def getTariff(request, choice):
    if choice=='cost':
        s1 = """
                <P>In order to calculate the cost of your electricity, you need to enter your tariff. This can be a fixed price. To use a fixed price, 
                include a parameter like &tariff=10.3. This tariff is assumed to be in p/kwh, including VAT, excluding any standing charges.</P>

            <P>You can also choose a time-varying tariff, although at this point I have only added the data for Octopus Agile 
            and GO and Tracker for electricity (AGILE-18-02-21, GO-18-06-12, SILVER-2017-1). Next on my list to add are Octopus's Go Faster tariffs, and their Gas Tracker. 
            Let me know if there are time-varying 
            tariffs from other suppliers you would find useful for me to add.
            To use this, include the parameter &tariff=AGILE-18-02-21&amp;region=C. 
            <P>If you aren't sure what region you are in, you can find these in the following table:</P>
            <TABLE><TR><TH>Region</TH><TH>MPAN Number</TH><TH>Region Name</TH></TR>
            """
        for _, j in getregions().iterrows():
            s1 += '<TR><TD>{}</TD><TD>{}</TD><TD>{}</TD></TR>'.format(j.letter, j.number, j['name'])
        s1 += '</TABLE><BR><BR>'


    elif choice == 'gascost':
        s1 = """
            <P>In order to calculate the cost of your gas, you need to enter your tariff. This can be a fixed price. To use a fixed price, 
                include a parameter like &gastariff=10.3. This tariff is assumed to be in p/kwh, including VAT, excluding any standing charges.</P>

            <P>I have also added teh ability to use a time-varying tariff, for example &gastariff=SILVER-2017-1. If you do this, you will need to include 
            your region code. If you aren't sure what region you are in, you can find these in the following table:</P>
            <TABLE><TR><TH>Region</TH><TH>MPAN Number</TH><TH>Region Name</TH></TR>
            """
        for _, j in getregions().iterrows():
            s1 += '<TR><TD>{}</TD><TD>{}</TD><TD>{}</TD></TR>'.format(j.letter, j.number, j['name'])
        s1 += '</TABLE><BR><BR>'
        
    elif choice == 'exportrevenue':
        s1 = """
            <P>In order to calculate the revenue from your electricity exports, you need to enter your export tariff. This can be a fixed price. To use a fixed price, 
                include a parameter like &exporttariff=10.3. This tariff is assumed to be in p/kwh.</P>

            <P>I have also added the ability to use a time-varying tariff, for example &exporttariff=AGILE-OUTGOING-19-05-13. If you do this, you will need to include
            your region code. If you aren't sure what region you are in, you can find these in the following table:</P>
            <TABLE><TR><TH>Region</TH><TH>MPAN Number</TH><TH>Region Name</TH></TR>
            """
        for _, j in getregions().iterrows():
            s1 += '<TR><TD>{}</TD><TD>{}</TD><TD>{}</TD></TR>'.format(j.letter, j.number, j['name'])
        s1 += '</TABLE><BR><BR>'


    output = create_sm_page(request, s1, 'Tariffs')
    return output


def n3rgyrequest(url, key):
    headers = {'Authorization': key}
    try:
        r = requests.get(url=url, headers=headers )
        r = r.json()
    except Exception:
        raise Exception('Problem loading data from n3rgy: {}, {}'.format(url, r))
    return r

def n3adjtime(s, n3adj):
    return (pd.Timestamp(s)-pd.offsets.Minute(n3adj*30)).strftime('%Y%m%d%H%M')

def n3rgymeters(key, n3adj):       
    meters = []        
    for type_id in [0,1,2]:
        if type_id==0:
            url = "https://consumer-api.data.n3rgy.com/electricity/consumption/1/"
        elif type_id==1:
            url = "https://consumer-api.data.n3rgy.com/gas/consumption/1/"
        elif type_id==2:
            url = "https://consumer-api.data.n3rgy.com/electricity/production/1/"     
        headers = {'Authorization': key}
    
        r = requests.get(url=url, headers=headers )
        if r.status_code==403:
            return r.status_code, r.json() 
        if (r.status_code==200):       
            if 'end' in r.json()['availableCacheRange']:
                lastdatetime = r.json()['availableCacheRange']['end']
                if n3adj!=0:
                    lastdatetime = n3adjtime(lastdatetime, n3adj)
                meters.append([type_id, r.status_code, lastdatetime])    
            else:
                meters.append([type_id, 404, 'no data'])    
        else:
            meters.append([type_id, r.status_code, r.json()])
    return meters


def latesttariff(mp):
    d = pd.DataFrame(mp['agreements'])
    d = d.sort_values('valid_to', ascending=False).tariff_code.iloc[0]
    return d

def gettariffdetails(tariff):
    register = tariff[:4]
    product = tariff[5:-2]
    region = tariff[-1]
    
    url = f"https://api.octopus.energy/v1/products/{product}/"
    r3 = requests.get(url).json()
    
    if register=='E-1R':
        if 'single_register_electricity_tariffs' in r3:
            rec = r3['single_register_electricity_tariffs'][f'_{region}']['direct_debit_monthly']
            url2 = f'https://api.octopus.energy/v1/products/{product}/electricity-tariffs/{tariff}/standard-unit-rates/'
            if requests.get(url2).json()['count']>5:
                results = {x[:-8]: rec[x] for x in ['standing_charge_inc_vat']}
            else:
                results = {x[:-8]: rec[x] for x in ['standing_charge_inc_vat', 'standard_unit_rate_inc_vat']} 
        else:
            results = 'not available'
    elif register == 'G-1R':
        if 'single_register_gas_tariffs' in r3:
            rec = r3['single_register_gas_tariffs'][f'_{region}']['direct_debit_monthly']
            results = {x[:-8]: rec[x] for x in ['standing_charge_inc_vat', 'standard_unit_rate_inc_vat']} 
        else:
            results = 'not available'
    elif register == 'E-2R': 
        if 'dual_register_electricity_tariffs' in r3:
            rec = r3['dual_register_electricity_tariffs'][f'_{region}']['direct_debit_monthly']
            results = {x[:-8]: rec[x] for x in ['standing_charge_inc_vat', 'day_unit_rate_inc_vat', 'night_unit_rate_inc_vat']} 
        else:
            results = 'not available'
    return [tariff, product, region, results]


def octopusmeters(key, getprices=False):
    url =   'https://api.octopus.energy/v1/accounts/' + key[:10]
    r = requests.get(url, auth=(key[10:],''))   
    if r.status_code!=200:
        return r.status_code, r.json()['detail']
    meters = []
    for commod, mpan in [('electricity','mpan'),('gas','mprn')]:
        for x in r.json()['properties']:
            if x['moved_out_at'] is None:
                points = x[f'{commod}_meter_points']
                for i in points:
                    for j in i['meters']:
                        url = f"https://api.octopus.energy/v1/{commod}-meter-points/{i[mpan]}/meters/{j['serial_number']}/consumption/"
                        url += '?period_from=2019-01-01T00:00:00&period_to=2021-01-01T00:00:00&page_size=1'
                        r2 = requests.get(url, auth=(key[10:],'')).json()
                        if len(r2.get('results', [])):
                            lateststartdate = r2['results'][0]['interval_start']
                            tariff = latesttariff(i)
                            if commod=='electricity':
                                if 'OUTGOING' in tariff:
                                    type_id=2
                                else:
                                    type_id=0
                            elif commod=='gas':
                                type_id=1
                            if getprices:
                                p = gettariffdetails(tariff)
                            else:
                                p = [tariff]
                            meters.append([type_id, i[mpan], j['serial_number'], 
                                    lateststartdate] + p) 

    cols = ['type_id','mpan','serial','laststart','tariff']
    if getprices:
        cols += ['product','region','prices']
    meters = pd.DataFrame(meters, columns=cols)
    meters.sort_values('type_id', inplace=True)    
    a = pd.DatetimeIndex(meters.laststart.str[:16])
    b = pd.TimedeltaIndex(np.where(meters.laststart.str.len()==25,1,0), 
                              unit='h')
    meters.laststart = a-b     
    return meters

def octopusconsumptionformpan(key, mpan, meter, type):
    import numpy as np
    dfs = []
    url = 'https://api.octopus.energy/v1/{}-meter-points/{}/meters/{}/consumption/?period_from=2019-01-01T00:00:00&period_to=2021-01-01T00:00:00&page_size=10000'
    url = url.format(type, mpan, meter)
    r = requests.get(url, auth=(key[10:],''))
    if len(r.json().get('results',[])):
        dfs.append(pd.DataFrame(r.json()['results']))
        while r.json()['next'] is not None:
            r = requests.get(r.json()['next'], auth=(key[10:],''))
            dfs.append(pd.DataFrame(r.json()['results']))
            if len(dfs)>30:
                raise Exception
        dfs = pd.concat(dfs)    
        a = pd.DatetimeIndex(dfs.interval_start.str[:16])
        b = pd.TimedeltaIndex(np.where(dfs.interval_start.str.len()==25,1,0), 
                              unit='h')
        dfs['timestamp'] = a-b
        dfs = dfs[['timestamp','consumption']].reset_index()
        return dfs



def octopusconsumption(key, type_id, first=None, last=None):
    url =   'https://api.octopus.energy/v1/accounts/' + key[:10]
    r = requests.get(url, auth=(key[10:],''))   
    dfs = None 
    if type_id in [0,2]:
        meters = []
        for p in r.json()['properties']:
            if p['moved_out_at'] is None:
                for i in p['electricity_meter_points']:
                    for j in i['meters']:
                        if len(j['serial_number'])>0:
                            meters.append([i['mpan'], j['serial_number'], latesttariff(i)])                
        if type_id==0:
            meters = [e for e in meters if 'OUTGOING' not in e[2]]   
        elif type_id==2:
            meters = [e for e in meters if 'OUTGOING' in e[2]]   
        for e in meters:
            dfs = octopusconsumptionformpan(key, e[0], e[1], 'electricity')
            if dfs is not None:
                break
    elif type_id == 1:
        meters = []
        for p in r.json()['properties']:
            for i in p['gas_meter_points']:
                for j in i['meters']:
                    if len(j['serial_number'])>0:
                        meters.append([i['mprn'], j['serial_number'], latesttariff(i)])      
        for e in meters:
            dfs = octopusconsumptionformpan(key, e[0], e[1], 'gas')
            if dfs is not None:
                break     
    else:
        raise Exception('Not implemented yet for type_id {}'.format(type_id))        
    
    if dfs is None:
        return None, None
    
    region = e[2][-1]
    idx = pd.date_range(START, '202101010000', freq='30T')  
    df = pd.DataFrame()
    df['timestamp'] = idx
    df = pd.DataFrame(idx, columns=['timestamp'])
    dfs = df.merge(right=dfs, on='timestamp', how='left')
    dfs = dfs[dfs.consumption.notna()]
    dfs = dfs[['timestamp','consumption']]
    dfs.columns=['timestamp','value']
    return dfs, region



def getDataFromN3RGY(key, type_id, n3adj, first=None, last=None):    
    if type_id==0:
        url = "https://consumer-api.data.n3rgy.com/electricity/consumption/1/"
    elif type_id==1:
        url = "https://consumer-api.data.n3rgy.com/gas/consumption/1/"
    elif type_id==2:
        url = "https://consumer-api.data.n3rgy.com/electricity/production/1/"
    else:
        raise Exception('Not implemented for type_id {}'.format(type_id))
    if first:
        if last:
            daterange = (first, last)
        else:
            last = datetime.datetime.now() + datetime.timedelta(days=1)
            daterange = (first, last.strftime('%Y%m%d%H%M'))
    else:
        r = n3rgyrequest(url, key)
        if 'availableCacheRange' not in r.keys():
            raise Exception('Problem loading data from n3rgy: {}, {}'.format(url, r))
        daterange = r['availableCacheRange']
        daterange = (n3adjtime(daterange['start'], n3adj), n3adjtime(daterange['end'], n3adj))


    
    idx = pd.date_range(START, daterange[1], freq='30T')  
    df = pd.DataFrame()
    df['timestamp'] = idx
    df = pd.DataFrame(idx, columns=['timestamp'])
    
    dfs = []
    start = max(daterange[0],START)
    
    while start<daterange[1]:
        end = (pd.Timestamp(start) + 
                 pd.offsets.Day(90)).strftime('%Y%m%d%H%M')
        end = min(end, daterange[1])
        if n3adj==0:
            api_url = url + '?start={}&end={}'.format(start, end)
        else:
            api_url = url + '?start={}&end={}'.format(n3adjtime(start,-1*n3adj), n3adjtime(end, -1*n3adj))
        r = n3rgyrequest(api_url, key)
        df2 = pd.DataFrame(r['values'])
        dfs.append(df2)
        start = end[:-1] + '1'
    dfs = pd.concat(dfs)
    dfs['timestamp'] = pd.DatetimeIndex(dfs['timestamp'])
    if n3adj!=0:
        dfs['timestamp'] = dfs['timestamp'] - pd.offsets.Minute(30*n3adj) 


    df = df.merge(right=dfs, on='timestamp', how='left')
    
    test = df[df.timestamp>pd.Timestamp(daterange[0])]
    test[test.value.isna()]
    test = test[test.value.isna()]
    if test.shape[0]>0:
        print('Missing records: {}'.format(test.timestamp.tolist() ))
    
    df = df[df.value.notna()]
    return df



def deleteSmData(smid):
    s = f"""
    update sm_accounts set active='0' where session_id='{smid}';
    delete from sm_quantity 
    where account_id not in 
       (select account_id from sm_accounts where active='1');
    """
    _ = loadDataFromDb(s)

def isdemo(request):
    key = request.GET.get('octopus', request.GET.get('n3rgy',None))
    return (key is None)


def get_sm_id(request, createifnone=False):
    key = request.GET.get('octopus', request.GET.get('n3rgy',None))
    demokey = 'e4280c7d-9d06-4bbe-87b4-f9e106ede788'
    if (key is None) or (key[-6:] == 'RMePtm'):
        return demokey
    if f'sm_id_{key[-3:]}' in request.COOKIES:
        return request.COOKIES[f'sm_id_{key[-3:]}']
    if createifnone:
        import uuid
        return str(uuid.uuid4())


def loadSmData(request, type_id):
    smid = get_sm_id(request, createifnone=True)
    if ('n3rgy' in request.GET):
        key = request.GET.get('n3rgy')
        region = None
        n3adj = int(request.GET.get('n3adj','1'))
        df = getDataFromN3RGY(key, type_id, n3adj)
        source_id=0
    elif 'octopus' in request.GET:
        key = request.GET.get('octopus')
        df, region = octopusconsumption(key, type_id)
        source_id=1
    else:
        raise Exception('MAC, n3rgy or octopus keys are not provided')
    if (df is None) or (df.shape[0]==0):
        estr = 'No {} data retrieved from {} - go back to Admin page, check key and try again.'
        estr = estr.format(['Electricity Consumption', 'Gas Consumption','Export'][type_id],
                            ['n3rgy', 'Octopus'][source_id])
        raise Exception(estr)

    s = f"select * from sm_accounts where session_id='{smid}' and type_id={type_id} and active='1' limit 1"
    accounts = loadDataFromDb(s, returndf=True)
    if len(accounts):
        account_id = accounts['account_id'].values[0]
        s = "delete from sm_quantity where account_id={}".format(account_id)
        _ = loadDataFromDb(s)
        s = """
        update sm_accounts
        set first_period='{}', last_period='{}', last_updated=CURRENT_TIMESTAMP, session_id='{}'
        where account_id={} and type_id={}  and active='1'; 
        """
        s = s.format(df['timestamp'].iloc[0].strftime('%Y-%m-%d %H:%M'),
                     df['timestamp'].iloc[-1].strftime('%Y-%m-%d %H:%M'), 
                     smid, account_id, type_id)
        _ = loadDataFromDb(s)
    else:
        if region is not None:
            region = "'{}'".format(region)
        else:
            region = 'Null'
        s = """
        insert into sm_accounts (type_id, first_period, last_period, last_updated, source_id, region, session_id, active)
        values ({}, '{}', '{}', CURRENT_TIMESTAMP, {}, {}, '{}', '1') returning account_id;"""
        s = s.format(type_id,
                    df['timestamp'].iloc[0].strftime('%Y-%m-%d %H:%M'),
                    df['timestamp'].iloc[-1].strftime('%Y-%m-%d %H:%M'),
                    source_id, region, smid )
        account_id = loadDataFromDb(s)
        account_id = account_id[0][0]
    s = """
    insert into sm_quantity (account_id, period_id, quantity) 
    VALUES
    """
    for i, j in df.iterrows():
        s+= ' ({}, {}, {}),'.format(account_id, i, j.value)
    s = s[:-1] + ';'
    _ = loadDataFromDb(s)

    return smid


def quantitystr(smid, type_id):
    return f"""select period_id, quantity from sm_quantity 
            inner join sm_accounts on sm_quantity.account_id=sm_accounts.account_id  
                where session_id='{smid}' and type_id={type_id} and active='1'"""

def parsetariff(request, tariff, vat, **kwargs):
    isfixed = tariff.replace(',','').replace(':','').replace('-','').replace('.','').isnumeric()
    if isfixed:
        t = tariff.split(',')
        if len(t)>1:
            pricestr = 'CASE '
            for opt in t:
                if ':' in opt:
                    [s, p] = opt.split(':')
                    [s, e] = [x[:2] + ':' + x[-2:] for x in s.split('-')]        
                    if s<e:
                        pricestr += f"WHEN local_time>='{s}' and local_time<'{e}' then {float(p)/vat} "
                    else:
                        pricestr += f"WHEN local_time>='{s}' or local_time<'{e}' then {float(p)/vat} "            
                else:
                    pricestr += f' else {float(opt)/vat} end as value'
        else:
            pricestr = f' {float(t[0])/vat} as value'
        pricestr = f"select period_id, {pricestr} from periods"
        return isfixed, pricestr
    else:
        region = kwargs.get('region', None) or request.GET.get('region')
        s = f"select var_id, granularity_id from sm_variables where product='{tariff}' and region='{region}'"
        s = loadDataFromDb(s)
        if len(s):
            var_id = s[0][0]
            granularity_id = s[0][1]
        else:
            raise Exception("No data for tariff {} and region {}".format(tariff, region))
        if granularity_id==0:
            pricestr = f"select period_id, value from sm_hh_variable_vals v where v.var_id={var_id} "
        elif granularity_id==1:
            pricestr = f"""
                    select period_id, value from periods 
                    inner join sm_d_variable_vals v on periods.local_date=v.local_date 
                    where v.var_id={var_id}"""
        return isfixed, pricestr


def adj_url(url, remove, change):
    import re
    for k in remove:
        p = re.compile('[&\?]' + k + '[^&]*')
        r = p.search(url)
        if r is not None:
            url = url.replace(r.group(), '')
    for k in change:
        p = re.compile('[&\?]' + k[0] + '[^&]*')
        r = p.search(url)
        if r is not None:
            if k[1] is None:
                url = url.replace(r.group(), '&{}'.format(k[0]) )
            else:
                url = url.replace(r.group(), '&{}={}'.format(k[0],k[1]))
        else:
            if k[1] is None:
                url += '&{}'.format(k[0])
            else:
                url += '&{}={}'.format(k[0],k[1])
    if '?' not in url:
        url = url.replace('&','?', 1)
    return url


menuitems = [(0,[('consumption','Consumption'), ('cost','Cost'), ('bills', 'Bill Calculator'), ('compare','Compare Tariffs'), ('analysis','Profile Analysis'), ('emissions', 'Emissions'), ('savecsv', 'Save to CSV')]),
                (1, [('gasconsumption','Consumption'), ('gascost','Cost'), ('gasbills','Bill Calculator'), ('gascompare','Compare Tariffs'), ('gassavecsv', 'Save to CSV')]),
                (2, [('export', 'Export Quantities'), ('netimport', 'Net Import'), ('exportrevenue','Revenue'), ('exportbills', 'Bill Calculator'), ('exportcompare', 'Compare Tariffs'), ('exportsavecsv','Save to CSV')])  ]


def sm_log(request, choice, smid=None):
    url = request.get_full_path()
    method = 0

    if request.method=='POST':
        method=1
        tasks = list(request.POST.keys())
        if len(tasks):
            if (smid is None) and ('load' in tasks[0]):
                return None
            else:
                url = url + '&task=' + tasks[0]

    if 'octopus' in request.GET:
        url = adj_url(url, [], [('octopus','')])
    elif 'n3rgy' in request.GET:
        url = adj_url(url, [], [('n3rgy','')])

    if smid is None:
        if isdemo(request):
            smid = None
        else:
            smid = get_sm_id(request)

    if smid is None:
        smid = 'Null'
    else:
        smid = f"'{smid}'"

    s = f"""
    insert into sm_log (datetime, choice, method, session_id, url) values 
    (CURRENT_TIMESTAMP, '{choice}', {method}, {smid}, '{url[:120]}');
    """
    loadDataFromDb(s)


def sidebarhtml(request):
    mode = request.GET.get('mode','100')
    url = request.get_full_path()
    current = request.path_info.split('/')[-1]
    if mode[2]=='1':
        menulabels = ['Electricity Import', 'Gas', 'Electricity Export']
    else:
        menulabels = ['Electricity', 'Gas','Generation/Export']

    s = """
        <nav id="sidebarMenu" class="col-md-3 col-lg-2 d-md-block bg-light sidebar collapse">
          <div class="sidebar-sticky pt-3">
       """
    for commod in [0,2,1]:
        if mode[commod]=='1':
            if current in [x[0] for x in menuitems[commod][1]]:
                s1 = """
                <a class="nav-link  active big bg-light" data-toggle="collapse" href="#collapse{commod}" role="button" 
                                aria-expanded="false" aria-controls="collapse{commod}" >{commodlabel}</a>
                      <div class="collapse show" id="collapse{commod}">
                    """
            elif (commod==0) and (current in ['admin','home','info', 'getstarting']):
                 s1 = """
                <a class="nav-link  big bg-light" data-toggle="collapse" href="#collapse{commod}" role="button" 
                                aria-expanded="false" aria-controls="collapse{commod}" >{commodlabel}</a>
                      <div class="collapse show" id="collapse{commod}">
                    """               
            else:
                s1 = """
                <a class="nav-link  big bg-light" data-toggle="collapse" href="#collapse{commod}" role="button" 
                                aria-expanded="false" aria-controls="collapse{commod}" >{commodlabel}</a>
                      <div class="collapse" id="collapse{commod}">
                    """
            s1 = s1.format(commod=commod, commodlabel=menulabels[commod])
            for x in menuitems[commod][1]:
                if current == x[0]:
                    s1 += '<a class="nav-link active bg-light" href="" >&nbsp;&nbsp;&nbsp;&nbsp;{}</a>'.format(x[1])
                else:
                    s1 += '<a class="nav-link bg-light" href="{}" >&nbsp;&nbsp;&nbsp;&nbsp;{}</a>'.format(url.replace(current, x[0], 1), x[1])
            s += s1 + '</div>'

    for item in [('admin', 'Admin'), ('getstarting', 'Getting Started'), ('other', 'Other Links')]:
        if current == item[0]:
            s1 = """
                <a class="nav-link  active big bg-light" >{commodlabel}</a>
                """ 
        else:
            s1 = """
                <a class="nav-link  big bg-light" href="{url}" >{commodlabel}</a>
                    """
        s += s1.format(commod=item[0], commodlabel=item[1], url=url.replace(current, item[0], 1))
    s += """
        </div>
    </nav>"""    
    return url.replace(current, 'home', 1), s            
                
                

regions = ['Eastern England', 'East Midlands', 'London', 'Merseyside and Northern Wales',
           'West Midlands', 'North Eastern England', 'North Western England', 'Northern Scotland',
           'Southern Scotland', 'South Eastern England', 'Southern England', 'Southern Wales',
           'South Western England', 'Yorkshire']

regionletters = ['A','B','C','D','E','F','G','P','N','J','H','K','L','M']

def getregions():
    df = pd.DataFrame()
    df['letter'] = regionletters
    df['name'] = regions
    df['number'] = df.index+10
    df.sort_values('letter', inplace=True)
    return df

def getRegion(code):
    if isinstance(code, str):
        if len(code)==1:
            letter = code
            num = regionletters.index(letter)+10
            region = regions[num-10]
        elif len(code)==2:
            assert code.isnumeric()
            code = int(code)
        elif len(code)>2:
            region = code
            num = regions.index(region)+10
            letter = regionletters[code-10]
    else:
        assert isinstance(code, int)
        assert code>=10
        region = regions[code-10]
        letter = regionletters[code-10]
        num = code
    return letter, num, region