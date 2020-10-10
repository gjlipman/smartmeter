from django.http import HttpResponse, HttpRequest
from django.shortcuts import render, redirect
import os
import sys
import pandas as pd
import numpy as np
import datetime

from myutils.utils import (getConnection, loadDataFromDb )
from myutils.smutils import (adj_url, sidebarhtml, getregions, get_sm_id, 
                            quantitystr, parsetariff, create_sm_page, get_type_id)


def getregionselector(region):
    df = getregions()
    if region == None:
        s = '<option selected="true"></option>'
    else:
        s = '<option></option>'
    for _, j in df.iterrows():
        match = ' selected="true"' if region == j.letter else ''
        s += '<option value="{}" {}>{}: {}</option>'.format(j.letter, match, j.letter, j['name'])
    return s

def gettariffselector(type_id, tariff, price):
    if price is None:
        if tariff.replace('.','').replace('-','').isnumeric():
            price = float(tariff)
            tariff = 'Fixed'
        else:
            price = 0.0

    if type_id == 0:
        tariffs =   ['Fixed','AGILE-18-02-21', 'GO-18-06-12', 'SILVER-2017-1']    
    elif type_id == 2:
        tariffs = ['Fixed', 'AGILE-OUTGOING-19-05-13']
    else:
        tariffs = ['Fixed', 'SILVER-2017-1']

    tariffid = tariffs.index(tariff) if tariff in tariffs else 0
    s = ''
    for i, j in enumerate(tariffs):
        match = ' selected="True"' if i==tariffid else 0
        s += '<option value="{}"{}>{}</option>'.format(j, match, j) 

    if tariffid==0:
        pricedisplay = 'flex'
        regiondisplay = 'none'
    else:
        pricedisplay = 'none'
        regiondisplay = 'flex'
    return s, price, pricedisplay, regiondisplay

def calculatebill(choice, request):
    type_id, type_label = get_type_id(choice)
    vat = 1 if type_id==2 else 1.05
    standingcharge = float(request.POST.get('standingcharge'))
    standingcharge /= vat

    start = request.POST.get('startdate')
    end = request.POST.get('enddate')     
    numdays = (pd.Timestamp(end)-pd.Timestamp(start)).days+1
    smid = get_sm_id(request)

    tariff = request.POST.get('tariff')
    if tariff=='Fixed':
        tariff = request.POST.get('price')

    isfixed, pricestr = parsetariff(request, tariff, vat, region=request.POST.get('region',None)) 
    qstr = quantitystr(smid, type_id)
    
    endstr = f"""
        select value price, count(period_id) as numperiods, 
        sum(quantity) as total_quantity, sum(quantity*value) as total_cost  
        from fulldata group by value order by value"""

    s = f'''
    with periods as (select * from sm_periods where local_date between '{start}' and '{end}' )
    , prices as ({pricestr} )
    , quantities as ({qstr})
    , fulldata as 
        (select periods.*, quantities.quantity, prices.value 
        from periods inner join quantities on periods.period_id=quantities.period_id
        inner join prices on periods.period_id=prices.period_id)
       {endstr}
    ''' 
    dfbyprice = loadDataFromDb(s, returndf=True)
    if len(dfbyprice)==0 or dfbyprice.sum().total_quantity==0:
        s+= '<P><B>There is no consumption data available during this bill period.</B>'
        return s    

    if type_id==1:
        gasmult = float(request.POST.get('gasmult', '1'))
        dfbyprice['total_quantity']*=gasmult
        dfbyprice['total_cost'] *= gasmult

    dfbyprice.total_cost /= 100
    df = dfbyprice.sum()
    df['price'] = 100*df.total_cost/df.total_quantity
    
    s = f"""
    with p as (select period, period_id from sm_periods where local_date between '{start}' and '{end}' ), 
    q as ({qstr})
    select count(p.period) from p 
    left join q on q.period_id=p.period_id
    where q.quantity is null;
    """
    df_gaps = loadDataFromDb(s)[0][0]

    s = ''

    if df_gaps:
        urladmin = request.get_full_path().replace('bills','admin', 1)
        s+= f'<P><B>Note that there are {df_gaps} half hours of consumption data missing during this bill period, so the bill will be incorrect.</B>. '
        s += f'Check the <A HREF="{urladmin}">Admin Page</A> for details of the gaps.'
    else:
        s+= '<P>All data covering this bill period has been received.'

    if type_id==1:
        if gasmult == 1:
            gaswarn = '<BR><P><B>These results do not include any <A HREF="https://www.theenergyshop.com/guides/how-to-convert-gas-units-to-kwh" target="_blank">gas conversion from m3 to kwh</A>. If your meter is SMETS2, you should most likely include a parameter gasmult=11.18 or thereabouts.</B>'
        elif 10<gasmult<12:    
                gaswarn = f'<BR><P>These results are based on a gas conversion of {gasmult}kwh per m3. This factor can be adjusted in the url. This conversion should not be applied if your meter is SMETS1.</P>'
        else:
                gaswarn = f'<BR><B>These results are based on a gas conversion of {gasmult}kwh per m3. This factor appears wrong.</B> It <A HREF="https://www.theenergyshop.com/guides/how-to-convert-gas-units-to-kwh" target="_blank">should be around 11.18</A>, based on a volume correction factor of 1.02264, a calorific value of about 40, and dividing by the kwh to joule conversion of 3.6. Your latest bill should show the applicable conversions.'
        s += gaswarn

    s += '''<P>There are many valid ways of rounding half-hourly consumption and costs to reach the total. As a result, you should not expect 
             the total from this calculation to match exactly the consumption or total cost on your bill - they can be a few percent out. 
             However, if it is out by more than this, it may indicate a problem (either with your bill or with this calculator).
             '''
    s += f'<H2>{type_label} Bill</H2>'
    s += f'<P>From {start} to {end}</P>'

    if type_id in [0,1]:
        s += '<H3>VAT Exclusive Method</H3>'
        s += "<TABLE>"
        if 1<len(dfbyprice)<5:
            for _, j in dfbyprice.iterrows():
                s += '<TR><TH></TH><TD>{:.1f} kwh</TD><TD>{:.3f}p/kwh</TD><TD>£{:.2f}</TD></TR>'.format(j.total_quantity, j.price, j.total_cost)

        s += '<TR><TH>Energy</TH><TD>{:.1f} kwh</TD><TD>{:.3f}p/kwh</TD><TD>£{:.2f}</TD></TR>'.format(df.total_quantity, df.price, df.total_cost)
        sc = numdays*standingcharge/100
        s += '<TR><TH>Standing Charge</TH><TD>{} days</TD><TD>{:.2f} p/day</TD><TD>£{:.2f}</TD></TR>'.format(numdays, standingcharge, sc)
        s += '<TR><TH>VAT@5%</TH><TD></TD><TD></TD><TD>£{:.2f}</TH></TR>'.format((sc+df.total_cost)*0.05)
        s += '<TR><TH>Total</TH><TD></TD><TD></TD><TH>£{:.2f}</TH></TR>'.format((sc+df.total_cost)*1.05)
        s += '</TABLE><BR><H3>VAT Inclusive Method</H3><TABLE>'
        if 1<len(dfbyprice)<5:
            for _, j in dfbyprice.iterrows():
                s += '<TR><TH></TH><TD>{:.1f} kwh</TD><TD>{:.3f}p/kwh</TD><TD>£{:.2f}</TD></TR>'.format(j.total_quantity, 1.05*float(j.price), 1.05*float(j.total_cost))    
        s += '<TR><TH>Energy</TH><TD>{:.1f} kwh</TD><TD>{:.3f}p/kwh</TD><TD>£{:.2f}</TD></TR>'.format(df.total_quantity, df.price*1.05, df.total_cost*1.05)
        sc = numdays*standingcharge/100
        s += '<TR><TH>Standing Charge</TH><TD>{} days</TD><TD>{:.2f} p/day</TD><TD>£{:.2f}</TD></TR>'.format(numdays, standingcharge*1.05, sc*1.05)
        s += '<TR><TH>Total</TH><TD></TD><TD></TD><TH>£{:.2f}</TH></TR>'.format((sc+df.total_cost)*1.05)
        s += '</TABLE><BR>'    
    else:
        s += "<TABLE>"
        if 1<len(dfbyprice)<5:
            for _, j in dfbyprice.iterrows():
                s += '<TR><TH></TH><TD>{:.1f} kwh</TD><TD>{:.3f} p/kwh</TD><TD>£{:.2f}</TD></TR>'.format(j.total_quantity, j.price, j.total_cost)
        s += '<TR><TH>Energy</TH><TD>{:.1f} kwh</TD><TD>{:.3f} p/kwh</TD><TD>£{:.2f}</TD></TR>'.format(df.total_quantity, df.price, df.total_cost)
        s += '</TABLE><BR>'            
    return s

def billsPage(request, choice):
    type_id, type_label = get_type_id(choice)
    heading = '{} Bill Calculator'.format(type_label)
    prefix = ['', 'gas', 'export'][type_id]
    tariff = request.GET.get(f'{prefix}tariff', '0.0')
    standingcharge = request.GET.get(f'{prefix}sc','0.0')
    price = None
    region = request.GET.get('region', None)
    gasmult = request.GET.get('gasmult','1.0')
    start = '2020/07/01'
    end = '2020/07/31'
    s2 = ''

    if request.method=='POST':
        region = request.POST.get('region')
        tariff = request.POST.get('tariff')
        price = request.POST.get('price')
        standingcharge = request.POST.get('standingcharge')
        gasmult = request.POST.get('gasmult', '1.0')
        s2 = calculatebill(choice, request)
        start = request.POST.get('startdate')
        end = request.POST.get('enddate')    


    regionselector = getregionselector(region)
    tariffselector, price, pricedisplay, regiondisplay = gettariffselector(type_id, tariff, price)
    VAT = ', incl VAT' if type_id in [0,1] else ''



    s = """
    <form action="{url}" method="post">
 
    <div class="form-group row" id="electariff">
    <label for="inputEmail3" class="col-sm-2 col-form-label" >Tariff</label>
    <div class="col-sm-10">
        <select class="form-control" id="tariffselect" name="tariff" select onchange="JavaScript: showForm2( this.value );">
		{tariffselector}
        </select>
    </div>
    </div>

    <div class="form-group row" id="price" style="display:{pricedisplay};">
    <label for="inputEmail3" class="col-sm-2 col-form-label">Price (p/kwh{VAT})</label>
    <div class="col-sm-10">
      <input type="text" class="form-control" name="price" value="{price}">
    </div>
    </div>

    <div class="form-group row" id="region" style="display:{regiondisplay};">
    <label for="inputEmail3" class="col-sm-2 col-form-label">Region (for time-varying tariffs)</label>
    <div class="col-sm-10">
        <select class="form-control" name="region">
        {regionselector}
        </select>
    </div>
    </div>

    <div class="form-group row" id="gasmult" style="display:{gasmultdisplay};">
    <label for="inputEmail3" class="col-sm-2 col-form-label">Gas Multiplier for SMETS2</label>
    <div class="col-sm-10">
      <input type="text" class="form-control" name="gasmult" value="{gasmult}">
    </div>
    </div>

    <div class="form-group row">
    <label for="inputEmail3" class="col-sm-2 col-form-label">Standing Charge (p/day{VAT})</label>
    <div class="col-sm-10">
      <input type="string" class="form-control" name="standingcharge" value="{standingcharge}">
    </div>
    </div>

    <div class="form-group row">
    <label for="inputEmail3" class="col-sm-2 col-form-label">Start Date (yyyy/mm/dd)</label>
    <div class="col-sm-10">
      <input type="string" class="form-control" name="startdate" value="{start}">
    </div>
    </div>
 
    <div class="form-group row">
    <label for="inputEmail3" class="col-sm-2 col-form-label">End Date (yyyy/mm/dd)</label>
    <div class="col-sm-10">
      <input type="text" class="form-control" name="enddate" value="{end}">
    </div>
    </div>

    <div class="form-group row">
      <div class="col-sm-10">
        <button type="submit" class="btn btn-primary">Calculate Bill</button>
      </div>
    </div>
    </form>
    """
    s = s.format(regionselector=regionselector, 
                 tariffselector=tariffselector,
                 price=price, gasmult=gasmult,
                 gasmultdisplay='flex' if type_id==1 else 'none',
                 url=request.get_full_path(), standingcharge=standingcharge,
                 pricedisplay=pricedisplay, regiondisplay=regiondisplay,
                 start=start, end=end, VAT=VAT)


    s+= """
        <script>

            function showForm2( v ) {
                if( v == "Fixed" )
                {
                    document.getElementById( "price" ).style.display = "flex";
                    document.getElementById( "region" ).style.display = "none";}
                else {
                    document.getElementById( "price" ).style.display = "none";
                    document.getElementById( "region" ).style.display = "flex";} 
            }

        </script>
            """





    content = s + s2

    return create_sm_page(request, content, heading)

def memoryPage(request):
    s = ''
    for i, j in request.META.items():
        s += f'<BR>{i}: {j}'
    return s


    import gc
    found_objects = gc.get_objects()
    data = [(sys.getsizeof(obj), str(type(obj)).replace('<','').replace('>',''), repr(obj)[:130]) for obj in found_objects]
    data.sort(reverse=True)
    m = sum([d[0] for d in data])/1000000
    s = f'Total: {m}MB ({len(data)} objects)<BR>' 
    data = [d for d in data if len(d[1])>1]
    s += '<BR>'.join([f'{d[0]}: {d[1]}: {d[2]}' for d in data[:50]])


    




def savetocsv(request, type_id):
    smid = get_sm_id(request)
    option = request.POST.get('option')
    vat = 1 if type_id==2 else 1.05
    includevat = 1 if type_id in [0,1] and request.POST.get('inclvat')=='y' else 0
    
    if option in ['hh_p','hh_q_p', 'd_q_c', 'd_p']:
        tariff = request.POST.get('tariff')
        region = request.POST.get('region', None)
        if tariff=='Fixed':
            tariff = request.POST.get('price')
        isfixed, pricestr = parsetariff(request, tariff, vat, region=region) 
    else:
        pricestr = 'select period_id, 1 as value from periods'

    if option in ['hh_q','hh_q_p', 'd_q', 'd_q_c']:
        qstr = quantitystr(smid, type_id)
        qstr2 = 'quantity, '
    else:
        qstr = 'select period_id, 1 as quantity from periods'

    start = request.POST.get('startdate')
    if start in ['yyyy/mm/dd', '']:
        start = '2019/01/01'
    end = request.POST.get('enddate')    
    if end in ['yyyy/mm/dd','']:
        end = '2021/04/01'

    if option[:2]=='hh':
        cols = {'q': ', quantity ', 'p': ', price', 'q_p': ', quantity, price '}[option[3:]]
        endstr = f'select period, local_date, local_time, timezone_adj {cols}  from fulldata order by period'
    elif option[:2]=='d_':
        cols = {'q': ', sum(quantity) as quantity ', 'p': ', sum(quantity*price)/sum(quantity) as price', 
                'q_c': ', sum(quantity) as quantity, sum(quantity*price) as cost '}[option[2:]]
        endstr = f'''select local_date {cols}
                    from fulldata group by local_date order by local_date'''

    s = f"""
    with periods as (select * from sm_periods where local_date between '{start}' and '{end}' )
       , q as ({qstr})
       , prices as ({pricestr})
       , fulldata as (
           select periods.*, q.quantity, prices.value price
           from periods inner join q on periods.period_id=q.period_id
           inner join prices on periods.period_id=prices.period_id 
       )
       {endstr}
    """

    df = loadDataFromDb(s, returndf=True)
 

    if includevat:
        for col in ['price','cost']:
            if col in df.columns:
                df[col]*=1.05

    for col, dp in [('quantity', 3), ('price', 3), ('cost', 1)]:
        if col in df.columns:
            df[col] = df[col].round(dp)


    df.rename(columns={'period': 'Period_UTC', 'local_date': 'Date_local',
                        'local_time': 'StartTime_local', 'timezone_adj': 'Timezone_Adj',
                        'quantity': 'Quantity (kwh)'}, inplace=True)
    
    if includevat:
        df.rename(columns={'price': 'Price (p/kwh incl VAT)', 'cost': 'Cost (p incl VAT)',
                        }, inplace=True)
    else:
        df.rename(columns={'price': 'Price (p/kwh excl VAT)', 'cost': 'Cost (p excl VAT)',
                        }, inplace=True)
    

    response = HttpResponse(content_type='text/csv')
    filename = ['electricity_consumption.csv', 'gas_consumption.csv', 'electricity_export.csv']
    response['Content-Disposition'] = f'attachment; filename="{filename[type_id]}"'
    df.to_csv(response, index=False)
    return response

def get_savecsvPage(choice, request):
    type_id, type_label = get_type_id(choice)


    if request.method=='POST':
        return savetocsv(request, type_id)
    else:
        url = request.get_full_path()
        heading = 'Save {} Data to CSV'.format(type_label)
        prefix = ['', 'gas', 'export'][type_id]
        tariff = request.GET.get(f'{prefix}tariff', '0.0')
        price = None
        region = request.GET.get('region', None)        
        regionselector = getregionselector(region)
        tariffselector, price, pricedisplay, regiondisplay = gettariffselector(type_id, tariff, price)
        VAT = ', incl VAT' if type_id in [0,1] else ''
        VAT2 = 'block' if type_id in [0,1] else 'none'
        gasmultdisplay = 'flex' if type_id==1 else 'none'
        gasmult = request.GET.get('gasmult','1')

        s = f"""

                <form action="{url}" method="post">
                <div class="form-group row" id="options">
                <label for="inputEmail3" class="col-sm-2 col-form-label" >Options</label>
                <div class="col-sm-10">
                <select class="form-control" id="optionselect" name="option" select onchange="JavaScript: showForm1( this.value );">
                <option value="hh_q">Half Hourly Quantity</option>
                <option value="hh_p">Half Hourly Price</option>
                <option value="hh_q_p">Half Hourly Quantity and Price</option>
                <option value="d_q">Daily Quantity</option>
                <option value="d_q_c">Daily Quantity and Cost</option>
                <option value="d_p">Daily Price</option>
                </select>
                </div>
                </div>
                <div id="tariffs" style="display:none;">
                    <div class="form-group row" id="electariff">
                    <label for="inputEmail3" class="col-sm-2 col-form-label" >Tariff</label>
                    <div class="col-sm-10">
                        <select class="form-control" id="tariffselect" name="tariff" select onchange="JavaScript: showForm2( this.value );">
                    {tariffselector}
                        </select>
                    </div>
                    </div>

                    <div class="form-group row" id="price" style="display:{pricedisplay};">
                    <label for="inputEmail3" class="col-sm-2 col-form-label">Price (p/kwh{VAT})</label>
                    <div class="col-sm-10">
                    <input type="text" class="form-control" name="price" value="{price}">
                    </div>
                    </div>

                    <div class="form-group row" id="region" style="display:{regiondisplay};">
                    <label for="inputEmail3" class="col-sm-2 col-form-label">Region (for time-varying tariffs)</label>
                    <div class="col-sm-10">
                        <select class="form-control" name="region">
                        {regionselector}
                        </select>
                    </div>
                    </div>
                <div style="display:{VAT2};">
                    <div class="form-group row" id="region" style="display:flex;">
                    <label for="inputEmail3" class="col-sm-2 col-form-label">Include VAT in output</label>
                    <div class="col-sm-10">
                        <select class="form-control" name="inclvat">
                        <option value="y" >Yes</option>
                        <option value="n" >No</option>
                        </select>
                    </div>
                    </div>
                </div>
                 </div>

                <div class="form-group row" id="gasmult" style="display:{gasmultdisplay};">
                <label for="inputEmail3" class="col-sm-2 col-form-label">Gas Multiplier for SMETS2</label>
                <div class="col-sm-10">
                <input type="text" class="form-control" name="gasmult" value="{gasmult}">
                </div>
                </div>



                <P>Dates (leave blank for full history):</P>
                <div class="form-group row">
                
                <label for="inputEmail3" class="col-sm-2 col-form-label">Start (yyyy/mm/dd)</label>
                <div class="col-sm-10">
                <input type="string" class="form-control" name="startdate" value="yyyy/mm/dd">
                </div>
                </div>
            
                <div class="form-group row">
                <label for="inputEmail3" class="col-sm-2 col-form-label">End (yyyy/mm/dd)</label>
                <div class="col-sm-10">
                <input type="text" class="form-control" name="enddate" value="yyyy/mm/dd">
                </div>
                </div>

                <div class="form-group row">
                <div class="col-sm-10">
                    <button type="submit" class="btn btn-primary">Save to CSV</button>
                </div>
                </div>
            </form>
            """

        s += """    

            <script>

                function showForm1( v ) {
                    if( ['hh_q','d_q'].includes(v) )
                    {
                        document.getElementById( "tariffs" ).style.display = "none"; }
                    else {
                        document.getElementById( "tariffs" ).style.display = "block";} 
                }

                function showForm2( v ) {
                    if( v == "Fixed" )
                    {
                        document.getElementById( "price" ).style.display = "flex";
                        document.getElementById( "region" ).style.display = "none";}
                    else {
                        document.getElementById( "price" ).style.display = "none";
                        document.getElementById( "region" ).style.display = "flex";} 
                }

            </script>
        """

        return create_sm_page(request, s, heading)


def checksPage(request):
    if request.GET.get('type','vars') == 'vars':
        s = '''
        with hhvars as (
        select t.var_id, t.product, t.region, min(v.period_id) min, max(v.period_id) max from sm_variables t
        left outer join sm_hh_variable_vals v on t.var_id=v.var_id 
        where t.granularity_id=0
        group by t.product, t.region, t.var_id order by product, region)

        select hhvars.var_id, hhvars.product, hhvars.region, pmin.period min, pmax.period max from hhvars 
        inner join sm_periods pmin on pmin.period_id=hhvars.min
        inner join sm_periods pmax on pmax.period_id=hhvars.max
        ;
        '''
        df_hhvars = loadDataFromDb(s, returndf=True)
    
        s = '''
        with hhvars as (
        select t.var_id, t.product, t.region, min(v.period_id) min, max(v.period_id) max from sm_variables t
        left outer join sm_hh_variable_vals v on t.var_id=v.var_id 
        where t.granularity_id=0
        group by t.product, t.region, t.var_id order by t.product, t.region)

        select hhvars.var_id, hhvars.product, hhvars.region, p.period, p.period_id
        from hhvars 
        inner join sm_periods p on p.period_id between hhvars.min and hhvars.max
        left outer join sm_hh_variable_vals v on v.period_id=p.period_id and hhvars.var_id=v.var_id
        where v.period_id is null
        ;
        '''
        df_hhgaps = loadDataFromDb(s, returndf=True)

        s = '''
        with dvars as (
        select t.var_id, t.product, t.region, min(v.local_date) min, max(v.local_date) max from sm_variables t
        left outer join sm_d_variable_vals v on t.var_id=v.var_id 
        where t.granularity_id=1
        group by t.product, t.region, t.var_id order by t.product, t.region)

        select dvars.var_id, dvars.product, dvars.region, dvars.min, dvars.max
        from dvars;
        '''
        df_dvars = loadDataFromDb(s, returndf=True)
    
        s = '''
        with dvars as (
        select t.var_id, t.product, t.region, min(v.local_date) min, max(v.local_date) max from sm_variables t
        left outer join sm_d_variable_vals v on t.var_id=v.var_id 
        where t.granularity_id=1
        group by t.product, t.region, t.var_id order by t.product, t.region)

        select distinct dvars.var_id, dvars.product, dvars.region, p.local_date
        from dvars 
        inner join sm_periods p on p.local_date between dvars.min and dvars.max
        left outer join sm_d_variable_vals v on v.local_date=p.local_date and dvars.var_id=v.var_id
        where v.local_date is null
        ;
        '''
        df_dgaps = loadDataFromDb(s, returndf=True)

        s = '''
        select v.var_id, t.product, t.region, v.period_id, p.period, min(id) min, max(id) max, count(id)  
        from sm_hh_variable_vals v 
        inner join sm_variables t on t.var_id=v.var_id and t.granularity_id=0
        inner join sm_periods p on v.period_id=p.period_id
        group by v.var_id, t.product, t.region, v.period_id, p.period
        having count(id)>1;
        '''
        df_hhduplicates = loadDataFromDb(s, returndf=True)

        s = '''
        select v.var_id, t.product, t.region, v.local_date, min(id) min, max(id) max, count(id)  
        from sm_d_variable_vals v 
        inner join sm_variables t on t.var_id=v.var_id and t.granularity_id=1
        group by v.var_id, t.product, t.region, v.local_date
        having count(id)>1;
        '''
        df_dduplicates = loadDataFromDb(s, returndf=True)



        content = '<H4>Half Hourly Variables</H4>'
        content += df_hhvars.to_html() + '<BR>'
        if len(df_hhgaps)==0:
            content += '<P>No gaps in hh variables</P>'
        else:
            content += '<P>Gaps in hh variables - see below</P>'
        if len(df_hhduplicates)==0:
            content += '<P>No duplicates in hh variables</P>'
        else:
            content += '<P>Duplicates in hh variables - see below</P>'

        content += '<H4>Daily Variables</H4>'
        content += df_dvars.to_html() + '<BR>'
        if len(df_dgaps)==0:
            content += '<P>No gaps in daily variables</P>'
        else:
            content += '<P>Gaps in daily variables - see below</P>'
        if len(df_dduplicates)==0:
            content += '<P>No duplicates in daily variables</P>'
        else:
            content += '<P>Duplicates in daily variables - see below</P>'

        if len(df_hhduplicates):
            content += '<H4>Half Hourly Duplicates</H4>'

        if len(df_hhduplicates) + len(df_dduplicates):
            content += '<H4>Duplicate variable values</H4>'
            if len(df_hhduplicates):
                content += df_hhduplicates.to_html() + '<BR>'
            if len(df_dduplicates):
                content += df_dduplicates.to_html() + '<BR>'

        if len(df_hhgaps) + len(df_dgaps):
            content += '<H4>Missing variable values</H4>'
            if len(df_hhgaps):
                content += df_hhgaps.to_html() + '<BR>'
            if len(df_dgaps):
                content += df_dgaps.to_html() + '<BR>'
        content += '<BR><BR>'
    else:
        content += ''
    
    output = create_sm_page(request, content, 'Checks')
    return output

def savelogtocsv(request):
    date = request.POST.get('date')
    extra = ''
    if 'reqagent' in request.GET:
        extra += ' and http_user_agent is not Null '
    if 'reqsession' in request.GET:
        extra += ' and session_id is not Null '
    s = f"""
    select id, datetime, method, choice, session_id, url, http_user_agent
    from sm_log where date(datetime-Interval '3 hours')='{date}'
    {extra}
    order by id desc;
    """
    
    df = loadDataFromDb(s, returndf=True)
    df['datetime'] = pd.DatetimeIndex(df['datetime']).strftime('%Y-%m-%dT%H:%M:%S.%f')
    df['method'] = df['method'].map({0: 'GET', 1: 'POST'})

    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="{}"'.format('smlog.csv')
    df.to_csv(response, index=False)
    return response

def logPage(request):
    if request.method=='POST':
        return savelogtocsv(request)
    else:
        s = '''
            with log as 
                (select date(datetime-Interval '3 hours') myday, 
                    * from sm_log where url not LIKE '%debug%')
                , log1 as (select myday, count(id) count from log group by myday)
                , log2 as (select myday, count(id) count from log where session_id is not null group by myday) 
                , log3 as (select myday, count(id) count from log where url like '%load%' group by myday)
                , log4 as (select myday, session_id from log where session_id is not null group by myday, session_id)
                , log5 as (select myday, count(session_id) from log4 group by myday )

            select log1.myday, log1.count pages, log2.count pages_in_sessions, log3.count loads, log5.count sessions
            from log1
            left join log2 on log1.myday=log2.myday
            left join log3 on log1.myday=log3.myday
            left join log5 on log1.myday=log5.myday

            order by log1.myday desc
            '''

        df = loadDataFromDb(s, returndf=True)
        df1 = df.fillna(0)

        s = '''
        with log as 
                (select date(datetime-Interval '3 hours') myday, 
                    * from sm_log where url not LIKE '%debug%'),
            log2 as (
        select 
        CASE WHEN POSITION('octopus' in url)>0 then 'octopus' WHEN POSITION ('n3rgy' in url)>0 then 'n3rgy' else 'unknown' end as source,  
        CASE when POSITION('loadgas' in url)>0 then 'gas' WHEN POSITION('loadexport' in url)>0 then 'export' else 'elec' end as type,
        *
        from log where url like '%load%')
        
        select 
        myday, source, type, count(id) count from log2 group by myday, source, type
        order by myday desc, source, type

        '''
        df = (loadDataFromDb(s, returndf=True))
        df = df[df['source']!='unknown']
        df['type'] = df['source'] + '_' +  df['type']
        df = df.groupby(['myday','type']).sum()['count']
        df = df.unstack(['type'])
        df.fillna(0, inplace=True)
        df2 = df.sort_index(ascending=False)

        
        url = request.get_full_path()
        heading = 'Log Data'
        today = datetime.datetime.today().strftime('%Y/%m/%d')
        from myutils.keys import ADVANCED_KEY
        s = ''
        if ADVANCED_KEY in request.GET:
            s += f"""
                <P>
                Testing
                </P> 
                <form action="{url}" method="post">
                    <input type="text" name="date" value="{today}">
                    <input type="submit" value="Save to CSV">
                </form><BR><BR>
                """

        s += df1.to_html(index=False, float_format='%.0f') + '<BR>'
        s += df2.to_html(float_format='%.0f') + '<BR>'
           
        return create_sm_page(request, s, heading)


def gastrackerpage(request):
    choice = request.path_info.split('/')[-1]
    type_id = 0 if choice=='electracker' else 1
    s = f'''
    select local_date, region, value from sm_d_variable_vals v 
    inner join sm_variables t on t.var_id=v.var_id and t.granularity_id=1 and t.product='SILVER-2017-1' 
    and type_id={type_id} order by local_date desc
    '''
    df = loadDataFromDb(s, returndf=True)
    df = df.groupby(['local_date','region']).sum()
    df = df*1.05
    df = df.unstack()
    df = df.sort_index(ascending=False)
    df = df['value']
    if type_id == 0:
        df['Wholesale'] = (df['A']/1.05-7.12)/1.2
    else:
        df['Wholesale'] = (df['A']/1.05-1.13)/1.06
    s = '<P>All prices are in p/kwh. Regional retail prices include VAT, wholesale prices do not include VAT.</P>'
    s += df.to_html(float_format='%.3f').replace('Wholesale','Wholesale<BR>p/kwh')
    header = 'Electricity' if type_id==0 else 'Gas'
    return create_sm_page(request, s, f'Octopus {header} Tracker Prices')

def calccomparison(request, choice, tariffs):
    type_id, type_label = get_type_id(choice)
    region = request.POST.get('region')
    gasmult = request.POST.get('gasmult', '1.0')
    start = request.GET.get('start', '2019-01-01')
    end = request.GET.get('end','2020-12-31')
    end = min(end, datetime.datetime.today().strftime('%Y-%m-%d'))
    smid = get_sm_id(request)
    metric = request.POST.get('metric')
    vat = 1 if type_id==2 else 1.05
    if metric in ['cost','price']:
        qstr = quantitystr(smid, type_id)
    elif metric in ['deemedprice']:
        qstr = """select period_id, value as quantity from sm_hh_variable_vals v
                inner join sm_variables var on v.var_id=var.var_id and var.product='Profile_1'"""
    
    calcs = []
    for t in tariffs:
        isfixed, pricestr = parsetariff(request, t, vat, region=region)
        endstr = f"""
            select date_trunc('month',local_date) as month, count(period_id) as numperiods, 
            sum(value)/count(value) as price, sum(quantity) as total_quantity, sum(quantity*value) as total_cost  
            from fulldata group by month order by month"""

        s = f'''
        with periods as (select * from sm_periods where local_date between '{start}' and '{end}' )
        , prices as ({pricestr} )
        , quantities as ({qstr})
        , fulldata as 
            (select periods.*, quantities.quantity, prices.value 
            from periods inner join quantities on periods.period_id=quantities.period_id
            inner join prices on periods.period_id=prices.period_id)
        {endstr}
        '''
        df = loadDataFromDb(s, returndf=True)
        if metric in ['price','deemedprice']:
            df = pd.DataFrame(df['price'].values, columns=[t], index=df.month.values)
        elif metric in ['cost']:
            df = pd.DataFrame(df['total_cost'].values/100, columns=[t], index=df.month.values)
        calcs.append(df)

    df = pd.concat(calcs, axis=1)
    df = df.astype(float)*vat
    if metric=='price':
        s = '<H4>Average Price (in p/kwh) for each tariff based on your consumption/export</H4>'
    elif metric=='cost':
        s = '<H4>Cost (in £) for each tariff based on your consumption/export</H4>'
    elif metric=='deemedprice':
        s = '<H4>Average Price (in p/kwh) for each tariff based on the average domestic deemed consumption</H4>'
    if type_id in [0,1]:
        s += '<P>Prices and costs include 5% VAT.</P>'
    df = df.iloc[::-1]    
    s += df.to_html(float_format='%.2f')

    return s


def tariffcomparison(request, choice):
    type_id, type_label = get_type_id(choice)
    heading = '{} Tariff Comparison'.format(type_label)
    prefix = ['', 'gas', 'export'][type_id]
    tariff = request.GET.get(f'{prefix}tariff', '0.0')
    region = request.GET.get('region', 'C')
    gasmult = request.GET.get('gasmult','1')



    s2 = ''

    if request.method=='POST':
        region = request.POST.get('region')
        gasmult = request.POST.get('gasmult', '1.0')
        tariffs = [request.POST.get(f'tariff{i}') for i in range(5)]
        tariffs = [t for t in tariffs if len(t)]
        s2 = calccomparison(request, choice, tariffs)
        
    else:
        tariffs = [tariff]
        
        if type_id==0:
            for t in ['AGILE-18-02-21','GO-18-06-12','SILVER-2017-1']:
                if tariff!=t:
                    tariffs.append(t)
            tariffs.append('14.469')
            tariffs.append('0000-0800:9.681,15.8445')
        elif type_id==1:
            if tariff!='SILVER-2017-1':
                tariffs.append('SILVER-2017-1')
            else:
                tariffs.append('2.562')
        elif type_id==2:
            if tariff!='AGILE-OUTGOING-19-05-13':
                tariffs.append('AGILE-OUTGOING-19-05-13')
            else:
                tariffs.append('5.5')

    while len(tariffs)<5:
        tariffs.append('')

    regionselector = getregionselector(region)
    
    metricselector = """
    <option value="cost" {}>Monthly cost (£) based on your volumes</option>
    <option value="price" {}>Monthly price (p/kwh) based on your volumes</option>"""
    if type_id==0:
        metricselector += '<option value="deemedprice" {}>Monthly price (p/kwh) based on deemed profile volumes</option>'
    
    i = ['cost','price','deemedprice'].index(request.POST.get('metric','cost'))
    j = ['','','']
    j[i] = 'selected="true"'
    metricselector = metricselector.format(*j)
    gasmultdisplay='flex' if type_id==1 else 'none'
    VAT = ', incl VAT' if type_id in [0,1] else ''
    url = request.get_full_path()


    s = f"""
    <form action="{url}" method="post">
 
    <div class="form-group row" id="metric" >
    <label for="inputEmail3" class="col-sm-2 col-form-label">Calculation</label>
    <div class="col-sm-10">
        <select class="form-control" name="metric">
        {metricselector}
        </select>
    </div>
    </div>
    <div class="form-group row" id="region" >
    <label for="inputEmail3" class="col-sm-2 col-form-label">Region (for time-varying tariffs)</label>
    <div class="col-sm-10">
        <select class="form-control" name="region">
        {regionselector}
        </select>
    </div>
    </div>


    <div class="form-group row" id="gasmult" style="display:{gasmultdisplay};">
    <label for="inputEmail3" class="col-sm-2 col-form-label">Gas Multiplier for SMETS2</label>
    <div class="col-sm-10">
      <input type="text" class="form-control" name="gasmult" value="{gasmult}">
    </div>
    </div>
    <P>Please include up to 5 tariffs. These can be time varying or fixed. Fixed tariffs should include VAT for electricity and gas consumption.</P>
    """

    for i, j in enumerate(tariffs):
        s += f"""
        <div class="form-group row" id="tariff">
        <label for="inputEmail3" class="col-sm-2 col-form-label" >Tariff {i+1}</label>
        <div class="col-sm-10">
        <input type="text" class="form-control" name="tariff{i}" value="{j}">  
        </div>
        </div>
        """

    s += """
    <div class="form-group row">
      <div class="col-sm-10">
        <button type="submit" class="btn btn-primary">Compare Tariffs</button>
      </div>
    </div>
    </form>
    """

    content = s + s2

    return create_sm_page(request, content, heading)



def analysisPage(request):

    smid = get_sm_id(request)
    if request.method=='GET':
        start = request.GET.get('start', '2020/01/01')
        end = request.GET.get('end','2020/07/31') 
    else:
        start = request.POST.get('startdate')
        end = request.POST.get('enddate')


    region = request.GET.get('region','C')
    _, pricestr = parsetariff(request, 'AGILE-18-02-21', 1.05, region=region)

    endstr = f"""
    select period, date_trunc('month', local_date) as month, local_date, local_time, actual_qty, prof_qty, price 
    from fulldata
    order by period"""

    s = f'''
    with periods as (select * from sm_periods where local_date between '{start}' and '{end}' )
    , prices as ({pricestr} )
    , quantities as ({quantitystr(smid, 0)})
    , prof as (select period_id, value from sm_hh_variable_vals v
                inner join sm_variables var on v.var_id=var.var_id and var.product='Profile_1')
    , fulldata as 
        (select periods.*, prices.value price, prof.value prof_qty, quantities.quantity actual_qty
        from periods inner join prices on periods.period_id=prices.period_id
        inner join prof on periods.period_id=prof.period_id 
        inner join quantities on periods.period_id=quantities.period_id)
        {endstr}
    '''   
    df = loadDataFromDb(s, returndf=True)
    df['price']*=1.05
    df['prof_qty']*= df.actual_qty.sum()/df.prof_qty.sum()

    df1 = df[['actual_qty']].copy()
    df1['bin'] = pd.qcut(df1.actual_qty.rank(method='first'), 20)
    df1 = df1.groupby('bin')
    df1 = np.hstack([df1.min().values, df1.max().values, df1.sum().values])
    df1[:,2]/=df1[:,2].sum()

    df1a = pd.DataFrame(df1, columns=['min','max','pct'])
    df1a = df1a.loc[df1a.index[::-1]]


    chart1heading = f'''
        This first chart shows how your consumption varied from half hour to half hour. It breaks the consumption into 20 buckets. The first 
        bucket is the 5% of half hours with the highest consumption (between {df1a.iloc[0]['min']} and {df1a.iloc[0]['max']}kwh), which makes up 
        {df1a.iloc[0].pct:.1%} of overall volume. The last bucket is the 5% of half hours with the lowest consumption (between {df1a.iloc[-1]['min']} and {df1a.iloc[-1]['max']}kwh), which makes up 
        just {df1a.iloc[-1].pct:.1%} of overall volume. 
        '''
    
    chart1labels = str([f"{d['min']}-{d['max']}" for _, d in df1a.iterrows()])
    print(chart1labels)
    quantity1 = str([f'{100*d.pct:.1f}' for _, d in df1a.iterrows()])
    print(df1a)

    df2 = df.groupby(['local_time']).mean()
    df2['actual_min'] = df.groupby(['local_time']).min().actual_qty
    df2['actual_max'] = df.groupby(['local_time']).max().actual_qty
    
    chart2labels = str([x for x in df2.index])
    quantity2a = str([f'{x:.3f}' for x in df2.actual_qty])
    quantity2b = str([f'{x:.3f}' for x in df2.prof_qty])
    quantity2c = str([f'{x:.2f}' for x in df2.price])

    table2 = df2[['actual_qty','prof_qty','price']].to_html(float_format='%.3f', header=False, index_names=False) 

    s = '''
    <thead>
    <tr><TH>Local Time</TH><TH>Your Quantity<BR>(kwh)</TH><TH>Average Profile<BR>Quantity (kwh)</TH><TH>Price<BR>(p/kwh)</TH>
    </thead><tbody>'''
    table2 = table2.replace('<tbody>', s)


    print(df2)

    df3 = df.copy()
    df3['actual_cost'] = df3.actual_qty*df3.price
    df3['prof_cost'] = df3.prof_qty*df3.price
    df3 = df3.groupby(['month']).sum()
    df3['actual_price'] = df3.actual_cost/df3.actual_qty
    df3['prof_price'] = df3.prof_cost/df3.prof_qty
    df3.index = df3.index.strftime('%Y-%m')

    df3.loc['Total'] = {'actual_price': df3.actual_cost.sum()/df3.actual_qty.sum(), 
                        'prof_price': df3.prof_cost.sum()/df3.prof_qty.sum(),
                        'prof_qty': 0,
                        'actual_qty': df3.actual_qty.sum(),
                        'actual_cost': 0,
                        'prof_cost': 0,
                        'price': 0}
    df3 = df3[['actual_qty','actual_price','prof_price']]
    
    print(df3)

    table3 = df3.to_html(float_format='%.3f', header=False, index_names=False)
    s = '''
    <thead>
    <tr><TH>Month</TH><TH>Quantity<BR>(kwh)</TH><TH>Your Profile<BR>Price (p/kwh)</TH><TH>Deemed Profile<BR>Price (p/kwh)</TH>
    </thead><tbody>'''
    table3 = table3.replace('<tbody>', s)
    
    with open(os.path.dirname(os.path.realpath(__file__)) + "/templates/analysis_template.html", "r") as f:
        inner = f.read()

    kwargs = {
            'chart1heading': chart1heading,
            'chart1labels': chart1labels,
            'quantity1': quantity1,
            'chart2labels': chart2labels,
            'quantity2a': quantity2a,
            'quantity2b': quantity2b,
            'quantity2c': quantity2c,
            'table2': table2,
            'table3': table3,
            'start': start,
            'end': end,
            'url': request.get_full_path()
            }    


    #import pdb; pdb.set_trace()
    for k, v in kwargs.items():
        inner = inner.replace('{' + k + '}', v)

    heading = 'Consumption Profile Analysis'

    return create_sm_page(request, inner, heading)

def load_transactions(key):
    import requests
    url = 'https://api.octopus.energy/v1/graphql/'
    
    query = """
        mutation APIKeyAuthentication($apiKey: String!) {
          apiKeyAuthentication(apiKey: $apiKey) {
            token,
          }
        }
    """
    variables = {'apiKey': key[10:]}
    
    r = requests.post(url, json={'query': query, 'variables': variables}, )
    headers = {'Authorization': r.json()['data']['apiKeyAuthentication']['token']}
    
    
    
    
    query = """
        query getBalanceHistory($accountNumber: String!, $cursor: String) {
                account(accountNumber: $accountNumber) {
                        transactions(first: 1000, after: $cursor) {
                            edges {
                                    node {__typename,id,title,postedDate,amount,balanceCarriedForward,isHeld,          
                                        ... on Charge {
                                                consumption {startDate,endDate,quantity,unit,__typename},
                                                __typename
                                                },
                                        }
                                        }
                        }  }}
    """
    
    
    variables= {'accountNumber': key[:10]}
    r = requests.post(url, json={'query': query, 'variables': variables}, headers=headers )
    
    t = r.json()['data']['account']['transactions']['edges']   
    t = [x['node'] for x in t ]
    t.sort(key=lambda x: int(x['id']), reverse=True)
    for num, x in enumerate(t):
        if num==len(t)-1:
            prior = 0
        else:
            prior = t[num+1]['balanceCarriedForward']
        x['amount'] = (x['balanceCarriedForward']-prior)/100   
        if 'consumption' in x.keys():
            if isinstance(x['consumption'], dict):
                for col in ['startDate','endDate','quantity']:
                    x[col] = x['consumption'][col]
            x.pop('consumption')
        x.pop('id')
        x.pop('balanceCarriedForward')
        x.pop('isHeld')
    return t

def get_transactions(t, hasexport):
    transactions = {'payments': [], 
                    'rewards': [], 
                    'electricity': [], 
                    'gas': [], 
                    'export': [],
                    'other': [], }
    
    
    for x in t:
        if x['__typename']=='Payment':
            transactions['payments'].append(x)
        elif x['__typename']=='Charge':
            if x['title']=='Gas':
                transactions['gas'].append(x)
            elif x['title']=='Electricity':
                if hasexport and x['amount']>0:
                    x['title']=='Export'
                    transactions['export'].append(x)
                elif x['amount']<0:
                    transactions['electricity'].append(x)
                else:
                    transactions['electricity'].append(x)
            elif x['title']=='Default':
                transactions['payments'].append(x)
            else:
                transactions['other'].append(x)
        elif x['__typename']=='Credit':
            if x['title'] in ['Electricity','Gas']:
                if x['title']=='Electricity' and hasexport and x['amount']<0:
                    x['title'] = 'Export'
                transactions['other'].append(x)
            elif 'reward' in x['title']:
                transactions['rewards'].append(x)
            elif 'gesture' in x['title']:
                transactions['rewards'].append(x)
            elif 'credit refund' in x['title']:
                transactions['payments'].append(x)
            else:
                transactions['other'].append(x)
        else:
            transactions['other'].append(x)
    
    
    
    for commod in ['electricity','gas','export']:
        otherise_old = 0
        if len(transactions[commod]):
            if otherise_old:
                for idx in range(len(transactions[commod])-1,-1,-1):
                    if transactions[commod][idx]['startDate'] is None:
                        transactions['other'].append(transactions[commod].pop(idx))
            
            temp = pd.DataFrame( transactions[commod])
            temp['valid'] = 1
            
            for i in range(1,len(temp)):
                start = temp.loc[i].startDate
                end = temp.loc[i].endDate         
                for j in range(i):
                    if temp.loc[j, 'valid']==1:
                        if (temp.loc[j].startDate<=start<=temp.loc[j].endDate) or (temp.loc[j].startDate<=end<=temp.loc[j].endDate):
                            temp.loc[i, 'valid']=0
                            break
            
            for i in temp[temp.valid==0].index.sort_values(ascending=False):
                transactions['other'].append(transactions[commod].pop(i))
            
            temp = temp[temp.valid==1]
            temp['days'] = (pd.DatetimeIndex(temp.endDate)-pd.DatetimeIndex(temp.startDate)).days+1
            temp['kWh_daily'] = temp.quantity.astype(float)/temp['days']
            temp['kWh_total'] = temp.quantity.astype(float)
            temp['cost_daily'] = temp.amount/temp['days']
            temp = temp[['postedDate','amount','startDate','endDate','cost_daily','kWh_daily','kWh_total']].sort_values('startDate',ascending=False)
            transactions[commod] = temp
    
    
    for commod in ['payments','rewards']:
        if len(transactions[commod]):
            transactions[commod] = pd.DataFrame(transactions[commod])[['title','postedDate','amount']].sort_values('postedDate',ascending=False)
    
    if len(transactions['other']):
        temp = pd.DataFrame(transactions['other'])
        temp = temp.sort_values(['title','postedDate'], ascending=False)
        temp = temp[['title','__typename','postedDate','amount','startDate','endDate']]
        transactions['other'] = temp
    return transactions

def octobillPage(request):
    key = request.GET.get('octopus')

    s = """
        <P>The Octopus balance history is quite hard to follow, especially if you have had bills recalculated. This page pulls in 
        all the transactions from your Octopus account and attempts to present them in a more meaningful manner. It won't be able to handle
        every situation, but I hope it is useful.</P>"""

    if request.method=='POST':
        start = request.POST.get('start')
        end = request.POST.get('end')
    else:
        start = '2016/01/01'
        end = '2020/12/31'

    s += f"""
    <form action="{request.get_full_path()}" method="post">
    <div class="form-group row">
    <label for="inputEmail3" class="col-sm-2 col-form-label">Start Date (yyyy/mm/dd)</label>
    <div class="col-sm-10">
      <input type="string" class="form-control" name="start" value="{start}">
    </div>
    </div>
 
    <div class="form-group row">
    <label for="inputEmail3" class="col-sm-2 col-form-label">End Date (yyyy/mm/dd)</label>
    <div class="col-sm-10">
      <input type="text" class="form-control" name="end" value="{end}">
    </div>
    </div>

    <div class="form-group row">
      <div class="col-sm-10">
        <button type="submit" class="btn btn-primary">Load Transactions</button>
      </div>
    </div>
    </form>
    """    

    t = load_transactions(key)
    prior = [x for x in t if x['postedDate']<=start.replace('/','-')]
    if len(prior):
        prior = pd.DataFrame(prior).amount.sum()
    else:
        prior = 0
    t = [x for x in t if start.replace('/','-')<x['postedDate']<=end.replace('/','-')]

    hasexport = int(request.GET.get('mode','111')[2])
    transactions = get_transactions(t, hasexport)

    s += "<H3>Summary</H3><TABLE>"
    s += f"<TR><TD>Prior Balance</TD><TD>{prior:.2f}</TD></TR>"
    for col in ['payments','rewards','gas','electricity','export','other']:
        if len(transactions[col]):
            s += f"<TR><TD>{col.capitalize()}</TD><TD>{transactions[col].amount.sum():.2f}</TD></TR>"
    s += f"<TR><TH>End Balance</TH><TH>{prior+pd.DataFrame(t).amount.sum():.2f}</TH></TR></TABLE><BR>"

    for col in ['payments','rewards','gas','electricity','export','other']:
        if len(transactions[col]):  
            s += f'<H3>{col.capitalize()}: {transactions[col].amount.sum():.2f}</H3>'
            s += transactions[col].to_html(float_format='%.2f', index=False) + '<BR>'

    return create_sm_page(request, s, 'Octopus Balance Reconciliation')