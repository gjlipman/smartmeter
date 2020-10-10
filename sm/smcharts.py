from django.http import HttpResponse, HttpRequest
from django.shortcuts import render, redirect
import os
import sys
import pandas as pd
import numpy as np

from myutils.utils import (getConnection, loadDataFromDb)
from myutils.smutils import (adj_url, sidebarhtml, 
                            getRegion, getregions, menuitems, octopusmeters, n3rgymeters, get_sm_id, quantitystr,
                            parsetariff, get_type_id, getmode, create_sm_page, getTariff)

type_ids = {'cost': 0, 'gascost': 1, 'exportrevenue': 2, 'consumption': 0, 'gasconsumption': 1, 'export': 2}


def getmonthoptions(url, selectedmonth, selectedday):
    months = pd.date_range(pd.Timestamp('2019-01-01'), pd.Timestamp.now()-pd.offsets.MonthBegin(), freq='MS')
    s = ''
    if selectedmonth is None:
        s+= f'<option selected>Month...</option>'
    for m in months.sort_values(ascending=False):
        u = adj_url(url, ['day'], [('month',f"{m:%Y-%m}")] )
        if m.strftime('%Y-%m')==selectedmonth:
            if selectedday is None:
                s += f'<option selected>{m:%B %Y}</option>'
            else:
                s += f'<option selected>Change Month...</option>'
        else:
            s += f'<option value="{u}">{m:%B %Y}</option>'
    return s

def getdayoptions(url, selectedmonth, selectedday):

    m = pd.Timestamp(selectedmonth + '-01')
    days = pd.date_range(m, m+pd.offsets.MonthEnd())
    s = ''
    for d in days:
        u = adj_url(url, [], [('day',d.day)])
        if selectedday and d.day == int(selectedday):
            s += f'<option selected value="{u}">{d:%A %d}</option>'
        else:
            s += f'<option value="{u}">{d:%A %d}</option>'
    if selectedday is None:
        s = '<option selected>Day...</option>' + s
    else:
        d = days[0]-pd.offsets.Day()
        u = adj_url(url, [], [('month', f'{d:%Y-%m}'), ('day',d.day)])
        s = f'<option value="{u}">{d:%A %d}</option>' + s
        d = days[-1]+pd.offsets.Day()
        u = adj_url(url, [], [('month', f'{d:%Y-%m}'), ('day',d.day)])
        s += f'<option value="{u}">{d:%A %d}</option>' 
    return s

def getnavbar(request):
    url = request.get_full_path()
    day = request.GET.get('day', None)
    month = request.GET.get('month', None)

    if 'oldbar' not in request.GET:
        homeurl = adj_url(url, ['month','day'],[])

        s = f'''
            <div class="form-row">
            <div class="col-auto">
                <a class="btn btn-outline-secondary" type="button" href="{homeurl}">
                    All Months</a>
            </div>
            <div class="col-auto">
            <select class="custom-select" onchange="document.location.href=this.value">
            {getmonthoptions(url, month, day)}
            </select>
            </div>
        '''

        if month is not None:
            if day is not None:
                u = adj_url(url, ['day'],[])
                ms = pd.Timestamp(month).strftime('%b %Y')
                s += f'''
                <div class="col-auto">
                    <a class="btn btn-outline-secondary" type="button" href="{u}">
                        {ms}</a>
                </div>'''

            s += f'''
                <div class="col-auto">
                <select class="custom-select" id="day" onchange="document.location.href=this.value" >
                {getdayoptions(url, month, day)}
                </select>
                </div>
            '''
        s += '</div>'

        return s


    else:
        if month is None:
            navbar = ''
        elif day is None:
            today = pd.Timestamp('{}-{:02d}'.format(month,1))
            prevmonth = (today-pd.offsets.MonthBegin()).strftime('%Y-%m')
            nextmonth = (today+pd.offsets.MonthBegin()).strftime('%Y-%m')
            navbar = '<A HREF="{}">{}</A> &nbsp; <A HREF="{}">{}</A> &nbsp; <A HREF="{}">{}</A>'
            navbar = navbar.format(adj_url(url, [],[('month',prevmonth)]), prevmonth,
                                adj_url(url, ['month'],[]) , 'All Months',
                                adj_url(url, [],[('month',nextmonth)]), nextmonth)
        else:
            today = pd.Timestamp('{}-{:02d}'.format(month,day))
            prevday = (today-pd.offsets.Day())
            nextday = (today+pd.offsets.Day())
            navbar = '<A HREF="{}">{}</A> &nbsp; <A HREF="{}">Whole Month</A>&nbsp; <A HREF="{}">{}</A>'
            navbar = navbar.format(adj_url(url, [],[('day',prevday.day), ('month',prevday.strftime('%Y-%m'))]), prevday.strftime('%a %b %d'),
                                adj_url(url, ['day'],[]) ,
                                adj_url(url, [],[('day',nextday.day), ('month',nextday.strftime('%Y-%m'))]), nextday.strftime('%a %b %d'))
        return navbar

def nodata(request):
    url = request.get_full_path()
    content = getnavbar(request) 
    content += '<BR><P>No data available for this date. Please '
    
    if 'HTTP_REFERER' in request.META:
        content += f'''return to the <A HREF="{request.META.get('HTTP_REFERER')}">previous page</A>, '''
    content += 'select a different date using the options above, or click All Months. '
    if ('start' in request.GET) or ('end' in request.GET):
        content += f'Note that you have a start and/or end date in the url, which you may need to remove to display available data. '
    u = url.replace(request.path_info.split('/')[-1], 'admin', 1)
    content += f'For more details on the available data, visit the <A HREF="{u}">Admin Page</A>.</P>'
    heading = 'No Data Available'
    inner = content.format(url)
    return create_sm_page(request, inner, heading)

def consumptionPage(request, choice):
    smid = get_sm_id(request)
    type_id = type_ids[choice]
    start = request.GET.get('start', '2018-01-01')
    end = request.GET.get('end','2025-01-01')    

    if 'month' in request.GET:
        month = request.GET.get('month')
        if 'day' in request.GET:
            day = int(request.GET.get('day'))
            start = month + '-{:02d}'.format(day)
            end = start
            endstr = """
            select local_date, local_time as local_time_start, timezone_adj, quantity as total_quantity
            from fulldata order by period"""
        else:
            endstr = f"""
            select local_date as day, count(period_id) as numperiods, sum(quantity) as total_quantity
            from fulldata where date_trunc('month', local_date)='{month}-01'
            group by local_date order by local_date"""
    else:
        endstr = f"""
            select date_trunc('month',local_date) as month, count(period_id) as numperiods, 
            sum(quantity) as total_quantity 
            from fulldata group by month order by month"""

    s = f'''
    with periods as (select * from sm_periods where local_date between '{start}' and '{end}' )
    , quantities as ({quantitystr(smid, type_id)})
    , fulldata as 
        (select periods.*, quantities.quantity 
        from periods inner join quantities on periods.period_id=quantities.period_id)
        {endstr}
    ''' 

    data = loadDataFromDb(s, returndf=True)
    url = request.get_full_path()
    type = ['electricity consumption','gas consumption','electricity export'][type_id]
    
    if data.shape[0]==0:
        return nodata(request)
 
    if type_id==1:
        gasmult = float(request.GET.get('gasmult', '1'))
        data['total_quantity']*=gasmult
        if gasmult == 1:
            gaswarn = '<BR><P><B>These results do not include any <A HREF="https://www.theenergyshop.com/guides/how-to-convert-gas-units-to-kwh" target="_blank">gas conversion from m3 to kwh</A>. If your meter is SMETS2, you should most likely include a parameter gasmult=11.18 or thereabouts.</B>'
        elif 10<gasmult<12:    
                gaswarn = '<BR><P>These results are based on a gas conversion of {}kwh per m3. This factor can be adjusted in the url. This conversion should not be applied if your meter is SMETS1.</P>'.format(gasmult)
        else:
                gaswarn = '<BR><B>These results are based on a gas conversion of {}kwh per m3. This factor appears wrong.</B> It <A HREF="https://www.theenergyshop.com/guides/how-to-convert-gas-units-to-kwh" target="_blank"> should be around 11.18</A>, based on a volume correction factor of 1.02264, a calorific value of about 40, and dividing by the kwh to joule conversion of 3.6. Your latest bill should show the applicable conversions.'.format(gasmult)
    else: 
        gaswarn = ''

    if 'month' in request.GET:
        month = request.GET.get('month')
        if 'day' in request.GET:
            day = int(request.GET.get('day'))
            heading = 'Half-hourly {} on {}-{:02d}'.format(type.title(), month, day)
            navbar = getnavbar(request)
            description = f'{type.capitalize()} for each half hour (labels are start-times, in local time)'
            labels = str(data['local_time_start'].tolist())
            quantity = str(['{:.2f}'.format(x) for x in data['total_quantity'].values])
            table = '<TABLE><TR><TH>Total Quantity</TH><TD>{:.3f}</TD></TR></TABLE>'.format(data.total_quantity.sum())
            table += '<BR><TABLE><TR><TH>Period Start</TH><TH>Quantity</TH></TR>'
            for _, j in data.iterrows():
                t = '<TR><TD>{}</TD><TD>{:.3f}</TD></TR>'
                table += t.format(j.local_time_start, j.total_quantity)
            table += '</TABLE>'
            
        else:
            heading = 'Daily {} for {}'.format(type.title(), month)
            navbar = getnavbar(request)
            description = f"{type.capitalize()} (in KWh) for each day"
            labels = str([x.strftime('%d %b') for x in data['day'].tolist()])
            quantity = str(['{:.2f}'.format(x) for x in data.total_quantity.tolist()])

            table = '<TABLE><TR><TH>Total Quantity</TH><TD>{:.3f}</TD></TR>'.format(data.total_quantity.sum())
            avg = 48*(data.total_quantity).sum()/data.numperiods.sum()
            table += '<TR><TH>Avg Daily Quantity</TH><TD>{:.3f}</TD></TR></TABLE>'.format(avg)
            table += '<BR><TABLE><TR><TH>Day</TH><TH>Quantity</TH></TR>'
            for _, j in data.iterrows():
                t = '<TR><TD><A HREF="{}">{}</A></TD><TD>{:.3f}</TD></TR>'
                table += t.format(adj_url(url, [],[('day',j.day.day)]),
                                j.day.strftime('%a %b %d'), j.total_quantity)
            table += '</TABLE>'  
    else:        
        heading = 'Monthly {}'.format(type.title())
        navbar = getnavbar(request)
        description = f"Average daily {type.capitalize()} (in KWh) for each month:"
        labels = str([x.strftime('%b-%Y') for x in data['month'].tolist()])
        quantity = 48*data['total_quantity']/data['numperiods']
        quantity = str(['{:.2f}'.format(x) for x in quantity.tolist()])
        table = '<TABLE><TR><TH>Total Quantity</TH><TD>{:.3f}</TD></TR>'.format(data.total_quantity.sum())
        avg = 48*data.total_quantity.sum()/data.numperiods.sum()
        table += '<TR><TH>Avg Daily Quantity</TH><TD>{:.3f}</TD></TR></TABLE>'.format(avg)
        table += '<BR><TABLE><TR><TH>Month</TH><TH>Total<BR>Quantity</TH><TH>Daily<BR>Quantity</TR>'
        for _, j in data.iterrows():
            t = '<TR><TD><A HREF="{}">{}</A></TD><TD>{:.3f}</TD><TD>{:.3f}</TD></TR>'
            avg = 48*(j.total_quantity/j.numperiods)
            table += t.format(adj_url(url, [],[('month',j.month.strftime('%Y-%m'))]),
                                j.month.strftime('%b %Y'), j.total_quantity, avg)
        table += '</TABLE>'  

    if 'chartscale' in request.GET:
        c = request.GET.get('chartscale').split(',')
        chartscale = f'min: {c[0]}, '
        if len(c)>1:
            chartscale += f'max: {c[1]}, '
    else:
        chartscale = 'suggestedMin: 0'

    with open(os.path.dirname(os.path.realpath(__file__))  + "/templates/chart_template.html", "r") as f:
        inner = f.read()


    kwargs = {'navbar': navbar + gaswarn,
            'description': description,
            'chartlabels': labels,
            'quantity': quantity,
            'table': table+'<BR>',
            'chartscale': chartscale,
            }    
    for k, v in kwargs.items():
        inner = inner.replace('{' + k + '}', v)

    output = create_sm_page(request, inner, heading)
    return output

def costPage(request, choice):
    type_id = type_ids[choice]
    prefix = ['','gas','export'][type_id]
    smid = get_sm_id(request)
    vat = 1 if type_id==2 else 1.05
    isfixed, pricestr = parsetariff(request, request.GET.get(prefix+'tariff'), vat)
    start = request.GET.get('start', '2018-01-01')
    end = request.GET.get('end','2025-01-01')   
    if 'month' in request.GET:
        month = request.GET.get('month')
        if 'day' in request.GET:
            day = int(request.GET.get('day'))
            start = month + '-{:02d}'.format(day)
            end = start
            endstr = """
            select local_date, local_time as local_time_start, timezone_adj, quantity as total_quantity, 
            value as price, quantity*value as total_cost 
            from fulldata order by period"""
        else:
            endstr = f"""
            select local_date as day, count(period_id) as numperiods, sum(quantity) as total_quantity, 
            sum(value)/count(value) as price, sum(quantity*value) as total_cost  
            from fulldata where date_trunc('month', local_date)='{month}-01'
            group by local_date order by local_date"""
    else:
        endstr = f"""
            select date_trunc('month',local_date) as month, count(period_id) as numperiods, 
            sum(value)/count(value) as price, sum(quantity) as total_quantity, sum(quantity*value) as total_cost  
            from fulldata group by month order by month"""

 
    s = f'''
    with periods as (select * from sm_periods where local_date between '{start}' and '{end}' )
    , prices as ({pricestr} )
    , quantities as ({quantitystr(smid, type_id)})
    , fulldata as 
        (select periods.*, quantities.quantity, prices.value 
        from periods inner join quantities on periods.period_id=quantities.period_id
        inner join prices on periods.period_id=prices.period_id)
       {endstr}
    ''' 
    data = loadDataFromDb(s, returndf=True)
    url = request.get_full_path()
    type = ['electricity cost','gas cost','export revenue'][type_id]
    costrev = 'revenue' if type_id==2 else 'cost'

    if data.shape[0]==0:
        return nodata(request)

    if type_id==1:
        gasmult = float(request.GET.get('gasmult', '1'))
        data['total_quantity']*=gasmult
        data['total_cost']*=gasmult
        if gasmult == 1:
            gaswarn = '<BR><P><B>These results do not include any <A HREF="https://www.theenergyshop.com/guides/how-to-convert-gas-units-to-kwh" target="_blank">gas conversion from m3 to kwh</A>. If your meter is SMETS2, you should most likely include a parameter gasmult=11.18 or thereabouts.</B>'
        elif 10<gasmult<12:    
                gaswarn = '<BR><P>These results are based on a gas conversion of {}kwh per m3. This factor can be adjusted in the url. This conversion should not be applied if your meter is SMETS1.</P>'.format(gasmult)
        else:
                gaswarn = '<BR><B>These results are based on a gas conversion of {}kwh per m3. This factor appears wrong.</B> It <A HREF="https://www.theenergyshop.com/guides/how-to-convert-gas-units-to-kwh" target="_blank"> should be around 11.18</A>, based on a volume correction factor of 1.02264, a calorific value of about 40, and dividing by the kwh to joule conversion of 3.6. Your latest bill should show the applicable conversions.'.format(gasmult)
    else: gaswarn = ''

    data['price'] = np.where(data.total_quantity==0, data.price, data.total_cost/data.total_quantity)
    data['total_cost'] *= vat
    data['price'] *= vat

    if 'month' in request.GET:
        month = request.GET.get('month')
        if 'day' in request.GET:
            day = int(request.GET.get('day'))
            heading = 'Half-hourly {} on {}-{:02d}'.format(type.title(), month, day)
            navbar = getnavbar(request)
            description = f'{costrev.title()} in pence for each half hour (labels are start-times, in local time). '
            labels = str(data['local_time_start'].tolist())
            cost = str(['{:.2f}'.format(x) for x in data['total_cost'].values])
            table = '<TABLE><TR><TH>Total Quantity (kwh)</TH><TD>{:.2f}</TD></TR>'.format(data.total_quantity.sum())
            table += '<TR><TH>Total {} (£)</TH><TD>{:.2f}</TD></TR>'.format(costrev.title(), data.total_cost.sum()/100)
            avg = data.total_cost.sum()/data.total_quantity.sum()
            table += '<TR><TH>Average Price (p/kwh)</TH><TD>{:.2f}</TD></TR></TABLE>'.format(avg)
            table += f'<BR><TABLE><TR><TH>Period Start</TH><TH>Quantity</TH><TH>Price</TH><TH>{costrev.title()}</TH></TR>'
            for _, j in data.iterrows():
                t = '<TR><TD>{}</TD><TD>{:.3f}</TD><TD>{:.2f}</TD><TD>{:.2f}</TD></TR>'
                table += t.format(j.local_time_start, j.total_quantity, j.price, j.total_cost )
            table += '</TABLE>'
            
        else:
            heading = 'Daily {} for {}'.format(type.capitalize(), month)
            navbar = getnavbar(request)
            description = f"{costrev.title()} (in £) for each day. "
            labels = str([x.strftime('%d %b') for x in data['day'].tolist()])
            cost = str(['{:.2f}'.format(x/100) for x in data.total_cost.tolist()])

            table = '<TABLE><TR><TH>Monthly Quantity (kwh)</TH><TD>{:.1f}</TD></TR>'.format(data.total_quantity.sum())
            table += '<TR><TH>Total {} (£)</TH><TD>{:.2f}</TD></TR>'.format(costrev.title(), data.total_cost.sum()/100)
            avg = 48*data.total_cost.sum()/data.numperiods.sum()/100
            table += '<TR><TH>Avg Daily {} (£)</TH><TD>{:.2f}</TD></TR>'.format(costrev.title(), avg)
            avg = 48*(data.total_quantity).sum()/data.numperiods.sum()
            table += '<TR><TH>Avg Daily Quantity (kwh)</TH><TD>{:.2f}</TD></TR>'.format(avg)
            avg = data.total_cost.sum()/data.total_quantity.sum()
            table += '<TR><TH>Avg Price (p/kwh)</TH><TD>{:.2f}</TD></TR></TABLE>'.format(avg)

            table += f'<BR><TABLE><TR><TH>Day</TH><TH>Quantity</TH><TH>{costrev.title()}</TH><TH>Average<BR>Price</TH></TR>'
            for _, j in data.iterrows():
                t = '<TR><TD><A HREF="{}">{}</A></TD><TD>{:.3f}</TD><TD>{:.3f}</TD><TD>{:.3f}</TD></TR>'
                table += t.format(adj_url(url, [],[('day',j.day.day)]),
                                j.day.strftime('%a %b %d'), j.total_quantity, j.total_cost/100, j.price)
            table += '</TABLE>'  
    else:        
        heading = 'Monthly {}'.format(type.capitalize())
        navbar = getnavbar(request)
        description = f"{costrev.title()} (in £) for each month. "
        labels = str([x.strftime('%b-%Y') for x in data['month'].tolist()])
        cost = str(['{:.2f}'.format(x/100) for x in data['total_cost'].tolist()])
        table = '<TABLE><TR><TH>Total Quantity (kwh)</TH><TD>{:.1f}</TD></TR>'.format(data.total_quantity.sum())
        table += '<TR><TH>Total {} (£)</TH><TD>{:.2f}</TD></TR>'.format(costrev.title(), data.total_cost.sum()/100)
        avg = 48*data.total_cost.sum()/data.numperiods.sum()/100
        table += '<TR><TH>Avg Daily {} (£)</TH><TD>{:.2f}</TD></TR>'.format(costrev.title(), avg)
        avg = 48*(data.total_quantity).sum()/data.numperiods.sum()
        table += '<TR><TH>Avg Daily Quantity (kwh)</TH><TD>{:.2f}</TD></TR>'.format(avg)
        avg = data.total_cost.sum()/data.total_quantity.sum()
        table += '<TR><TH>Avg Price (p/kwh)</TH><TD>{:.2f}</TD></TR></TABLE>'.format(avg)
        table += f'<BR><TABLE><TR><TH>Month</TH><TH>Total<BR>Quantity</TH><TH>Total<BR>{costrev.title()}</TH>' 
        table += f'<TH>Daily<BR>{costrev.title()}</TH><TH>Daily<BR>Quantity</TH><TH>Average<BR>Price</TH></TR>'
        for _, j in data.iterrows():
            t = '<TR><TD><A HREF="{}">{}</A></TD><TD>{:.3f}</TD><TD>{:.3f}</TD><TD>{:.3f}</TD><TD>{:.3f}</TD><TD>{:.3f}</TD></TR>'
            dq = 48*(j.total_quantity/j.numperiods)
            dc = 48*(j.total_cost/j.numperiods)/100
            table += t.format(adj_url(url, [],[('month',j.month.strftime('%Y-%m'))]),
                                j.month.strftime('%b %Y'), j.total_quantity, j.total_cost/100, dc, dq, j.price)
        table += '</TABLE>'  

    if type_id!=2:
        description += 'All costs include 5% VAT and exclude standing charges. '

    if 'chartscale' in request.GET:
        c = request.GET.get('chartscale').split(',')
        chartscale = f'min: {c[0]}, '
        if len(c)>1:
            chartscale += f'max: {c[1]}, '
    else:
        chartscale = 'suggestedMin: 0'

    with open(os.path.dirname(os.path.realpath(__file__))  + "/templates/chart_template.html", "r") as f:
        inner = f.read()
    kwargs = {'navbar': navbar + gaswarn,
            'description': description,
            'chartlabels': labels,
            'quantity': cost,
            'table': table,
            'chartscale': chartscale,
            }    
    for k, v in kwargs.items():
        inner = inner.replace('{' + k + '}', v)

    output = create_sm_page(request, inner, heading)
    return output

def emissionsPage(request):
    smid = get_sm_id(request)
    url = request.get_full_path()
    start = request.GET.get('start', '2018-01-01')
    end = request.GET.get('end','2025-01-01')   

    if 'month' in request.GET:
        month = request.GET.get('month')
        if 'day' in request.GET:
            day = int(request.GET.get('day'))
            start = month + '-{:02d}'.format(day)
  
            endstr = f"""
            select period local_date, local_time, prof_qty*intensity prof_emis, 
            prof_qty, actual_qty*intensity actual_emis, actual_qty
            from fulldata where local_date='{start}'
            order by period"""
        else:
            endstr = f"""
            select local_date, sum(prof_qty*intensity) prof_emis, 
            sum(prof_qty) prof_qty, sum(actual_qty*intensity) actual_emis, sum(actual_qty) actual_qty
            from fulldata
            where date_trunc('month', local_date)='{month}-01' 
            group by local_date
            order by local_date"""
    else:
        endstr = """
        select date_trunc('month', local_date) as month, sum(prof_qty*intensity) prof_emis, 
        sum(prof_qty) prof_qty, sum(actual_qty*intensity) actual_emis, sum(actual_qty) actual_qty
        from fulldata
        group by month
        order by month"""

  

    s = f'''
    with periods as (select * from sm_periods where local_date between '{start}' and '{end}' )
    , quantities as ({quantitystr(smid, 0)})
    , emis as (select period_id, value from sm_hh_variable_vals v
                inner join sm_variables var on v.var_id=var.var_id and var.product='CO2_National')
    , prof as (select period_id, value from sm_hh_variable_vals v
                inner join sm_variables var on v.var_id=var.var_id and var.product='Profile_1')
    , fulldata as 
        (select periods.*, emis.value intensity, prof.value prof_qty, coalesce(quantities.quantity, 0) actual_qty
        from periods inner join emis on periods.period_id=emis.period_id
        inner join prof on periods.period_id=prof.period_id 
        left outer join quantities on periods.period_id=quantities.period_id)
        {endstr}
    '''  
 
    data = loadDataFromDb(s, returndf=True) 
    if data.shape[0]==0:
        return nodata(request)
        
    data['prof_intensity'] = data.prof_emis/data.prof_qty
    data['actual_intensity'] = np.where(data.actual_qty==0, data.prof_intensity, data.actual_emis/data.actual_qty)
    if 'month' in request.GET:
        month = request.GET.get('month')
        if 'day' in request.GET:
            data['prof_emis'] = data.prof_emis*data.actual_qty.sum()/data.prof_qty.sum()
            data['prof_qty'] = data.prof_qty*data.actual_qty.sum()/data.prof_qty.sum()
            day = int(request.GET.get('day'))
            heading = 'Half-hourly Carbon Intensity on {}-{:02d}'.format(month, day)
            navbar = getnavbar(request)
            description = 'Carbon intensity for average domestic profile and this customer in g/kwh for each half hour (labels are start-times, in local time). '
            labels = str(data['local_time'].tolist())
            profile = str(['{:.2f}'.format(x) for x in data['prof_qty'].values])
            actual = str(['{:.2f}'.format(x) for x in data['actual_qty'].values])
            intensity = str(['{:.2f}'.format(x) for x in data['prof_intensity'].values])
        
            avg = data.prof_emis.sum()/data.prof_qty.sum()
            table = '<TABLE><TR><TH>Average Domestic Intensity (g/kwh)</TH><TD>{:.1f}</TD></TR>'.format(avg)
            if data.actual_qty.sum()>0:
                avg = data.actual_emis.sum()/data.actual_qty.sum()
            table += '<TR><TH>Your Intensity (g/kwh)</TH><TD>{:.1f}</TD></TR>'.format(avg)
            table += '<TR><TH>Your Emissions (kg)</TH><TD>{:.2f}</TD></TR></TABLE>'.format(data.actual_emis.sum()/1000)
            table += '<BR><TABLE><TR><TH>Period Start</TH><TH>Intensity</TH><TH>Avg Domestic<BR>Consumption</TH><TH>Your<BR>Consumption</TH><TH>Your<BR>Emissions</TH></TR>'
            for _, j in data.iterrows():
                t = '<TR><TD>{}</TD><TD>{:.0f}</TD><TD>{:.2f}</TD><TD>{:.2f}</TD><TD>{:.2f}</TD></TR>'
                table += t.format(j.local_time, j.prof_intensity, j.prof_qty, j.actual_qty, j.actual_emis/1000 )
            table += '</TABLE>'
            
        else:
            heading = 'Daily Carbon Intensity for {}'.format( month)
            navbar = getnavbar(request)
            description = "Carbon intensity for average domestic profile and this customer in g/kwh for each day."
            labels = str([x.strftime('%d %b') for x in data['local_date'].tolist()])
            profile = str(['{:.2f}'.format(x) for x in data['prof_intensity'].values])
            actual = str(['{:.2f}'.format(x) for x in data['actual_intensity'].values])
            avg = data.prof_emis.sum()/data.prof_qty.sum()
            table = '<TABLE><TR><TH>Average Domestic Intensity (g/kwh)</TH><TD>{:.1f}</TD></TR>'.format(avg)
            if data.actual_qty.sum()>0:
                avg = data.actual_emis.sum()/data.actual_qty.sum()
            table += '<TR><TH>Your Intensity (g/kwh)</TH><TD>{:.1f}</TD></TR>'.format(avg)
            table += '<TR><TH>Your Consumption (kwh)</TH><TD>{:.1f}</TD></TR>'.format(data.actual_qty.sum())
            table += '<TR><TH>Your Emissions (kg)</TH><TD>{:.1f}</TD></TR></TABLE>'.format(data.actual_emis.sum()/1000)
            table += '<BR><TABLE><TR><TH>Day</TH><TH>Avg Domestic Intensity</TH><TH>Your Intensity</TH><TH>Your Consumption</TH><TH>Your Emissions</TH></TR>'
            for _, j in data.iterrows():
                t = '<TR><TD><A HREF="{}">{}</A></TD><TD>{:.1f}</TD><TD>{:.1f}</TD><TD>{:.1f}</TD><TD>{:.1f}</TD></TR>'
                table += t.format(adj_url(url, [],[('day',j.local_date.day)]),
                                j.local_date.strftime('%a %b %d'), j.prof_intensity, j.actual_intensity, j.actual_qty, j.actual_emis/1000)
            table += '</TABLE>'  
    else:        
        heading = 'Monthly Carbon Intensity'
        navbar = getnavbar(request)
        description = "Carbon intensity for average domestic profile and this customer in g/kwh for each month"
        labels = str([x.strftime('%b-%Y') for x in data['month'].tolist()])
        profile = str(['{:.1f}'.format(x) for x in data['prof_intensity'].values])
        actual = str(['{:.1f}'.format(x) for x in data['actual_intensity'].values])
        avg = data.prof_emis.sum()/data.prof_qty.sum()
        table = '<TABLE><TR><TH>Average Domestic Intensity (g/kwh)</TH><TD>{:.1f}</TD></TR>'.format(avg)
        if data.actual_qty.sum()>0:
            avg = data.actual_emis.sum()/data.actual_qty.sum()
        table += '<TR><TH>Your Intensity (g/kwh)</TH><TD>{:.1f}</TD></TR>'.format(avg)
        table += '<TR><TH>Your Consumption (kwh)</TH><TD>{:.1f}</TD></TR>'.format(data.actual_qty.sum())
        table += '<TR><TH>Your Emissions (kg)</TH><TD>{:.1f}</TD></TR></TABLE>'.format(data.actual_emis.sum()/1000)

        table += '<BR><TABLE><TR><TH>Month</TH><TH>Avg Domestic<BR>Intensity</TH><TH>Your<BR>Intensity</TH>' 
        table += '<TH>Your<BR>Consumption</TH><TH>Your Emissions</TH></TR>'
        for _, j in data.iterrows():
            t = '<TR><TD><A HREF="{}">{}</A></TD><TD>{:.1f}</TD><TD>{:.1f}</TD><TD>{:.1f}</TD><TD>{:.1f}</TD></TR>'
            table += t.format(adj_url(url, [],[('month',j.month.strftime('%Y-%m'))]),
                                j.month.strftime('%b %Y'), j.prof_intensity, j.actual_intensity, j.actual_qty, j.actual_emis/1000)
        table += '</TABLE>'  

    kwargs = {'navbar': navbar,
                'description': description,
                'chartlabels': labels,
                'profile': profile,
                'actual': actual,
                'table': table + '<BR><BR>'
                }  

    if 'day' in request.GET:
        with open(os.path.dirname(os.path.realpath(__file__))  + "/templates/chart_emis2.html", "r") as f:
            inner = f.read()        
            kwargs['intensity'] = intensity
    else:   
        with open(os.path.dirname(os.path.realpath(__file__))  + "/templates/chart_emis.html", "r") as f:
            inner = f.read()        

   
    for k, v in kwargs.items():
        inner = inner.replace('{' + k + '}', v)

    output = create_sm_page(request, inner, heading)
    return output


def netimportPage(request):
    smid = get_sm_id(request)
    start = request.GET.get('start', '2018-01-01')
    end = request.GET.get('end','2025-01-01')    

    if 'month' in request.GET:
        month = request.GET.get('month')
        if 'day' in request.GET:
            day = int(request.GET.get('day'))
            start = month + '-{:02d}'.format(day)
            end = start
            endstr = """
            select local_date, local_time as local_time_start, timezone_adj, import as total_import, export as total_export
            from fulldata order by period"""
        else:
            endstr = f"""
            select local_date as day, count(period_id) as numperiods, sum(import) as total_import, sum(export) as total_export
            from fulldata where date_trunc('month', local_date)='{month}-01'
            group by local_date order by local_date"""
    else:
        endstr = f"""
            select date_trunc('month',local_date) as month, count(period_id) as numperiods, 
            sum(import) as total_import,
            sum(export) as total_export
            from fulldata group by month order by month"""

    s = f'''
    with periods as (select * from sm_periods where local_date between '{start}' and '{end}' )
    , quantities1 as ({quantitystr(smid, 0)})
    , quantities2 as ({quantitystr(smid, 2)})
    , full1 as 
     (select periods.*, quantities1.quantity 
    from periods inner join quantities1 on periods.period_id=quantities1.period_id)
    , full2 as 
     (select periods.*, quantities2.quantity
    from periods inner join quantities2 on periods.period_id=quantities2.period_id)
    , fulldata as
    (select full2.*, coalesce(full1.quantity,0) as import, coalesce(full2.quantity,0) as export
    from full2 left outer join full1 on full2.period_id=full1.period_id)
        {endstr}
    ''' 
    #raise Exception(s)
    data = loadDataFromDb(s, returndf=True)
    url = request.get_full_path()
    
    if data.shape[0]==0:
        return nodata(request)
 
    #raise Exception(request.GET)
    if 'month' in request.GET:
        month = request.GET.get('month')
        if 'day' in request.GET:
            day = int(request.GET.get('day'))
            heading = 'Half-hourly Net Import on {}-{:02d}'.format(month, day)
            navbar = getnavbar(request)
            description = f'Import and export for each half hour (in kWh, labels are start-times, in local time)'
            labels = str(data['local_time_start'].tolist())
            imports = str(['{:.2f}'.format(x) for x in data['total_import'].values])
            exports = str(['{:.2f}'.format(x) for x in data['total_export'].values])
            table = '<TABLE><TR><TH>Total Import</TH><TD>{:.3f}</TD></TR>'.format(data.total_import.sum())
            table += '<TR><TH>Total Export</TH><TD>{:.3f}</TD></TR>'.format(data.total_export.sum())
            table += '<TR><TH>Total Net Import</TH><TD>{:.3f}</TD></TR></TABLE>'.format(data.total_import.sum()-data.total_export.sum())
            table += '<BR><TABLE><TR><TH>Period Start</TH><TH>Import</TH><TH>Export</TH><TH>Net Import</TH></TR>'
            for _, j in data.iterrows():
                t = '<TR><TD>{}</TD><TD>{:.3f}</TD><TD>{:.3f}</TD><TD>{:.3f}</TD></TR>'
                table += t.format(j.local_time_start, j.total_import, j.total_export, j.total_import-j.total_export)
            table += '</TABLE>'
            
        else:
            heading = 'Daily Net Import for {}'.format(month)
            navbar = getnavbar(request)
            description = f"Daily import and export (in kWh) for each day"
            labels = str([x.strftime('%d %b') for x in data['day'].tolist()])
            imports = str(['{:.2f}'.format(x) for x in data.total_import.tolist()])
            exports = str(['{:.2f}'.format(x) for x in data.total_export.tolist()])

            table = '<TABLE><TR><TH>Total Import</TH><TD>{:.3f}</TD></TR>'.format(data.total_import.sum())
            table += '<TR><TH>Total Export</TH><TD>{:.3f}</TD></TR>'.format(data.total_export.sum())
            table += '<TR><TH>Total Net Import</TH><TD>{:.3f}</TD></TR></TABLE>'.format(data.total_import.sum()-data.total_export.sum())
            table += '<BR><TABLE><TR><TH>Day</TH><TH>Import</TH><TH>Export</TH><TH>Net Import</TH></TR>'
            for _, j in data.iterrows():
                t = '<TR><TD><A HREF="{}">{}</A></TD><TD>{:.3f}</TD><TD>{:.3f}</TD><TD>{:.3f}</TD></TR>'
                table += t.format(adj_url(url, [],[('day',j.day.day)]),
                                j.day.strftime('%a %b %d'), j.total_import, j.total_export, j.total_import-j.total_export)
            table += '</TABLE>'  
    else:        
        heading = 'Monthly Net Import'
        navbar = getnavbar(request)
        description = f"Monthly import and export (in kWh) for each month:"
        labels = str([x.strftime('%b-%Y') for x in data['month'].tolist()])
        imports = str(['{:.2f}'.format(x) for x in data.total_import.tolist()])
        exports = str(['{:.2f}'.format(x) for x in data.total_export.tolist()])
        table = '<TABLE><TR><TH>Total Import</TH><TD>{:.3f}</TD></TR>'.format(data.total_import.sum())
        table += '<TR><TH>Total Export</TH><TD>{:.3f}</TD></TR>'.format(data.total_export.sum())
        table += '<TR><TH>Net Import</TH><TD>{:.3f}</TD></TR></TABLE>'.format(data.total_import.sum()-data.total_export.sum())

        table += '<BR><TABLE><TR><TH>Month</TH><TH>Total<BR>Import</TH><TH>Total<BR>Export</TH><TH>Net<BR>Import</TH></TR>'
        for _, j in data.iterrows():
            t = '<TR><TD><A HREF="{}">{}</A></TD><TD>{:.3f}</TD><TD>{:.3f}</TD><TD>{:.3f}</TD></TR>'
            table += t.format(adj_url(url, [],[('month',j.month.strftime('%Y-%m'))]),
                                j.month.strftime('%b %Y'), j.total_import, j.total_export, j.total_import-j.total_export)
        table += '</TABLE>'  
    #raise Exception
    if 'chartscale' in request.GET:
        c = request.GET.get('chartscale').split(',')
        chartscale = f'min: {c[0]}, '
        if len(c)>1:
            chartscale += f'max: {c[1]}, '
    else:
        chartscale = 'suggestedMin: 0'

    with open(os.path.dirname(os.path.realpath(__file__))  + "/templates/chart_template3.html", "r") as f:
        inner = f.read()


    kwargs = {'navbar': navbar ,
            'description': description,
            'chartlabels': labels,
            'imports': imports,
            'exports': exports,
            'table': table+'<BR>',
            'chartscale': chartscale,
            }    
    for k, v in kwargs.items():
        inner = inner.replace('{' + k + '}', v)

    output = create_sm_page(request, inner, heading)
    return output
