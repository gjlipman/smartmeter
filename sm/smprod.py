from django.http import HttpResponse, HttpRequest
from django.shortcuts import render, redirect
import os
import sys
import pandas as pd
import numpy as np

from myutils.utils import (getConnection, loadDataFromDb)
from myutils.smutils import (adj_url, sidebarhtml, 
                            getRegion, getregions, menuitems, octopusmeters, n3rgymeters,
                            loadSmData, deleteSmData, get_sm_id, isdemo, sm_log,
                            get_type_id, getmode, create_sm_page, getTariff)



def checkRequest(request):
    choice = request.path_info.split('/')[-1]
    url = request.get_full_path()
    smid = get_sm_id(request)

    if choice=='':
        if '?' in url:
            url = url.replace('?','home?')
        else:
            url = url + 'home'
        return redirect(url)
    elif choice in ['sm','smartmeter']:
        url = url.replace(choice, f'{choice}/home')
        return redirect(url)

    if 'MAC' in request.GET:
        return redirect(adj_url(url, ['MAC'],[('n3rgy', request.GET.get('MAC'))]))

    if isdemo(request):
        if ('tariff' not in request.GET) and (choice in ['cost']):
            u = adj_url(url, [], [('tariff', '15')])
            return redirect(u)

    if choice in ['home', 'admin', 'info', 'getstarting', 'checks', 'logpage','gastracker']:
        return choice

    type_id, type_label = get_type_id(choice)   

    if type_id==-1: 
        content = """
        <P>This functionality is not yet implemented. Please try another page from the menu or check back later.</P>"""
        return create_sm_page(request, content, 'Invalid Page')

    if smid is not None:
        s = f"select account_id from sm_accounts where session_id= '{smid}' and type_id={type_id} and active='1'"
        account_id = loadDataFromDb(s)
    else:
        account_id = []

    if len(account_id)==0:
        adminurl = url.replace(choice, 'admin', 1)
        heading = 'No {} data stored for this user'.format(type_label)
        if type_id==0:
            if isdemo(request):
                content = """
                <P>There is no {} data stored for the demo account. Please try back later, or you can load your own data via the <A HREF="{}">Admin Page</A>, for which you will need your Octopus or n3rgy key.</P>"""
            else:
                content = """
                <P>There is no {} data stored for you. Please visit the <A HREF="{}">Admin Page</A> to check what data is stored, and load additional data. Alternatively, remove the Octopus or n3rgy key from the url to access the demo account data.</P>"""          
        else:
            if isdemo(request):
                content = """
                <P>There is no {} data stored for the demo account, only Electricity. Alternatively you can load your own data via the <A HREF="{}">Admin Page</A>, for which you will need your Octopus or n3rgy key.</P>"""
            else:
                content = """
                <P>There is no {} data stored for you. Please visit the <A HREF="{}">Admin Page</A> to check what data is stored, and load additional data. </P>"""          
              
        content = content.format(type_label, adminurl) 
        return create_sm_page(request, content, heading)

    if choice in ['cost','gascost','exportrevenue']:
        prefix = ['', 'gas', 'export'][type_id]
        tariff = request.GET.get(prefix+'tariff', None)
        if tariff is None:
            return getTariff(request, choice)
        else:
            isfixed = tariff.replace(',','').replace(':','').replace('-','').replace('.','').isnumeric()
            if (isfixed==0):
                if 'region' not in request.GET:
                    return getTariff(request, choice) 
                region = request.GET.get('region')
                s = "select var_id, granularity_id from sm_variables where product='{}' and region='{}'".format(tariff, region)
                s = loadDataFromDb(s)  
                if len(s)==0:
                    return getTariff(request, choice)   

    return choice
    

def homepagewhatwehave(request, df):
    mode = request.GET.get('mode','111')
    labels = ['Electricity Consumption','Gas Consumption','Electricity Export']
    s = '<UL>'
    for i in [0,2,1]:
        if ( mode[i]=='1') and (i in df.type_id.unique()):
            j = df[df.type_id==i].iloc[0]
            s += f'<LI>{labels[i]} data from {j.first_period} to {j.last_period} </LI>'
    s += '</UL>'
    urladmin = request.get_full_path().replace('home','admin',1)
    s += f'<P>For more details of the available data visit the <A HREF="{urladmin}">Admin Page</A>.</P>'
    return s





def homepage(request):
    urladmin = request.get_full_path().replace('home','admin',1)
    smid = get_sm_id(request) 
    if smid is not None:
        sql = f"select type_id, first_period, last_period, source_id, region from sm_accounts where session_id= '{smid}' and active='1' order by type_id;"
        df = loadDataFromDb(sql, returndf=True)
    else:
        df = pd.DataFrame()

    s = """<P>This website allows UK energy customers with smart meters to access their energy consumption data,  
            and to perform a range of calculations on the back of this, for example reconciling bills and tracking carbon intensity. </P>"""
    u = request.get_full_path().replace('home','getstarting')
    s += f'<P>If you are new to this site, I recommend taking a look at the <A HREF="{u}">Getting Started</A> page.</P>'

    if isdemo(request):
        if df.shape[0]==0:
            s += """<P>You are running this in demo mode (ie without a key), which is designed to show you the functionality with 
                    data from a made up user. Unfortunately for some reason, the server is missing the data for the made up user. Please try 
                    again later. Alternatively, you can load your own data from Octopus or n3rgy, by selecting the option from the dropdown list below: </P>
            """
        else:
            s += """<P>You are running this in demo mode (ie without a key), which is designed to show you the functionality with 
                    electricity consumption data from a made up user. You can try out the functionality using this demo data. 
                    Alternatively, you can load your own data from Octopus or n3rgy (which can include gas consumption or electricity export), 
                    by selecting the option from the dropdown list below. </P>
            """
        s += formforkeys(request) 
    else:
        if df.shape[0]==0:
            s += f"""<P>The server no longer has the data for this user (it is deleted within 7 days of last being accessed). 
            You can load new data from Octopus or n3rgy on the <A HREF="{urladmin}">Admin Page</A>. </P>
            """
        else:
            s += """<P>The server has the following data for this user:</P>"""
            s += homepagewhatwehave(request, df)
    s += '</DIV>'
    output = create_sm_page(request, s, 'Welcome')
    return output


def seewhatwehave(request):
    smid = get_sm_id(request)
    if smid is None:
        return pd.DataFrame(), pd.DataFrame(), ''
    df = loadDataFromDb(f"select * from sm_accounts where session_id = '{smid}' and active='1' order by type_id", returndf=True)
    df['type'] = df['type_id'].map({0: 'Electricity', 1: 'Gas', 2:'Electricity Export'})
    df['source_id'].fillna(0, inplace=True)
    df['source'] = df['source_id'].map({0: 'n3rgy', 1: 'Octopus'})
    s = f"""
        select sm_accounts.account_id, sm_accounts.type_id, sm_periods.period from sm_accounts 
        left join sm_periods on sm_periods.period between sm_accounts.first_period and sm_accounts.last_period 
        left join sm_quantity on sm_quantity.period_id=sm_periods.period_id and sm_quantity.account_id= sm_accounts.account_id
        where sm_quantity.quantity is null and sm_accounts.session_id='{smid}' and sm_accounts.active='1'
        """
    df_gaps = loadDataFromDb(s, returndf=True)
    df2 = df_gaps.groupby('account_id').count()
    df['gaps'] = df.account_id.map(df2.period)
    df.gaps.fillna(0, inplace=True)
    df_gaps.sort_values(['type_id','period'], ascending=[True, False], inplace=True)
    s3 = '<P>All times on this page are in UTC.</P><TABLE><TR><TH>ID</TH><TH>Type</TH><TH>First Period</TH><TH>Last Period</TH><TH>Gaps</TH><TH>Last Updated</TH><TH>Source</TH></TR>'
    for i, j in df.iterrows():
        s3a = '<TR><TD>{}</TD><TD>{}</TD><TD>{}</TD><TD>{}</TD><TD>{}</TD><TD>{}</TD><TD>{}</TD></TR>'
        g = '0' if j.gaps == 0 else '{:.0f}'.format(j.gaps) 
        s3 += s3a.format(j.account_id, j.type, j.first_period, j.last_period, g, str(j.last_updated)[:16], j.source)
    s3 += '</TABLE><BR>'
    return df, df_gaps, s3


def formforkeys(request):

    url = request.get_full_path().replace('home','admin',1)
    s = f"""
        <form action="{url}" method="get">
        <div class="form-group row">
        <label for="inputEmail3" class="col-sm-2 col-form-label" >Source: </label>
        <div class="col-sm-10">
            <select class="form-control" id="source" name="source" select >
                    <option value="n3rgy">n3rgy</option>
                    <option value="octopus">Octopus</option>
            </select>
        </div>
        </div>

        <div class="form-group row">
        <div class="col-sm-10">
            <button type="submit" class="btn btn-primary">Check Data Source</button>
        </div>
        </div>
    </form>
    """
    return s


def adminPage(request):
    url = request.get_full_path()
    if request.method=='POST':
        task = list(request.POST.keys())[0]
        if isdemo(request):
            raise Exception('You must use your n3rgy code or your octopus code to load or delete data. Click Back to return to the Admin screen.')
        if task == 'delete':
            smid = get_sm_id(request)
            deleteSmData(smid)
            output = redirect(adj_url(url, [], [('mode','101')]))
            for k, v in request.COOKIES.items():
                if k[:5]=='sm_id':
                    output.set_cookie(k,  v, max_age=-1)
            return output
        
        if task == 'loadelectricity':
            smid = loadSmData(request, 0)
        elif task == 'loadgas':
            smid = loadSmData(request, 1)
        elif task == 'loadexport':
            smid = loadSmData(request, 2)
        else:
            raise Exception(str(request.POST.keys()))
        sm_log(request, 'admin', smid)
        mode = getmode(smid)
        output = redirect(adj_url(url, [], [('mode',mode)]))
        key = 'sm_id_' + request.GET.get('octopus', request.GET.get('n3rgy',None))[-3:]
        output.set_cookie(key,  smid, max_age=3600)
        return output

    smid = get_sm_id(request)
    types = ['Electricity','Gas','Export']
    if 'source' in request.GET:
        source = request.GET.get('source')
        if source == 'octopus':
            sample = 'A-ABCD1234sk_live_BCmPlrj6LwktwfYvosMRePcd'
            s = '''
            <P>If you are an Octopus customer, you can access your data using a key made up of your account 
            (eg A-ABCD1234 - you can find this on your bill) followed by your security key (which should be like
            sk_live_BCmPlrj6LwktwfYvosMRePcd, which you can find on 
            the <A HREF="https://octopus.energy/dashboard/developer/" target="_blank">Octopus developer page</A>). 
            These two strings should be joined together without any gap between them, eg A-ABCD1234sk_live_BCmPlrj6LwktwfYvosMRePcd.</P>
            <P>Your Octopus key will never be stored by the website. You may later choose to store consumption data, however you will be 
            given further details before you do.</P>
            '''
        elif source == 'n3rgy':
            sample = 'B816B327CE29A3C1'
            s = '''
            <P>If you have registered with n3rgy, you will have registered using your MAC, which is a unique 16 digit string
            printed on your in-house display (something like B816B327CE29A3C1 - don't include any hyphens or spaces). 
            If you have not yet registered with n3rgy, you can do for free by going to 
            <A HREF="https://data.n3rgy.com/home" target="_blank">their website</A> and clicking on the "I'm a Consumer" button 
            in the top right corner. </P>
            <P>Your n3rgy key will never be stored by the website. You may later choose to store consumption data, however you will be 
            given further details before you do.</P>

            '''
        url = adj_url(url, ['source','octopus','hash'], [])
        s += f"""
                <form action="{url}" method="get">
                <div class="form-group row">
                <label for="inputEmail3" class="col-sm-2 col-form-label">Key</label>
                <div class="col-sm-10">
                <input type="text" class="form-control" name="{source}" value="{sample}">
                </div>
                </div>

                <div class="form-group row">
                <div class="col-sm-10">
                    <button type="submit" class="btn btn-primary">Check for data</button>
                </div>
                </div>
            </form>
            """

        return create_sm_page(request, s, 'Smart Meter Admin')    

    s = ''
    
    data, df_gaps, tablestr = seewhatwehave(request)
    choosesource = formforkeys(request)      

    if isdemo(request):
        if data.shape[0]==0:
            s += """
                <P>You are currently looking at this page without providing an account key, so it is just attempting to display 
                data for the demo account. However, it can't currently find any data for the demo account. However, if you would like 
                to load your own data from Octopus or n3rgy, select the source from the dropdown list below.</P>"""
            s += choosesource    
        else:
            s += """
                <P>You are currently looking at this page without providing an account key, so it will just display data
                for the <B>demo account</B>. Alternatively, if you would would like to see your own data, you can 
                select one of Octopus or n3rgy as a source from the dropdown below.<P>
                """
            s += choosesource
            s += """
                 <H3>Currently Loaded Data</H3>
                 """
            s += tablestr


    else:
        source = 'Octopus' if 'octopus' in request.GET else 'n3rgy'
        s += f"""
        <P>This is the Admin page for your account where you can see the data we have for you, and any additional data from 
        {source} that you may wish to load. You can also delete all your data from this server.  </P>
        """
        if data.shape[0]>0:
            s += "<H4>Currently Loaded Data</H4>"
            s += tablestr
            numgaps = df_gaps.shape[0]
            if numgaps>0:
                s += f'<P><B>You have {numgaps} gaps in the data we have stored</B>. '
                if request.GET.get('hidegaps','0')=='0':
                    s += 'These are shown at the bottom of the page.</P>'
                else:
                    s += 'If you want to see these, remove the hidegaps=1 flag in the url.'
            s += f'''
                <P>Any data will be deleted from the server up to 7 days after it is last updated. You can delete the data manually 
                at any point using the following buttons.</P>
                <form action="{url}" method="post">
            <input type="submit" name="delete" value="Delete Data">
            </form> <BR><BR> '''

        else:
            s += "<P>We currently have no data stored for this account.</P>"
        s += """
        <script type="text/javascript">
        var submit;
        var _formConfirm_submitted = false;
        function checkForm(form) // Submit button clicked
        {
            submit.disabled = true;
            submit.value = "Please wait...";
            return true;
        }

        function Clicked(button)
        {
        submit= button ;
        }

        </script>
        """


        if source=='Octopus':
            key = request.GET.get('octopus')
            if request.GET.get('includeprice','0')=='1':
                df = octopusmeters(key, getprices=True)
            else:
                df = octopusmeters(key, getprices=False)
            s += '<H4>Data Available from Octopus</H4>'
            if isinstance(df, tuple):
                s += '<P><B>Invalid key</B>. Your key should be the combination of your account number and your security key, with no characters in between. '
                if df[0]==404:
                    s += f'The Octopus API responded that it could not recognise account number {key[:10]}. '
                    s += 'This should be your account number, eg A-ABCD1234. You can find it on your bill or your Octopus webpage. '
                elif df[0]==401:
                    s += f'The Octopus API responded with a security error. This may mean that your security key {key[10:]} was wrong. '
                    s += 'You should be able to find it on the <A HREF="https://octopus.energy/dashboard/developer/" target="_blank">developer page</A> of the Octopus website. '
                    s += f'Alternatively, it may be that this is merely the wrong security key for the account {key[:10]}. ' 
                else:
                    s += f'Unknown error connecting to your account: {df[1]}. ' 
            elif df.shape[0]==0:
                s += '''
                    <P><B>We connected to your Octopus account, but couldn't find any meters with consumption data</B>. Please get in touch with 
                    me so I can investigate. 
                    '''
            else:

                if request.GET.get('includeprice','0')=='1':
                    s += '<TABLE><TR><TH>Type</TH><TH>MPAN</TH><TH>Meter</TH><TH>Last Time (UTC)</TH><TH>Tariff</TH><TH>Product</TH><TH>Region</TH><TH>Price</TH></TR>'
                    for i, j in df.iterrows():
                        s += f"<TR><TD>{types[j.type_id]}</TD><TD>{j.mpan}</TD><TD>{j.serial}</TD><TD>{j.laststart}</TD><TD>{j.tariff}</TD><TD>{j['product']}</TD><TD>{j.region}</TD><TD>{j['prices']}</TD></TR>"
                    s += '</TABLE>'
                else:
                    s += '<TABLE><TR><TH>Type</TH><TH>MPAN</TH><TH>Meter</TH><TH>Last Time (UTC)</TH><TH>Tariff</TH><TH>Product</TH><TH>Region</TH></TR>'
                    for i, j in df.iterrows():
                        s += f"<TR><TD>{types[j.type_id]}</TD><TD>{j.mpan}</TD><TD>{j.serial}</TD><TD>{j.laststart}</TD><TD>{j.tariff}</TD><TD>{j.tariff[5:-2]}</TD><TD>{j.tariff[-1]}</TD></TR>"
                    s += '</TABLE>'                    

                s += '<P>Please let me know if you have additional meters that you think I should be picking up.</P> '

                s += '''
                        <P>You can load the latest data to the server using the buttons below. <B>Clicking this button indicates
                        that you are happy for your data to be stored on the server</B>. Your security key will not be stored, nor will any 
                        other information that could be used to identify you. Your data will be deleted automatically within 7 days of you last 
                        loading data, and you can delete it manually at any time on this page.
                        <P>Note that at the moment each of these buttons will take about 10 seconds to load in the data. I will be working on optimising 
                        this over the coming days. </P>
                        <form onsubmit="if( _formConfirm_submitted == false ){ _formConfirm_submitted = true;return true }else{ return false;  }" '''
                s += f'''action="{url}" method="post" >'''
                j = []  
                if 0 in df.type_id.unique():
                    j.append('''<input type="submit" name="loadelectricity" value="Load Electricity Data"  onclick="this.value='Loading...';">''') 
                if 1 in df.type_id.unique():
                    j.append('''<input type="submit" name="loadgas" value="Load Gas Data" onclick="this.value='Loading...';">''')
                if 2 in df.type_id.unique():
                    j.append('''<input type="submit" name="loadexport" value="Load Export Data" onclick="this.value='Loading...';">''')
                s += '&nbsp;&nbsp;&nbsp;'.join(j) + '</form><BR>'
  
        if source == 'n3rgy':
            key = request.GET.get('n3rgy')
            n3adj = int(request.GET.get('n3adj','0'))
            ndata = n3rgymeters(key, n3adj)
            
            s += '<H4>Data Available from n3rgy</H4>'
            if isinstance(ndata, tuple):
                s += "<P><B>Invalid key</B>. Your key should be the in-home display's MAC code that you used to set up your n3rgy account."
                s += " it should be 16 digits without any spaces or dashes in between the characters. "
                s += f"Error code: {ndata[0]}, message: {ndata[1]}"
            else:
                found = [x[0] for x in ndata if x[1]==200]
                numvalid = len(found)
                if numvalid==0:
                    s += "<P><B>We connected to your n3rgy account. but couldn't find any meters with data</B>.</P> "
                
                if numvalid>0:
                    s += '<P>We connected to your n3rgy account, and the following data types were found:</P>'
                    s += '<TABLE><TR><TH>Type</TH><TH>LastDateTime (UTC)</TH></TR>'    
                    for i in range(3):
                        if ndata[i][1]==200:
                            s += f"<TR><TD>{types[i]}</TD><TD>{ndata[i][2]}</TD></TR>"
                    s += '</TABLE>'

                if numvalid<3 and False:
                    s += "<P>We had problems accessing the following data types.</P> "
                    s += '<TABLE><TR><TH>Type</TH><TH>Status</TH><TH>Message</TH></TR>'
                    
                    for i in range(3):
                        if ndata[i][1]!=200:
                            s += f"<TR><TD>{types[i]}</TD><TD>{ndata[i][1]}</TD><TD>{ndata[i][2]}</TD></TR>"
                    s += '</TABLE><BR>'

                if numvalid>0:   
                    s += '''<P>You can load the latest data to the server using the buttons below. <B>Clicking this button indicates 
                            that you are happy for your data to be stored on the server</B>. Your security key will not be stored, nor will any 
                            other information that could be used to identify you. Your data will be deleted automatically within 7 days of you last 
                            loading data, and you can delete it manually at any time on this page.
                            <P>Note that at the moment each of these buttons will take about 10 seconds to load in the data. I will be working on optimising 
                            this over the coming days. </P>
                            <form onsubmit="if( _formConfirm_submitted == false ){ _formConfirm_submitted = true;return true }else{ return false;  }" '''
                    s += f'''action="{url}" method="post" >'''
                    j = []  
                    if 0 in found:
                        j.append('''<input type="submit" name="loadelectricity" value="Load Electricity Data" onclick="this.value='Loading...';">''') 
                    if 1 in found:
                        j.append('''<input type="submit" name="loadgas" value="Load Gas Data" onclick="this.value='Loading...';">''')
                    if 2 in found:
                        j.append('''<input type="submit" name="loadexport" value="Load Export Data" onclick="this.value='Loading...';">''')
                    s += '&nbsp;&nbsp;&nbsp;'.join(j) + '</form><BR><BR>'
   
        if df_gaps.shape[0]>0:
            if request.GET.get('hidegaps','0')=='0':
                df_gaps['type'] = df_gaps.type_id.map({0: 'Electricity', 1: 'Gas', 2:'Electricity Export'})
                s5 = """
                <P>The following table is a list of all gaps in your data, that is, any periods between the first period and last period 
                for which we don't have consumption data. If you click 'Load' above, it will try and fill these gaps. 
                However, it may be the case that n3rgy or Octopus have been unable to retrieve your data from your smart meter.</P>
                <TABLE><TR><TH>Type</TH><TH>Period</TH>
                """
                for _, j in df_gaps.iterrows():
                    s5 += '<TR><TD>{}</TD><TD>{}</TD></TR>'.format(j['type'], j['period'])
                s5 += '</TABLE><BR>'

                s += s5

    s += '</DIV>'
        
    output = create_sm_page(request, s, 'Smart Meter Admin')
    return output


def defaultDisplay(request):

    s = """
        <P>This functionality is not yet implemented. Please check back later.</P>
        </DIV>
    """
    output = create_sm_page(request, s, 'Smart Meter Data')
    return output


def moreinfo(request):
    s = """
            <H2>Troubleshooting</H2>
            <P>This website and underlying infrastructure has been put together by me over a few weeks, and is hosted on a 
            virtual server. I am working on improving it, but it is likely to have a few glitches for now. 
            Also, I am reliant on data that ultimately comes from smart meters, and these have had many many teething issues. 
            <P>If you do have issues, please do get in touch. At some point I'll set up a web form for you to contact me, but in 
            the meantime, please email guy dot j dot lipman at gmail dot com (replace dot with . and at with @), and let me know the url 
            where you had the problem.

            <H2>Getting my data</H2>
            <P>At the moment if you want to get your data, you will need a smart meter. If you have an account with Octopus Energy, your access key 
            will be the concatenation of your account number (should be something like A-ABCD1234, and appears in the url of the 
            <A HREF="https://octopus.energy/dashboard/" target="_blank">main dashboard</A> when you log in) and your API key 
            (which is a 32 character string and can be found on their <A HREF="https://octopus.energy/dashboard/developer/" target="blank">developer dashboard</A>).
            If you aren't with Octopus (or even if you are), you can register for a free consumer account with n3rgy by going to their 
            <A HREF="https://data.n3rgy.com/home" target="_blank">main website</A> and clicking on the "I'm a Consumer button in the top right corner. 
            In order to authenticate that you have the right to access your account information, you will need to provide n3rgy with 
            your electricity MPAN (which can be found on your latest bill), the MAC code of your in-house display (a 16 character string - leave out the hyphens), and 
            roughly when you moved into your home. Once you are registered with n3rgy, you will be able to access your data using your MAC code. 

            <H2>Suggestions</H2>
            <P>This website is a work in progress, so I would be very open to suggestions of things that would make it a lot more useful.
            In particular, if there are particular views of your data that you'd find useful, please let me know. And if
            you know of ways other than Octopus and n3rgy that customers can access their smart meter data, please let me know. The best way to get
            in touch is by email, on guy dot j dot lipman at gmail dot com (replace dot with . and at with @).

            <H2>The background to this project</H2>
            <P>As a long term energy geek, I was excited to get a smart meter in early 2018. Finally I would be able to know 
            how much electricity I was using, and when. 

            <P>Unfortunately I was quickly disappointed. There was an in-house display, but it didn't show much information, and didn't provide any way to get that data 
            onto my computer. It did save me from manually submitting meter readings, but I didn't entirely trust my supplier, 
            and worried that if anything went wrong I wouldn't have anything to point to.

            <P>Then at the end of 2018, I switched to <A HREF="https://octopus.energy/" target="_blank">Octopus Energy</A>, 
            and one of their big selling points for me was their API that lets customers access their consumption data. They also have 
            a tariff, Agile, which varies every half hour based on market prices. This offers a huge scope for saving, but also a need
            to be able to track all the consumption data and prices.</P>

            <P>Over the past 18 months I have been doing <A HREF="https://www.guylipman.com/octopus/" target="_blank">quite a bit of coding</A>, in both python and javascript, trying to make the best 
            use of the Octopus API. I have been part of a group of like-minded and clever people who are trying to do their bit to make it easier for
            customers <A HREF="https://octopus.energy/blog/agile-smart-home-diy/" target="_blank">to save money and the planet</A>.</P>
            
            <P>Obviously only a minority of smart meter customers are on Octopus, so I have also been thinking a lot
            about how to let other smart meter customers 
            make the most of the data they are collecting. Sure, many people don't want to worry about things like this,
            but if they can't see their data on demand, we can't be surprised when they don't trust the system.</P>
            
            <P>Over the past six months I had many conversations, with Octopus, with the <A HREF="https://serl.ac.uk/" target="_blank">Smart Energy Research Lab</A>, 
             <A HREF="https://www.hildebrand.co.uk/" target="blank">
            Hildebrand Technology</A>, <A HREF="https://data.n3rgy.com/home" target="_blank">n3rgy</A>, 
            Octopus, <A HREF="https://carbon.coop/" target="_blank">
            Carbon Coop</A> as well as with many smart meter users. 
            I wrote a <A HREF="https://medium.com/@guylipman/letting-people-access-their-electricity-data-e3d36ad9b6c0" target="_blank">blog post</A>
            on the topic, and even presented on my ideas as part of a job interview. </P>
            I finally found myself with a bit of time so I decided to give it a go.</P>

            <P>I'm not entirely sure where this project will go. At the moment I am volunteering my time to this, and 
            it is just hosted on a virtual server for which I'm paying £10/month. But I am still convinced that there are 
            a lot of smart meter customers that would like to know that they could get ready access to their data if they wanted.</P>

            
            """
    
    output = create_sm_page(request, s, 'More Information')
    return output    


def gettingStartedPage(request):
    s = """
    <P>This site is designed to let energy consumers with smart meters access the information 
    that these devices collect in a secure way. I want it to be something that is easy to use, but also gives
    reliable data and useful insights. This page is designed to help you get started as quickly as possible. 
    Initially it is limited to customers of Octopus Energy or those with an n3rgy account (these are free to set up and 
    should work with most UK smart meters). </P>

    <P>If you have questions or suggestions, you can email me on guy.j.lipman &#64; gmail.com (removing the spaces). 
    I do have more features planned, but I will be guided by suggestions from users. I hope you find it useful.</P>
 

    <H4>Loading data for the first time</H4>
    <P>To get started, you want to load some data. 
    <UL>
    <LI> If you are an Octopus Energy customer, click 
    <A HREF="https://energy.guylipman.com/sm/admin?source=octopus" target="_blank">here</A> to enter 
    your security key. </LI>
    <LI>If you have an n3rgy data account (or want to get one - it is free), click 
    <A HREF="https://energy.guylipman.com/sm/admin?source=n3rgy" target="_blank">here</A>.</LI>
    <LI>If you don't have an Octopus or n3rgy account and just want to see the website using electricity consumption data for 
    a made-up user, that is fine. If the url doesn't have any octopus or n3rgy key in it, it will be using the made-up user data.</LI>
    </UL> 
    <P>At this stage the website won't store anything, but will check with Octopus or n3rgy that your key is valid, 
    and let you know what data we can load. If you want then, you can click a button to load electricity consumption data, 
    gas consumption data (if you have it) or electricity export data (if you have it). </P>
    <P>If you click on the Load button, it will load and store the data going back as far as 1 January 2019. You can delete it at any point,
    or I will delete it automatically within 24 hours of you loading it. I don't store your security key, but do create a unique session_id 
    which allows you to access your data for the next hour.</P>

    <H4>Electricity Consumption Data</H4>
    <P>The first page to look at is the consumption charts. These show monthly consumption since 2019, daily consumption for a given month, and half hourly consumption
    for a given day. You can navigate between the different views.</P>
    <P>A useful tip for many of the views: if you want to limit the data that is picked up, you can add a start and end date in the url. For example, 
    &start=2020/01/01&end=2020/03/31 will just show the data for Q1 2020.</P>
    
    <H4>Electricity Cost Data</H4>
    <P>Similarly to electricity consumption, you can see monthly cost since 2019, daily cost for a given month, and half hourly cost for a given day.</P>
    <P>In order to calculate the cost, we need to know your tariff. You can set a fixed tariff (&tariff=15) - this is interpreted as the tariff in p/kwh, 
    including VAT, and excluding any standing charges. Alternatively, you can use a time-varying tariff like Octopus's AGILE-18-02-21 or GO-18-06-12 
    (at the moment these are the only two time-varying tariffs I've made available, but I will be adding more later). In order to use the 
    time-varying tariffs, you will also need to include your region code, eg C for London. So for me, I set &tariff=AGILE-18-02-21&amp;region=C. If you aren't sure
    what your region code is, you can find it on the Admin page, or if you leave it off you'll be presented with a list. </P>

    <H4>Carbon Intensity</H4>
    <P>This page allows you to view your average carbon intensity and compare it with what it would have been if you consumed
    with a profile/shape that matched the average UK domestic customer. You can drill down to daily or half-hourly level.</P>
    
    <H4>Electricity Bill Calculator</H4>
    <P>The bill calculator lets you calculate what your bill should have been between two dates. As with the Cost Data, you will need to 
    include either a fixed or time-varying tariff. If fixed, include a price (including VAT). If time-varying, you will need to include
    the region. If you have already set these in the url, it should pick them up automatically. You will also need to 
    include the daily standing charge (including VAT). </P>
    <P>It should be noted that the bill calculated won't be exactly the same as what your supplier calculates, because of rounding of consumption and costs. 
    However, it should get you within a percent or two. If you're out by more please get in touch and I can look into it.</P>

    <H4>Save to CSV</H4>
    <P>This page allows you to generate a csv with your half-hourly consumption, prices and costs, and daily consumption and costs.</P>

    <H4>Gas Data</H4>
    <P>Most of the features for gas consumption data work similarly to electricity consumption data, so make sure you read the sections above.</P>
    <P>For gas, the tariff it uses for calculating cost should be set with &gastariff=3.5 (including VAT, excluding standing charges). The reason 
    This allows you to switch back and forth between electricity and gas without modifying the url parameters. </P>
    <P>You can also use the Octopus Gas Tracker tariff, ie gastariff=SILVER-2017-1, in which case you also need to include your region, 
    eg region=C (your region will by the same for electricity and gas).</P>
    <P>If your meter is SMETS2, the gas consumption data we receive is in cubic metres, not kwh. Unfortunately I can't 
    reliably tell from Octopus or n3rgy whether you are SMETS1 or 2, so I can't adjust it automatically. As a result, if you are SMETS2
    you will need to add a parameter &gasmult=11.19 or something like that. This conversion should be specified on your 
    latest bill, and is made up of a volume correction factor (around 1.02264) multiplied by a calorific value (around 40) divided by
    a kwh conversion factor (3.6). </P>

    <H4>Electricity Export Data</H4>
    <P>Most of the features for electricity export data work similarly to electricity consumption data, so make sure you read the sections above.</P>
    <P>For electricity export, the tariff it uses for calculating is specified with &exporttariff=5.5 (in p/kwh, export tariffs don't have VAT). 
    This allows you to switch back and forth between consumption and export without modifying the url parameters.</P>
    <P>You can also use the Octopus Agile Outgoing tariff, ie exporttariff=AGILE-OUTGOING-19-05-13, in which case you also need to include your region, 
    eg region=C (your region will by the same for consumption and export).</P>

    <H4>Frequently Asked Questions</H4>

    <P><B>Why are there lots of parameters in the URL?</B> You will notice that the url will often contain a number of additional parameters, for example mode, tariffs and 
    your octopus/n3rgy key. I do this to make it easy to see and change assumptions, for you to be able to bookmark particular 
    screens, and also for me to be able to replicate any issues you face.</P> 


    <P><B>What is the mode parameter in the URL?</B> The url should include a mode parameter, which will be a 3 digit code, eg 100. This keeps track of what data we have stored, and therefore
    what options to show you. 100 means we just have your electricity consumption data. 110 means we have electricity and gas consumption data. 101 means we just have 
    electricity consumption and export data. And 111 means we have electricity consumption and export data as well as gas data. If you remove this flag it 
    will reset it automatically. You can modify it manually, but it may lead to you seeing irrelevant options, or missing valid ones.</P>



    """

    output = create_sm_page(request, s, 'Getting Started')
    return output    

