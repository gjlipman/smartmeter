from django.http import HttpResponse, HttpRequest
from django.shortcuts import render
import os
import pandas as pd
import datetime



def index(request):
    try:
        if 'case' in request.path_info:
            stat = 'cases'
            url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
        else:
            stat = 'deaths'
            url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv'

        minpopulation = int(request.GET.get('minpopulation', '0'))

        df = pd.read_csv(url)
        df.drop(columns=['Lat','Long'], inplace=True)
        df = df.groupby(['Country/Region']).sum().reset_index()

        refreshpops = False
        if refreshpops:
            w = 'https://en.wikipedia.org/wiki/List_of_countries_and_dependencies_by_population'
            pops= pd.read_html(w)
            pops = pops[0]
            pops = pops[pops.columns[1:3]]
            pops.columns = ['Country','Population']
            pops.Country = [x.split('[')[0] for x in pops.Country.values]

            #a = set(df['Country/Region'].tolist())
            #b = set(pops.Country.tolist())
            rename = {'Myanmar': 'Burma', 'Cape Verde': 'Cabo Verde',
            'Congo': 'Congo (Brazzaville)','DR Congo': 'Congo (Kinshasa)',
            'Ivory Coast': "Cote d'Ivoire",'Czech Republic': 'Czechia',
            'Vatican City': 'Holy See','South Korea': 'Korea, South',
            'Taiwan': 'Taiwan*','East Timor': 'Timor-Leste',
            'United States': 'US'}

            names = {k:k for k in pops.Country.tolist()}
            names.update(rename)
            pops['Country'] = pops.Country.map(names)
            pops = pops[pops.Country.isin(df['Country/Region'].unique())]
            pops = pops.sort_values('Country')
            popdict = pd.Series(pops.Population.values, index=pops.Country).to_dict()
        else:
            popdict = {'Afghanistan': 32890171, 'Albania': 2845955, 'Algeria': 43000000, 'Andorra': 77543, 'Angola': 31127674, 'Antigua and Barbuda': 97895, 'Argentina': 45376763, 'Armenia': 2956900, 'Australia': 25769223, 'Austria': 8910696, 'Azerbaijan': 10067108, 'Bahamas': 389410, 'Bahrain': 1592000, 'Bangladesh': 168940146, 'Barbados': 287025, 'Belarus': 9397800, 'Belgium': 11528375, 'Belize': 408487, 'Benin': 12114193, 'Bhutan': 748931, 'Bolivia': 11633371, 'Bosnia and Herzegovina': 3281000, 'Botswana': 2374698, 'Brazil': 211782426, 'Brunei': 459500, 'Bulgaria': 6951482, 'Burkina Faso': 21510181, 'Burma': 54817919, 'Burundi': 11215578, 'Cabo Verde': 556857, 'Cambodia': 15288489, 'Cameroon': 26545864, 'Canada': 38097233, 'Central African Republic': 5496011, 'Chad': 16244513, 'Chile': 19458310, 'China': 1403496680, 'Colombia': 50372424, 'Comoros': 897219, 'Congo (Brazzaville)': 5518092, 'Congo (Kinshasa)': 89561404, 'Costa Rica': 5111238, "Cote d'Ivoire": 26453542, 'Croatia': 4076246, 'Cuba': 11193470, 'Cyprus': 875900, 'Czechia': 10694364, 'Denmark': 5824857, 'Djibouti': 1108567, 'Dominica': 71808, 'Dominican Republic': 10448499, 'Ecuador': 17525000, 'Egypt': 100608449, 'El Salvador': 6486201, 'Equatorial Guinea': 1454789, 'Eritrea': 3546000, 'Estonia': 1328976, 'Eswatini': 1093238, 'Ethiopia': 98665000, 'Fiji': 889327, 'Finland': 5498027, 'France': 67081000, 'Gabon': 2226000, 'Gambia': 2417000, 'Georgia': 3716858, 'Germany': 83166711, 'Ghana': 30280811, 'Greece': 10724599, 'Grenada': 112003, 'Guatemala': 16858333, 'Guinea': 12559623, 'Guinea-Bissau': 1624945, 'Guyana': 787000, 'Haiti': 11743017, 'Holy See': 825, 'Honduras': 9304380, 'Hungary': 9769526, 'Iceland': 366130, 'India': 1364603167, 'Indonesia': 269603400, 'Iran': 83606612, 'Iraq': 40150200, 'Ireland': 4921500, 'Israel': 9221710, 'Italy': 60238522, 'Jamaica': 2726667, 'Japan': 125930000, 'Jordan': 10722520, 'Kazakhstan': 18736688, 'Kenya': 47564296, 'Korea, South': 51780579, 'Kosovo': 1782115, 'Kuwait': 4420110, 'Kyrgyzstan': 6533500, 'Laos': 7231210, 'Latvia': 1902000, 'Lebanon': 6825442, 'Lesotho': 2007201, 'Liberia': 4568298, 'Libya': 6871287, 'Liechtenstein': 38749, 'Lithuania': 2794207, 'Luxembourg': 626108, 'Madagascar': 26251309, 'Malawi': 19129952, 'Malaysia': 32838760, 'Maldives': 374775, 'Mali': 20250833, 'Malta': 493559, 'Mauritania': 4173077, 'Mauritius': 1265475, 'Mexico': 127792286, 'Moldova': 2640400, 'Monaco': 38100, 'Mongolia': 3327144, 'Montenegro': 621873, 'Morocco': 35954097, 'Mozambique': 30066648, 'Namibia': 2504498, 'Nepal': 29996478, 'Netherlands': 17486381, 'New Zealand': 5009191, 'Nicaragua': 6460411, 'Niger': 23196002, 'Nigeria': 206139587, 'North Macedonia': 2077132, 'Norway': 5372355, 'Oman': 4645249, 'Pakistan': 220892331, 'Panama': 4278500, 'Papua New Guinea': 8935000, 'Paraguay': 7252672, 'Peru': 32824358, 'Philippines': 108881966, 'Poland': 38379000, 'Portugal': 10295909, 'Qatar': 2795484, 'Romania': 19405156, 'Russia': 146748590, 'Rwanda': 12374397, 'Saint Kitts and Nevis': 52823, 'Saint Lucia': 178696, 'Saint Vincent and the Grenadines': 110608, 'San Marino': 33553, 'Saudi Arabia': 34218169, 'Senegal': 16705608, 'Serbia': 6926705, 'Seychelles': 98055, 'Sierra Leone': 8100318, 'Singapore': 5703600, 'Slovakia': 5457873, 'Slovenia': 2095861, 'Somalia': 15893219, 'South Africa': 59622350, 'South Sudan': 13249924, 'Spain': 47329981, 'Sri Lanka': 21803000, 'Sudan': 42659275, 'Suriname': 587000, 'Sweden': 10348730, 'Switzerland': 8619259, 'Syria': 17500657, 'Taiwan*': 23586562, 'Tajikistan': 9127000, 'Tanzania': 57637628, 'Thailand': 66527742, 'Timor-Leste': 1425134, 'Togo': 7706000, 'Trinidad and Tobago': 1363985, 'Tunisia': 11722038, 'Turkey': 83154997, 'US': 329940508, 'Uganda': 41590300, 'Ukraine': 41806221, 'United Arab Emirates': 9890400, 'United Kingdom': 66796807, 'Uruguay': 3530912, 'Uzbekistan': 34291508, 'Venezuela': 28435943, 'Vietnam': 96208984, 'Western Sahara': 597000, 'Yemen': 29825968, 'Zambia': 17885422, 'Zimbabwe': 15473818}
    
        df['Population'] = df['Country/Region'].map(popdict)
        df = df[df.Population.notna()]
        df['Population'] /= 1000000
        df2 = pd.DataFrame()
        df2['Country'] = df['Country/Region']
        date = df.columns[-2]
        date2 = df.columns[-9]
        date3 = df.columns[-16]
        datestr = datetime.datetime.strptime(date,'%m/%d/%y').strftime('%b %d')
        #date2str = datetime.datetime.strptime(date2,'%m/%d/%y').strftime('%b %d')
        df2['T'] = df[date]
        df2['D1'] = (df[date]-df[date2])/7
        df2['D2'] = (df[date2]-df[date3])/7
        df2['Tm'] = df2['T']/df['Population']
        df2['D1m'] = df2['D1']/df['Population']
        df2['D2m'] = df2['D2']/df['Population']
        df2['P'] = df.Population
        df2 = df2[df2.P>=minpopulation]
        df2.sort_values('D1', ascending=False, inplace=True)
        

        df2 = df2.round(1)
        s = df2.to_html(index=False)
        s = s.replace('<table border="1" class="dataframe">', '<table class="sortable-theme-dark" data-sortable>')
        colstr = '<th>{}</th>\n      <th>{}</th>\n      <th>{}</th>\n      <th>{}</th>\n      <th>{}</th>\n      <th>{}</th>\n      <th>{}</th>\n    </tr>'
        colstr = colstr.format('Total', 'Latest Week<BR>daily', 'Prior Week <BR>daily', 'Total <BR>per million <BR>population', 'Latest Week<BR>daily<BR>per million', 'Prior Week<BR>daily<BR>per million', 'Population')
        s = s.replace('<th>T</th>\n      <th>D1</th>\n      <th>D2</th>\n      <th>Tm</th>\n      <th>D1m</th>\n      <th>D2m</th>\n      <th>P</th>\n    </tr>', colstr)
        s2 = '<TABLE class="sortable-theme-dark" data-sortable><TR><TH data-sortable="false"><BR><BR>Rank</TH></TR>'
        for i in range(df2.shape[0]):
            s2 += '<TR><TD>{}</TD></TR>'.format(i+1)
        s2 += '</TABLE>' 
        html = """
        <!DOCTYPE html>
        <html>
        <head>
        <script src="/static/sortable-0.8.0/js/sortable.min.js"></script>
        <link rel="stylesheet" href="/static/sortable-0.8.0/css/sortable-theme-bootstrap.css" />
        
        </head>
        <body>
        <H1>Covid {} by country as at {}</H1>
        <P>Deaths and case stats are taken from https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_time_series and the population figures from
        https://en.wikipedia.org/wiki/List_of_countries_and_dependencies_by_population. </P>
        <P>(Click on any column heading to sort by that column.)</P>
        <TABLE><TR><TD>{}</TD><TD>{}</TD></TR></TABLE>
        </body></html>"""
        html = html.format(stat, datestr, s2, s )
        return HttpResponse(html)
    except Exception as err:    
        return HttpResponse(str(err))


    






