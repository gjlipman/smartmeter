import pandas as pd
import numpy as np
import requests
from io import StringIO
import datetime
import traceback
from myutils.utils import cronlog, loadDataFromDb, email_script

cronlog()


errstr = ''


try:
    url2 = 'SILVER-23-12-06/gas-tariffs/G-1R-SILVER-23-12-06-F/standard-unit-rates/'
    url = 'https://api.octopus.energy/v1/products/' + url2
    r = requests.get(url)
    r = r.json().get('results',[])
    last_from_date = r[0]['valid_from']
    if last_from_date < (datetime.datetime.now()).strftime('%Y-%m-%dT%H:%M:%SZ'):
        raise Exception('Last tracker price starts {}'.format(last_from_date))
            
except Exception as err:  
    errstr += 'Problem with Tracker price import \n'
    errstr +=  str(err) 
    errstr += traceback.format_exc() + '\n'


email_script(errstr, 'tracker.py', 1)
if len(errstr):
    print(errstr)