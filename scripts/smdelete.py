from myutils.utils import cronlog, loadDataFromDb, email_script


cronlog()
import pandas as pd
import numpy as np
import requests
from io import StringIO
import datetime
import traceback

errstr = ''
try:
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
    loadDataFromDb(s, returndf=True)
    #print(loadDataFromDb(s, returndf=True))

    s = "delete from sm_quantity where account_id not in (select account_id from sm_accounts where active='1') "
    loadDataFromDb(s, returndf=True)
    #print(loadDataFromDb(s, returndf=True))

except Exception as err:  
    errstr += str(err) 
    errstr += traceback.format_exc()

email_script(errstr, 'smdelete', 0)
if len(errstr):
    print(errstr)

