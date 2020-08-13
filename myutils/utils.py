import pandas as pd
import numpy as np
import datetime
import requests
from myutils.keys import metkey


def cronlog():
    import inspect
    import datetime
    frame_records = inspect.stack()[1][1]
    print(datetime.datetime.now().isoformat() + ': ' + frame_records )


def getConnection():
    import psycopg2
    from myutils.keys import DJANGO_DB
    creds = DJANGO_DB

    conn_string = "host="+ creds['HOST'] +" port="+ "5432" +" dbname="+ creds['NAME'] +" user=" + creds['USER'] \
    +" password="+ creds['PASSWORD']
    conn=psycopg2.connect(conn_string)

    cur = conn.cursor()
    return conn, cur

def loadDataFromDb(sqlstr, returndf=False):
    if sqlstr[-1]!=';':
        sqlstr += ';'
    conn, cur = getConnection()
    cur.execute(sqlstr)
    conn.commit()
    try:
        output = cur.fetchall()
        if returndf:
            output = pd.DataFrame(output, columns=[c.name for c in cur.description])
    except Exception:
        output = cur.statusmessage
    conn.close()
    return output


def load_bmrs_data(**kwargs):
    from myutils.keys import bmrskey
    kwargs['APIKEY'] = bmrskey
    url = ('https://api.bmreports.com/BMRS/{report}/' +
           'V1?APIKey={APIKEY}&{dates}ServiceType=csv')
    r = requests.get(url.format(**kwargs))
    return r.text


