import pandas as pd
import numpy as np
import datetime
import requests
import json



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

def loadfromdbremote(sqlstr, returndf=False):
    url = 'https://energy.guylipman.com/smsql/'
    password = ''
    if sqlstr[-1]!=';':
        sqlstr += ';'
    r = requests.post(url, data={'password': password, 'sqlquery': sqlstr})
    try:
        output = json.loads(r.text)
    except Exception:
        raise Exception(r.text)
    if 'datetime.datetime' in r.text:
        for i, row in enumerate(output['vals']):
            for j, cell in enumerate(row):
                if isinstance(cell, str) and cell[:17]=='datetime.datetime':
                    output['vals'][i][j] = datetime.datetime.strptime(cell[18:-1], '%Y, %m, %d, %H, %M, %S, %f' )

    if returndf:
        return pd.DataFrame(output['vals'], columns=output['cols'])
    else:
        return output['vals']


def queryreadonly(sqlstr):
    import psycopg2
    from myutils.keys import READONLY_DB
    import json
    creds = READONLY_DB

    conn_string = "host="+ creds['HOST'] +" port="+ "5432" +" dbname="+ creds['NAME'] +" user=" + creds['USER'] \
    +" password="+ creds['PASSWORD']
    conn=psycopg2.connect(conn_string)
    cur = conn.cursor()
    if sqlstr[-1]!=';':
        sqlstr += ';'
    try:
        cur.execute(sqlstr)
        conn.commit()
    except Exception as e:
        return e
    output = cur.fetchall()
    cols = [c.name for c in cur.description]
    j = json.dumps({'cols': cols, 'vals': output}, default=repr)
    return j



def load_bmrs_data(**kwargs):
    from myutils.keys import bmrskey
    kwargs['APIKEY'] = bmrskey
    url = ('https://api.bmreports.com/BMRS/{report}/' +
           'V1?APIKey={APIKEY}&{dates}ServiceType=csv')
    r = requests.get(url.format(**kwargs))
    return r.text


