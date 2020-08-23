from django.http import HttpResponse, HttpRequest
from django.shortcuts import render, redirect
import os
import sys
import pandas as pd
sys.path.append("/home/django/django_project/")
from sm.smviews import inner
from covidstats.views import index


class TestRequest:
    def __init__(self, url, method='GET', META=None, postdata=None):
        self.method = method
        self.url = url
        u = url.split('?')
        self.path_info = u[0]
        self.choice = u[0].split('/')[-1]
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
        return self.url

def checkurl(url, **kwargs):
    request = TestRequest(url, **kwargs)
    try:
        response = inner(request, request.choice)
        print(f'ok: {url}')
        return response
    except Exception as e:
        print(f'error: {url}: {e}')
        raise

def checkurl2(url, **kwargs):
    request = TestRequest(url, **kwargs)
    try:
        response = index(request)
        print(f'ok: {url}')
        return response
    except Exception as e:
        print(f'error: {url}: {e}')
        raise


if __name__ == "__main__":
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")    
    #checkurl('/sm/hom')
    #checkurl('/sm/admin')

    import time
    s = time.time()

    if False:
        checkurl('/sm/cost?mode=100&debug&tariff=11.3')
        print(time.time()-s); s=time.time()

        s = time.time()
        checkurl('/sm/cost?mode=100&debug&tariff=AGILE-18-02-21&region=C')
        print(time.time()-s); s=time.time()

        s = time.time()
        checkurl('/sm/cost?mode=100&debug&tariff=AGILE-18-02-21&region=C&new')
        print(time.time()-s); s=time.time()

        s = time.time()
        checkurl('/sm/cost?mode=100&debug&tariff=20&region=C')
        print(time.time()-s); s=time.time()

        s = time.time()
        checkurl('/sm/cost?mode=100&debug&tariff=20&region=C&new')
        print(time.time()-s); s=time.time()

        s = time.time()
        checkurl('/sm/cost?mode=100&debug&tariff=20&region=C&new&month=2020-06')
        print(time.time()-s); s=time.time()
    
        checkurl2('/forecasts?region=C')

    checkurl('/sm/netconsumption?debug')