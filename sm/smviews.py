from django.http import HttpResponse, HttpRequest
from django.shortcuts import render, redirect
import os
import sys
import pandas as pd

from myutils.smutils import sm_log
from .smprod import (adminPage, homepage, moreinfo,
                             checkRequest, gettingStartedPage, getmode)
from .smcharts import (consumptionPage, costPage, emissionsPage)
from .smtest import (get_savecsvPage, billsPage, checksPage, logPage, gastrackerpage, tariffcomparison, analysisPage)


def inner(request, choice):
    if choice == 'home':
        output = homepage(request)
    elif choice == 'gastracker':
        output = gastrackerpage(request)     
    elif choice == 'info':
        output = moreinfo(request)
    elif choice == 'getstarting':
        output = gettingStartedPage(request)
    elif choice == 'checks':
        output = checksPage(request)
    elif choice == 'logpage':
        output = logPage(request)
    elif choice =='admin':
        output = adminPage( request)
    elif choice in ['consumption','gasconsumption','export']:
        output = consumptionPage(request, choice)
    elif choice in ['cost','gascost','exportrevenue']:
        output = costPage(request, choice)
    elif choice in ['bills', 'gasbills','exportbills']:
        output = billsPage(request, choice)
    elif choice in ['compare','gascompare','exportcompare']:
        output = tariffcomparison(request, choice)
    elif choice in ['savecsv', 'gassavecsv','exportsavecsv']:
        output = get_savecsvPage(choice, request)
    elif choice in ['emissions']:
        output = emissionsPage(request)
    elif choice in ['analysis']:
        output = analysisPage(request)
    else:      
        raise Exception('Invalid Choice not picked up earlier')
    return output


def index(request, choice):
    try:
        if choice=='smidcheck':
            s = ''
            for k, v in request.COOKIES.items():
                s+= f'{k}: {v}<BR>'
            return HttpResponse(s)

        choice = checkRequest(request)
        if isinstance(choice, HttpResponse):
            return choice
        elif len(choice)>20: #indicates error response
            return HttpResponse(choice)
        sm_log(request, choice)
        output = inner(request, choice)
        if not isinstance(output, HttpResponse):
            output = HttpResponse(output)    
        return output
    except Exception as err:    
        import traceback
        errstr = str(err) + '<BR>'
        errstr += '<BR>'.join([x for x in traceback.format_exc().splitlines()])
        return HttpResponse(errstr)




