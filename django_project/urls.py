"""django_project URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from django.conf.urls import url
from . import view 
from forecasts import views as forecastviews
from covidstats import views as covidstatsviews
from sm import smviews

urlpatterns = [
    path('', view.index, name='index'),
    url(r'forecasts/*', forecastviews.index, name='forecasts'),
    url(r'covidstats/*', covidstatsviews.index, name='covidstats'),
    path('sm/<slug:choice>', smviews.index, name='sm')
]
