from django.http import HttpResponse, HttpRequest
import sys

helloWorld = """
<!DOCTYPE html>
<html>
<head>
<title>Energy Sandpit</title>
</head>
<body>
  <div>
    <h1>Guy Lipman's Sandpit</h1>

    <p>This a Python-based webserver in which I am testing out various bits of functionality.</p>
    <UL>
    <LI><A HREF="covidstats/deaths">Covid Statistics - Deaths</A></LI>
    <LI><A HREF="covidstats/cases">Covid Statistics - Cases</A></LI>
    <LI><A HREF="forecasts?region=C">Electricity Price Forecasts</A>
    <LI><A HREF="sm/home">Smart Meter Data Viewer</A>
    </div>
</body>
</html>
"""
def forecasts(request):
    return HttpResponse("hello")


def index(request):
    return HttpResponse(helloWorld.replace("{IPADDRESS}",request.get_host()).replace("{vers}",str(sys.path)))
