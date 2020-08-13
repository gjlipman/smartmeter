# smartmeter
A database / website for viewing smart meter data

This project is primarily a django website on top of a postgresql database, designed to allow
UK energy smart meter owners to load and analyse their data. This website can be accessed at 
https://energy.guylipman.com/sm/home.

For more information about the project visit the docs folder, or the blogpost at https://medium.com/@guylipman/a-website-for-viewing-your-smart-meter-data-4d4c84b2bc33.

This project also includes two subprojects:

The forecasts folder produces forecasts of hourly electricity prices for the next 7 days. You can see these endpoints on https://energy.guylipman.com/forecasts?region=C for region C retail prices for Octopus Agile, or https://energy.guylipman.com/forecasts for wholesale prices.

The covidstats folder displays the latest deaths and case statistics by country. You can see these endpoings on https://energy.guylipman.com/covidstats/deaths or https://energy.guylipman.com/covidstats/cases.



