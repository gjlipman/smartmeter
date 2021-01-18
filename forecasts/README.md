# Forecasts
This code generates a 7 day forecast.

The webpage itself is generated using the code in views.py. This makes use of a template in template.html.

The webpage loads data from the database. The data is populated using three main scripts:

scripts/pricefit.py runs daily and loads a few weeks of historical prices and historical net demand, and performs a regression, the parameters of which are saved in table price_function

scripts/priceforecast.py runs twice a day and loads inputs to forecast net demand. It then applies the parameters from price_function to forecast the price.

scripts/smloads.py runs twice a day and loads the latest Octopus Agile prices, to allow us to see how the forecast compared to the outturn prices. 