This document details the file structure of this Smart Meter Data Viewer Project
```
manage.py
django_project/
    settings.py
    view.py (just contains the view for the base homepage)
    urls.py
    wsgi.py
    statics/ sortable-0.8.0/

covidstats/ (separate project)
    views.py

forecasts/ (separate project)
    views.py    l
    template.html 

sm/  (main smart meter project)
    smviews.py
    smprod.py
    smcharts.py
    smtest.py
    templates/
        chart_template.html
        chart_emis.html
        chart_emis2.html
        analysis_template.html

myutils/  (these are modules that can be used by any scripts or modules)
    keys.py (module with passwords, not in github)
    utils.py
    smutils.py (utils just used by the smart meter project)
    base_template.html

scripts/
    smloads.py
    smdelete.py
    pricefit.py
    priceforecast.py
    postgrestest.py 
    tests.py

docs/
    database.md
    filestructure.md
```

    



