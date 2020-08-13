This document details the database tables used by the Smart Meter Data Viewer Website.

sm_periods (contains a row for each half hour)
---
period_id integer (0 is 2019-01-01 00:00)
period char(16)
local_date date
local_time char(5) (eg 23:30)
timezone_adj (1 if BST, 0 if GMT)

sm_accounts (contains a row for each session_id / type_id)
---
account_id integer
type_id smallint  (0 for electricity consumption, 1 for gas, 2 for elec export)
first_period varchar(16)
last_period varchar(16)
last_updated timestamp
region char(1) (A-P, or null)
source_id integer (0 for n3rgy, 1 for octopus)
session_id uuid 
active bool 

sm_quantity
---
id integer
account_id integer  (link to sm_accounts)
period_id integer   (link to sm_periods)
quantity float(8)

sm_variables 
---
var_id integer 
product varchar(32)
region char(1) (Z for variables that don't have a region)
type_id integer
granularity_id integer (0 for hh, 1 for daily)


sm_hh_variable_vals (values for half hourly variables, should be renamed sm_hh_vals)
---
id integer
var_id integer (link to sm_variables)
period_id integer (link to sm_periods)
value float(8)


sm_d_variable_vals (values for daily variables, should be renamed sm_d_vals)
---
id integer
var_id integer (link to sm_variables)
local_date integer (link to sm_periods)
value float(8)


sm_log 
---
id integer
datetime timestamp
url varchar(124)
method integer (0 for GET, 1 for POST)
session_id uuid
choice varchar(64) (the endpoint key, eg consumption or home)







