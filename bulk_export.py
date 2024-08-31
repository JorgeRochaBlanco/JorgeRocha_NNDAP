from datetime import datetime

import data_export.export_daily_insights as di

import sys


#Get parameters from command line (date_ini, date_end in YYYY-MM-DD format).
if 2 < len(sys.argv) > 3:
    raise Exception("Right format is 'python bulk_export dt_init [dt_end]', both in yyyy-mm-dd format")
dt_ini = sys.argv[1]
if len(sys.argv) > 2:
    dt_end = sys.argv[2]
else:
    dt_end = dt_ini
#Check format for dt_ini and dt_end
try:
    aux_dt_ini = datetime.strptime(dt_ini, '%Y-%m-%d')
    aux_dt_end = datetime.strptime(dt_end, '%Y-%m-%d')
except ValueError:
    raise Exception("Right format is 'python bulk_export dt_ini [dt_end]', both in yyyy-mm-dd format")

di.generate_daily_insights_data(dt_ini, dt_end)