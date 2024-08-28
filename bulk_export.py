import data_export.export_daily_insights as di

import sys


#Parameters to be included in YAML or Key Vault for productive software
BASEPATH = "C:/Jorge/NNDAP/bulk_export/"


#Get parameters from command line (date_ini, date_end in YYYY-MM-DD format). No format check done yet, TODO
dt_ini = sys.argv[1]
if len(sys.argv) > 2:
    dt_end = sys.argv[2]
else:
    dt_end = dt_ini

di.generate_daily_insights_data(dt_ini, dt_end)