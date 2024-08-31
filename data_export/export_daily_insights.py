import os

import pandas as pd
import datetime

from loader.farm_data_loader import COW_MASTER_FILE, MEASURES_MASTER_FILE

#Parameters to be included in YAML or Key Vault for productive software
BASEPATH_EXPORT = "C:/Jorge/NNDAP/bulk_export/"
EXPORT = "cow_insights.csv"
BASE_STORAGE = "C:/Jorge/NNDAP/farming_storage/"
COW_MASTER_FILE = "cows.csv"
MEASURES_MASTER_FILE = "measurements.csv"


########################################
# Code for daily exports (reports)
# 1 --> Cow kpis (daily milk production
# 2 --> Current weight and 30d avg weight
# 3 --> Likelyhood for sikness of cows (indicator)

def generate_daily_insights_data(dt_ini, dt_end):
    """
    Generates a file with daily insights data, based on initial / end date. In this version, data is not
    partitioned by day, but bulky generated for a given period. Previous file is deleted. For production uses,
    should be recommended to partition generated data (eg: farm / day), both in filesystem or metastore
    :param dt_ini: initial date
    :param dt_end: end date (inclusive)
    :return: None, data is written on file system
    """
    #File deletion (storage)
    try:
        os.remove(BASEPATH_EXPORT + EXPORT)
    except OSError:
        pass

    #Read stored data and join for insight extraction
    l_cows_columns = ['farm_id', 'cow_id', 'name', 'birthdate']
    df_cows = pd.read_csv(BASE_STORAGE + COW_MASTER_FILE, header=None, names=l_cows_columns)
    #print(df_cows.head(10))
    l_measures_columns = ['farm_id', 'timestamp', 'cow_id', 'sensor_id', 'value', 'unit']
    df_measures = pd.read_csv(BASE_STORAGE + MEASURES_MASTER_FILE, header=None, names=l_measures_columns)
    #print(df_measures.head(10))
    #Get date for date filtering (YYYY-MM-DD)
    df_measures['date'] = df_measures['timestamp'].apply(lambda x: datetime.datetime.fromtimestamp(x).strftime('%Y-%m-%d'))
    #Join of two DF to get whole info
    df_base_details = df_cows.merge(df_measures, how='inner', on=['farm_id', 'cow_id'])
    #print(df_base_details.head(10))
    dt_ini_dt = datetime.datetime.strptime(dt_ini, '%Y-%m-%d')   #datetime format, instead of str
    dt_end_dt = datetime.datetime.strptime(dt_end, '%Y-%m-%d')
    print("We are about to calculate KPIs for the period " + dt_ini + " to " + dt_end)

    #loop for generating data (grouped by day and cow id, all indicators)
    aux_date = dt_ini_dt
    l_datasets = []   #list for insights for each day, for union at the end
    while aux_date <= dt_end_dt:
        l_datasets.append(get_day_insighs_cow(aux_date, df_base_details))   #append result for day
        print("KPIs for " + aux_date.strftime('%Y-%m-%d') + " have been calculated")
        aux_date = aux_date + datetime.timedelta(days=1)

    df_result = pd.concat(l_datasets)
    #write result to disk
    print("Flat file with KPIs is going to be writen")
    df_result.to_csv(BASEPATH_EXPORT + EXPORT, index=False, header=True)
    print("Data file wrote")



def get_day_insighs_cow(date, df_base_details) -> pd.DataFrame:
    """
    Calculates insighs for all cows for a given day
    :param date: Date for analysis
    :param df_base_details: Dataframe with base data for calculations
    :return: Result dataframe
    """
    aux_date_str = date.strftime('%Y-%m-%d')   #for comparison
    #Milk production by cow / day (I'll get max value for any sensor)

    aux_df_milk = df_base_details[(df_base_details['date'] == aux_date_str) & (df_base_details['unit'] == 'L')]   #rows for day and milk production
    df_cow_milk_day = aux_df_milk.groupby(['cow_id', 'date'])['value'].max().reset_index()
    df_cow_milk_day['milk_prod'] = df_cow_milk_day['value']
    df_cow_milk_day = df_cow_milk_day.drop(['value'], axis=1)
    #Average weight last 30d weight
    dt_30d = date - datetime.timedelta(days=30)
    aux_30d_str = dt_30d.strftime('%Y-%m-%d')   #for comparison
    aux_30d_df_base = df_base_details[(df_base_details['date'] >= aux_30d_str) & (df_base_details['date'] <= aux_date_str) & (df_base_details['unit'] == 'kg')] #weight in 30d
    df_avg_30d_cow = aux_30d_df_base.groupby(['cow_id'])['value'].mean().reset_index()
    df_avg_30d_cow['date'] = aux_date_str  #to fix date insight
    df_avg_30d_cow['weight_avg_30d'] = df_avg_30d_cow['value']
    df_avg_30d_cow = df_avg_30d_cow.drop(['value'], axis=1)
    #Weight by cow / day (max value as well)
    aux_df_weight = df_base_details[(df_base_details['date'] == aux_date_str) & (df_base_details['unit'] == 'kg')]   #rows for day and weight
    df_cow_weight_day = aux_df_weight.groupby(['cow_id', 'date'])['value'].max().reset_index()
    df_cow_weight_day['day_weight'] = df_cow_weight_day['value']
    df_cow_weight_day = df_cow_weight_day.drop(['value'], axis=1)

    #we now join all datasets to conform result
    df_result = df_cow_milk_day.merge(df_cow_weight_day, how='inner', on=['cow_id', 'date'])
    df_result = df_result.merge(df_avg_30d_cow, how='inner', on=['cow_id', 'date'])
    # We calculate likelyhood of sikness by comparing last weight with previous 30d (1% variation just to try)
    df_result['likely_sick'] = df_result.apply(lambda x: sikness_likelyhood(x), axis=1)

    return df_result


def sikness_likelyhood(row) -> str:
    """
    Calculates sikness likelyhood for a given cow in a day, as if weight is 1% smaller than average 30d (example)
    :param row: dataset single row, for row apply
    :return: yes / no or ND if error
    """
    try:
        if (float(row['weight_avg_30d']) - float(row['day_weight']) ) / float(row['weight_avg_30d']) > 0.01:
            return 'yes'
        else:
            return 'no'
    except Exception:
        return 'ND'