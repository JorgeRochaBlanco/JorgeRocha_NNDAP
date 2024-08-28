import csv
import pandas as pd
import datetime

import schemas.farm_schemas as farm_schemas


########################################
# Parameters, to be specified in KeyVault or YAML file in productive environment
BASE_PATH = "C:/Jorge/NNDAP/farming_storage/"
COW_MASTER_FILE = "cows.csv"
SENSORS_MASTER_FILE = "sensors.csv"
MEASURES_MASTER_FILE = "measurements.csv"


def write_cow(cow: farm_schemas.Cow):
    """
    This function writes a new line in csv file with farm cow's master file.
    This is a simplified handled, as best option for data repository (sensors, cows and measures)
    would be a Database, SQL (eg. SQL Server) or index (eg Elasticsearch).
    To improve exercise performance, no checking for repeated data is implemented, should rely in DB handling.
    :param cow: Request with new cow's information
    :return: None
    """
    fields = [cow.farm_id, cow.cow_id, cow.name, cow.birthdate]
    with open(BASE_PATH+COW_MASTER_FILE, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(fields)


def write_sensor(sensor: farm_schemas.Sensor):
    """
    This function writes a new line in csv file with farm sensor's master file.
    Analog to write_cow()
    :param sensor: Request with new sensor's information
    :return: None
    """
    fields = [sensor.sensor_id, sensor.unit]
    with open(BASE_PATH+SENSORS_MASTER_FILE, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(fields)


def write_measure(measure: farm_schemas.Measure):
    """
    Function to write a measure of litres of milk produced or cow weight.
    Analog to write_cow()
    :param measure: Request with new measure's information
    :return: None
    """
    fields = [measure.farm_id, measure.timestamp, measure.cow_id, measure.sensor_id, measure.value, measure.unit]
    with open(BASE_PATH+MEASURES_MASTER_FILE, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(fields)


def get_cow_last_measures(id_cow: str): # -> list[dict]:
    """
    Gets details for a given cow, with last measure of each sensor
    :param id_cow: GUID
    :return: List of dictionaries
    """
    l_cow_columns = ['farm_id', 'id', 'name', 'birthdate']
    df_cows = pd.read_csv(BASE_PATH+COW_MASTER_FILE, names=l_cow_columns)
    print(df_cows.columns)
    df_cows = df_cows[df_cows['id'] == id_cow]   #filter cow data
    l_measure_columns = ['farm_id', 'timestamp', 'cow_id', 'sensor_id', 'value', 'unit']
    df_measures = pd.read_csv(BASE_PATH + MEASURES_MASTER_FILE, names=l_measure_columns)
    df_measures = df_measures[df_measures['cow_id'] == id_cow]
    df_cow_detail = df_cows.merge(df_measures, how='inner', left_on=['id', 'farm_id'], right_on=['cow_id', 'farm_id'])
    df_cow_detail = df_cow_detail[['farm_id', 'timestamp', 'cow_id', 'name', 'birthdate', 'sensor_id', 'value', 'unit']]
    print(df_cow_detail.columns)
    #We order descending (timestamp), then group by rest of fields (farm, cow, sensor, unit) and get first row (latest)
    df_cow_detail = df_cow_detail.sort_values(by=['farm_id', 'cow_id', 'sensor_id', 'unit', 'timestamp'],
                                              ascending=[False, False, False, False, False])
    df_cow_detail_last = df_cow_detail.groupby(['farm_id', 'cow_id', 'name', 'birthdate', 'sensor_id', 'unit']).agg({'timestamp': 'first', 'value': 'first'})
    df_cow_detail_last.columns = ['timestamp', 'value']
    df_cow_detail_last = df_cow_detail_last.reset_index()
    df_cow_detail_last = df_cow_detail_last.sort_values('timestamp', ascending=False)  #order, from last to first
    #Convert to legible datetime format
    df_cow_detail_last['timestamp'] = df_cow_detail_last['timestamp'].apply(lambda x: datetime.datetime.fromtimestamp(x).strftime('%c'))
    i = 1
    l_measures = []
    for index, row in df_cow_detail_last.iterrows():
        dict_fields = {}
        print(i, end='\t')
        i+=1
        for column in df_cow_detail_last.columns:
            print(column + ': ' + str(row[column]), end='\t')
            dict_fields[column] = row[column]
        l_measures.append(dict_fields)
        print()
    return l_measures