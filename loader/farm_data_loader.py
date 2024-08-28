import pandas as pd
import datetime
import os
import requests
import json


########################################
# Parameters, to be specified in KeyVault or YAML file in productive environment
BASE_PATH = "C:/Jorge/NNDAP/farming_storage/"
COW_MASTER_FILE = "cows.csv"
SENSORS_MASTER_FILE = "sensors.csv"
MEASURES_MASTER_FILE = "measurements.csv"
BASE_URL=f'http://127.0.0.1:8000/'
COWS_URI='cows/'
SENSORS_URI='sensors/'
MEASURES_URI='measures/'


def bulk_load_cows(df_cows):
    """
    Function to store in file storage all cows data. First step is to delete file if exists.
    In real environment, this storage should be Database (SQL or index, like Elastic).
    :param df_cows: Pandas DataFrame with cow data
    :return: Tuple with two keys, number of OK records, and number of KO records
    """
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'}
    #File deletion (storage)
    try:
        os.remove(BASE_PATH + COW_MASTER_FILE)
    except OSError:
        pass

    #For each DF row, call API to write it
    ok_inserts = 0
    ko_inserts = 0
    for index, row in df_cows.iterrows():
        #Farm id is hardcoded always

        payload_cows = {"farm_id": "Farm1", "cow_id": row['id'], "name": row['name'], "birthdate": str(row['birthdate'])}
        response_cows = requests.post(BASE_URL + COWS_URI + row['id'], headers=headers, data=json.dumps(payload_cows))
        data_cows = response_cows.json()
        if response_cows.status_code == 200:  #operation ok
            ok_inserts += 1
        else:
            ko_inserts += 1
            print(data_cows['detail'][0]['msg'])
    return ok_inserts, ko_inserts


def bulk_load_sensors(df_sensors):
    """
    Function to store in file storage all sensors data. First step is to delete file. Analog to cows bulk load
    :param df_sensors: set of sensors
    :return: tuple of ok / ko total rows
    """
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'}
    #File deletion (storage)
    try:
        os.remove(BASE_PATH + SENSORS_MASTER_FILE)
    except OSError:
        pass

    #For each DF row, call API to write it
    ok_inserts = 0
    ko_inserts = 0
    for index, row in df_sensors.iterrows():
        #Farm id is hardcoded always

        payload_sensors = {"farm_id": "Farm1", "sensor_id": row['id'], "unit": row['unit']}
        response_sensors = requests.post(BASE_URL + SENSORS_URI + row['id'], headers=headers, data=json.dumps(payload_sensors))
        if response_sensors.status_code == 200:  #operation ok
            ok_inserts += 1
        else:
            data_sensors = response_sensors.json()
            ko_inserts += 1
            print(data_sensors['detail'][0]['msg'])
    return ok_inserts, ko_inserts


def bulk_load_measures(df_measures):
    """
    Function to store in file storage all sensors measures. First step is to delete file. Analog to cows bulk load
    :param df_measures: set of measures
    :return: tuple of ok / ko total rows
    """
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'}
    #File deletion (storage)
    try:
        os.remove(BASE_PATH + MEASURES_MASTER_FILE)
    except OSError:
        pass

    #For each DF row, call API to write it
    ok_inserts = 0
    ko_inserts = 0
    for index, row in df_measures.iterrows():
        #Farm id is hardcoded always

        payload_measures = {"farm_id": "Farm1", "sensor_id": row['sensor_id'], "cow_id": row['cow_id'], "timestamp": row['timestamp'],
                            "value": row['value'], "unit": row['unit']}
        response_measures = requests.post(BASE_URL + MEASURES_URI + row['sensor_id'], headers=headers, data=json.dumps(payload_measures))
        data_measures = response_measures.json()
        if response_measures.status_code == 200:    #operation ok
            ok_inserts += 1
        else:
            ko_inserts += 1
            print(data_measures['detail'][0]['msg'])
    return ok_inserts, ko_inserts
