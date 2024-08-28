import loader.farm_data_loader as dl
import pandas as pd

#Parameters to be included in YAML or Key Vault for productive software
BASEPATH = "C:/Jorge/NNDAP/data/"

#Open parquet files into dataframes
df_cows = pd.read_parquet(BASEPATH + "cows.parquet", engine="pyarrow")
df_cows = df_cows[["id", "name", "birthdate"]]
df_measures = pd.read_parquet(BASEPATH + "measurements.parquet", engine="pyarrow")
df_measures["timestamp"] = df_measures["timestamp"].astype('float')
df_measures["value"] = df_measures["value"].astype('float')
df_measures = df_measures.dropna(axis=0, how='any')   #drop any row with NaN values (not valid for a sensor)
df_sensors = pd.read_parquet(BASEPATH + "sensors.parquet", engine="pyarrow")
#We add unit to measures, for each sensor, to pass it to API
df_measures_unit = df_measures.merge(df_sensors[['id', 'unit']], how="inner", left_on="sensor_id", right_on="id")
print(df_measures_unit.columns)


#Script for loading all data from parquet files. No date filtering is done, as indicated on exercise
### Cows
ok_cows, ko_cows = dl.bulk_load_cows(df_cows)
print("OK insertions (cows): " + str(ok_cows))
print("KO insertions (cows): " + str(ko_cows))
## Sensors
ok_sensors, ko_sensors = dl.bulk_load_sensors(df_sensors)
print("OK insertions (sensors): " + str(ok_sensors))
print("KO insertions (sensors): " + str(ko_sensors))
## Measures, last chunk of 10000 rows (laptop just don't have resources for the whole bunch)
ok_measures, ko_measures = dl.bulk_load_measures(df_measures_unit.iloc[-15000:])
print("OK insertions (measures): " + str(ok_measures))
print("KO insertions (measures): " + str(ko_measures))
