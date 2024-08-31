# Exercise explanation
In this repository you will find my proposed solution for the exercise of cow farm automation. Below I explain how to execute all scripts associated to code, as well as main structure of the proposed solution and assumptions done to complete it.

## Main modules / scripts
This software has 3 main parts:
- **REST API**
- **Bulk data load**
- **KPI / insights export**

First part exposes, through FastAPI and Uvicorn, a REST API with all demanded services, both to populate data from the 3 given data files (parquet) and to get detailed information for an specific cow (in this last one, I made the assumption of giving last measure for each sensor associated to a cow, but it's doubtfull for me to understand the specification).
Second one, is a script to load the three data files provided, inserting every single data (cows, sensors and measures) through the API, thus using data validation mechanisms implemented (ed. null, negative values, format, etc.). Data is persisted, through this API, in a local storage, as I have no datalake / database to store information. In a productive environment alternate, scalable environment should be used. Later in this page I'll explain main points that I would change for productive environment.
The third one is an export program, to generate a flat file with desired KPIs for a given farm (in this scenario there is only one). Resulting file has all insights in a day / cow basis, for further BI analityc uses a golden dataset should be considered in the future.

 ## Scripts execution and associated modules
 As mentioned, 3 main scripts are provided:
### REST API
First one in a number of endpoints, that run locally in this version, with demanded endpoints and data validation implementation. Suposing `127.0.0.1` as local IP, this is [Swagger](http://127.0.0.1:8000/docs) for them: `http://127.0.0.1:8000/docs`
This script runs using uvicorn as server, and is launched from [main.py](https://github.com/JorgeRochaBlanco/JorgeRocha_NNDAP/blob/master/main.py) file in this repository, in the repository root directory.
Associated modules are FastAPI definition and _uvicorn_ server in `main.py`, api method implementation in `api` module and schema and data validation with _Pydantic_ in `schemas` module.
### Bulk data load
In order to populate repository with preexisting data, a bulk loader has been developed, through the script [bulk_load.py](https://github.com/JorgeRochaBlanco/JorgeRocha_NNDAP/blob/master/bulk_load.py). It takes, row wise, all data for the three input files and populates data to the storage through API endpoints, as explained before. No parameters are needed, but paths and file names included in code (Key Vaults or config / YAML sources should be used for productive software instead).
Associated module for this functionality is `loader`.
### KPI / insights export
Demanded KPIs are generated from data storage, and exported to another file path, in a daily basis (insights are grouped by **day + cow**, as demanded). These data can be rebuilt based on data (cows, sensors, mearues), for a given day or period (loop). The script that runs this is [bulk_export.py](https://github.com/JorgeRochaBlanco/JorgeRocha_NNDAP/blob/master/bulk_export.py), and receives as parameter init and end dates for its processing, in `yyyy-mm-dd` format.
Associated module for this funcionality is `data_export`.

## Data for the exercise

In order to facilitate exercise analysis, input data in **parquet** format has been included in the [data_files](https://github.com/JorgeRochaBlanco/JorgeRocha_NNDAP/tree/master/data_files) folder. It's only for having all together, there is no direct reference from code to it, it must be copied to another folder and referenced on config variables.

## Considerations and scalability of the solution
Many assumptions have been made in order to accomplish implementation. I'll try to expose main ones, with the option I chose and an alternative for alternative productive software.

- **Storage for incoming (raw) data**

As I mentioned, no DB or datalake is available, so I used local (laptop) storage for all purposes. For incoming data, particularly en case of large or very large data scenario, should be based in a Database, SQL (eg SQL Server) or indexed (eg Elasticsearch). This would allow for online or near real time data acquisition in a scalable manner, through the API, particularly for sensor data reception. Data rotation policies should be also configured.

- **Analitic platform**

Again, local file system is used, instead of a Datalake platform. This last option, in case of large farming implantation, allows increased capabilities. Medallion data architecture should be implemented for analityc purposes.

- **API and realtime data acquisition scalability**

Local PC running Uvicorn has been used, having a low capacity in terms of concurrent data population. For scalable traffic handling a queuing system (eg Kafka or EventHub) and API Gateway/s should be considered, specially for use cases like large number of sensors submitting real time data. DB (SQL or indexed) would address concurrency if properly set up. Daily basis load to Datalake would allow in-depth (and long term) data storage and usage at all levels.

- **Environment configuration**

In this case, due to limitations, direct hard-coded variables set any parameter for the solution (eg. paths). YAML, Key Vaults or similar tools should be used to handle environment specific set up.

- **Unit tests**

Basic endpoint unit tests have been implemented (all). Disk writing is prevented through mocks.
- **Logger**

For productive software, logger should substitute `print()` sentences.