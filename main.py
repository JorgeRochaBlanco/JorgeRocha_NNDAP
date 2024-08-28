from fastapi import FastAPI

import api.farm_handlers as farm_handlers
import schemas.farm_schemas as farm_schemas
from schemas.farm_schemas import CowDetail

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Echo test OK"}


@app.post("/cows/{id}")
async def create_cow(id: str, cow: farm_schemas.Cow):
    """
    Creates a new cow in a given farm
    :param id: GUID for new cow
    :param cow: Cow structure (pydantic model) with 4 fields (farm id, cow id, name, birthdate)
    :return: message for the insertion
    """
    farm_handlers.write_cow(cow)

    return {"id": id, "msg": "Cow " + id + " created successfully!"}


@app.post("/sensors/{id}")
async def create_sensor(id: str, sensor: farm_schemas.Sensor):
    """
    Creates a new sensor in a given farm
    :param id:
    :param sensor:
    :return:
    """
    farm_handlers.write_sensor(sensor)

    return {"id": id, "msg": "Sensor " + id + " created successfully!"}


@app.post("/measures/{id}")
async def create_measures(id: str, measures: farm_schemas.Measure):
    """
    Creates a new measure in a given farm and a given sensor / cow
    :param id: sensor id
    :param measures: structure with measure data, including cow for eventual double check
    :return: none
    """
    farm_handlers.write_measure(measures)

    return {"id": id, "msg": "Measure for sensor " + id + " with value " + str(measures.value) + " created successfully!"}



@app.get("/cow_details/{id}")
async def get_cow_details(id: str) -> list[CowDetail]:
    """
    Returns details for a given cow, with cow base info and latest measure for each associated sensor.
    Specification in exercise is not clear. Last measure by sensor? Last by timestamp only? Last by type (L / Kg)?.
    I take last by sensor.
    :param id: cow Id
    :return: rows with latest measure for each sensor of this cow
    """
    l_measures = farm_handlers.get_cow_last_measures(id)
    l_results = []
    for row in l_measures:   #for each row (dictionary) we build a CowDetail and insert in list of results
        l_results.append(CowDetail(farm_id = 'Farm1',
                                   cow_id = row['cow_id'],
                                   cow_name = row['name'],
                                   cow_birthdate = row['birthdate'],
                                   sensor_id = row['sensor_id'],
                                   timestamp = row['timestamp'],
                                   value = row['value'],
                                   unit = row['unit'])
                         )
    return l_results



#For command line launching
# if __name__ == "__main__":
#     import uvicorn
#     # Run the FastAPI application using uvicorn
#     uvicorn.run(app, host="127.0.0.1", port=8000)