from fastapi.testclient import TestClient

from main import app

import schemas.farm_schemas as farm_schemas
import api.farm_handlers as farm_handlers

#############################################################################################################
#In a regular project, a new folder /tests should be created. This file is just intended to illustrate
#basic unit test with main endpoints of the REST API. Writing to storage is going to be mocked to prevent
#any data loss / modification
#############################################################################################################

client = TestClient(app)


def test_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Echo test OK"}


def test_cow_details_ok():
    response = client.get("/cow_details/9b0bb133-05a4-4b72-9245-7333b1edb4f6")
    assert response.status_code == 200
    assert (
        {
            "farm_id": "Farm1",
            "cow_id": "9b0bb133-05a4-4b72-9245-7333b1edb4f6",
            "cow_name": "Jennifer #7",
            "cow_birthdate": "2021-05-18",
            "sensor_id": "c9aac051-3edf-462a-b421-186f193ad095",
            "timestamp": "Thu Jul 11 12:00:00 2024",
            "value": 4.65,
            "unit": "L"
          }
        in response.json() )


def test_cow_creation_invalid_cow_id_format():
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }
    json_msg =  {
          "farm_id": "Farm1",
          "cow_id": "1111",
          "name": "Cow number 1",
          "birthdate": "2020-01-01"
    }
    response = client.post(
        '/cows/1111',   #invalid GUID format, generates error
        headers=headers,
        json=json_msg
    )
    assert response.status_code == 422   #uprocessable entity, Pydantic error (validation)


def test_cow_creation_invalid_cow_birhtdate_format():
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }
    json_msg =  {
          "farm_id": "Farm1",
          "cow_id": "15f06691-ab7b-4cac-89a9-dc7c6554c9c1",
          "name": "Cow number 1",
          "birthdate": "1234"
    }
    response = client.post(
        '/cows/15f06691-ab7b-4cac-89a9-dc7c6554c9c1',   #invalid YYYY-MM-DD format, generates error
        headers=headers,
        json=json_msg
    )
    assert response.status_code == 422   #uprocessable entity, Pydantic error (validation)


def test_cow_creation_valid_insert(mocker):
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }
    json_msg =  {
          "farm_id": "Farm1",
          "cow_id": "15f06691-ab7b-4cac-89a9-dc7c6554c9c1",
          "name": "Cow number 1",
          "birthdate": "2020-01-01"
    }
    response = client.post(
        '/cows/15f06691-ab7b-4cac-89a9-dc7c6554c9c1',
        headers=headers,
        json=json_msg
    )
    mocker.patch("api.farm_handlers.csv.writer")  #prevent disk writing
    assert response.status_code == 200   #call ok
    assert response.json() == {
        "id": "15f06691-ab7b-4cac-89a9-dc7c6554c9c1",
        "msg": "Cow 15f06691-ab7b-4cac-89a9-dc7c6554c9c1 created successfully!"
    }


def test_sensor_creation_invalid_sensor_id_format():
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }
    json_msg =  {
          "farm_id": "Farm1",
          "sensor_id": "1111",
          "unit": "L"
    }
    response = client.post(
        '/sensors/1111',   #invalid GUID format, generates error
        headers=headers,
        json=json_msg
    )
    assert response.status_code == 422   #uprocessable entity, Pydantic error (validation)


def test_sensor_creation_invalid_sensor_unit_format():
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }
    json_msg =  {
          "farm_id": "Farm1",
          "sensor_id": "0c0bc9b0-6572-4ee8-bfa0-908ba19b5c44",
          "unit": "foo unit"
    }
    response = client.post(
        '/sensors/0c0bc9b0-6572-4ee8-bfa0-908ba19b5c44',   #invalid unit, generates error
        headers=headers,
        json=json_msg
    )
    assert response.status_code == 422   #uprocessable entity, Pydantic error (validation)


def test_sensor_creation_valid_insert(mocker):
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }
    json_msg =  {
          "farm_id": "Farm1",
          "sensor_id": "0c0bc9b0-6572-4ee8-bfa0-908ba19b5c44",
          "unit": "kg"
    }
    response = client.post(
        '/sensors/0c0bc9b0-6572-4ee8-bfa0-908ba19b5c44',
        headers=headers,
        json=json_msg
    )
    mocker.patch("api.farm_handlers.csv.writer")  #prevent disk writing
    assert response.status_code == 200   #call ok
    assert response.json() == {
        "id": "0c0bc9b0-6572-4ee8-bfa0-908ba19b5c44",
        "msg": "Sensor 0c0bc9b0-6572-4ee8-bfa0-908ba19b5c44 created successfully!"
    }


def test_measure_creation_invalid_timestamp():
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }
    json_msg =  {
          "farm_id": "Farm1",
          "sensor_id": "0c0bc9b0-6572-4ee8-bfa0-908ba19b5c44",
          "cow_id": "9b0bb133-05a4-4b72-9245-7333b1edb4f6",
          "timestamp": "foo",
          "value": "4.50",
          "unit": "L"
    }
    response = client.post(
        '/measures/0c0bc9b0-6572-4ee8-bfa0-908ba19b5c44',   #invalid EPOCH, generates error
        headers=headers,
        json=json_msg
    )
    assert response.status_code == 422   #uprocessable entity, Pydantic error (validation)


def test_measure_creation_invalid_value():
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }
    json_msg =  {
          "farm_id": "Farm1",
          "sensor_id": "0c0bc9b0-6572-4ee8-bfa0-908ba19b5c44",
          "cow_id": "9b0bb133-05a4-4b72-9245-7333b1edb4f6",
          "timestamp": "1718992800.0",
          "value": "foo",
          "unit": "L"
    }
    response = client.post(
        '/measures/0c0bc9b0-6572-4ee8-bfa0-908ba19b5c44',   #invalid unit, generates error
        headers=headers,
        json=json_msg
    )
    assert response.status_code == 422   #uprocessable entity, Pydantic error (validation)


def test_measure_creation_invalid_value_unit_combination():
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }
    json_msg =  {
          "farm_id": "Farm1",
          "sensor_id": "0c0bc9b0-6572-4ee8-bfa0-908ba19b5c44",
          "cow_id": "9b0bb133-05a4-4b72-9245-7333b1edb4f6",
          "timestamp": "1718992800.0",
          "value": "500",
          "unit": "L"
    }
    response = client.post(
        '/measures/0c0bc9b0-6572-4ee8-bfa0-908ba19b5c44',   #invalid unit and value combination, error
        headers=headers,
        json=json_msg
    )
    assert response.status_code == 422   #uprocessable entity, Pydantic error (validation)


def test_sensor_creation_valid_insert(mocker):
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }
    json_msg =  {
          "farm_id": "Farm1",
          "sensor_id": "0c0bc9b0-6572-4ee8-bfa0-908ba19b5c44",
          "cow_id": "9b0bb133-05a4-4b72-9245-7333b1edb4f6",
          "timestamp": "1718992800.0",
          "value": "4.50",
          "unit": "L"
    }
    response = client.post(
        '/measures/0c0bc9b0-6572-4ee8-bfa0-908ba19b5c44',
        headers=headers,
        json=json_msg
    )
    mocker.patch("api.farm_handlers.csv.writer")  #prevent disk writing
    assert response.status_code == 200   #call ok
    assert response.json() == {
        "id": "0c0bc9b0-6572-4ee8-bfa0-908ba19b5c44",
        "msg": "Measure for sensor 0c0bc9b0-6572-4ee8-bfa0-908ba19b5c44 with value 4.5 created successfully!"
    }
