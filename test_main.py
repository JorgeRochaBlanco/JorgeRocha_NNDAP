from fastapi.testclient import TestClient

from .main import app

client = TestClient(app)

def test_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"msg": "Echo test OK"}


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
