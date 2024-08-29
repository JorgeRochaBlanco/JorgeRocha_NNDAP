from pydantic import BaseModel, field_validator, model_validator
import datetime


class Cow(BaseModel):
    """
    Structure (pydantic) for request body, to handle cow's fields'
    """
    farm_id: str        #string, no format checked. To be fixed value in this exercise
    cow_id: str         #GUID
    name: str           #Regular string
    birthdate: str      #YYYY-MM-DD for cow birthday

    @field_validator("cow_id")
    def validate_cow_id(cls, v):
        if len(str(v)) <= 30:    #check if it's a well formated GUID
            raise ValueError("Cow ID " + v + " must be a valid GUID")
        return v

    @field_validator("birthdate")
    def validate_date(cls, v):
        try:
            datetime.date.fromisoformat(v)
        except ValueError:
            raise ValueError("Incorrect data format for " + v + ", should be YYYY-MM-DD")
        return v


class Sensor(BaseModel):
    """
    Structure (pydantic) for request body, to handle sensor's fields'
    """
    farm_id: str
    sensor_id: str
    unit: str

    @field_validator("sensor_id")
    def validate_sensor_id(cls, v):
        if len(str(v)) <= 30:
            raise ValueError("Sensor ID " + v + " must be a valid GUID")
        return v
    @field_validator("unit")
    def validate_unit(cls, v):
        if v not in ("L", "kg"):
            raise ValueError("Unit " + v + " must be either 'L' or 'kg'")
        return v


class Measure(BaseModel):
    """
    Structure (pydantic) for request body, to handle measure's fields'
    """
    farm_id: str
    sensor_id: str
    cow_id: str
    timestamp: float
    value: float
    unit: str

    @field_validator("timestamp")
    def validate_timestamp(cls, v):
        try:
            float(v)
        except ValueError:
            raise ValueError("Incorrect EPOCH format for " + v + ", should be numeric")
        return v
    @field_validator("value")
    def validate_value(cls, v):
        try:
            float(v)
        except ValueError:
            raise ValueError("Incorrect value format for " + v + ", should be numeric")
        return v
    @field_validator("cow_id")
    def validate_cow_id(cls, v):
        if len(str(v)) <= 30:
            raise ValueError("Cow ID " + v + " must be a valid GUID")
        return v
    @field_validator("sensor_id")
    def validate_sensor_id(cls, v):
        if len(str(v)) <= 30:
            raise ValueError("Sensor ID " + v + " must be a valid GUID")
        return v
    @model_validator(mode='after')   #combined validation, unit and measure
    def validate_unit_value(self):
        if self.unit not in ["L", "kg"]:
            raise ValueError("Unit " + self.unit + " must be either 'L' or 'kg'")
        else:
            if self.unit == "L":
                if not(0 < self.value < 50):  #supposed 50 is way more than acceptable for milk
                    raise ValueError("Milk production (" + str(self.value) + ") must be between 0 and 50")
            else:
                if not(50 < self.value < 1200):  #supposed a cow shouldn't weight more than 1200kg
                    raise ValueError("Cow weight (" + str(self.value) + ") must be between 50 and 1200")
        return self


class CowDetail(BaseModel):
    """
    Structure to give cow details and latest measure by sensor.
    """
    farm_id: str
    cow_id: str
    cow_name: str
    cow_birthdate: str
    sensor_id: str
    timestamp: str
    value: float
    unit: str