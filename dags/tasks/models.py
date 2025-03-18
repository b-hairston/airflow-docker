from datetime import datetime
from typing import Dict, List, Union

import pandas as pd
from pydantic import BaseModel, ValidationError, field_validator


class WeatherData(BaseModel):
    observed_at: Union[str, datetime]
    temperature: float
    apparent_temperature: float
    wind_speed: float
    visibility: float


    @field_validator('observed_at', mode="before")
    def validate_observed_at(cls, v):
        if isinstance(v, str):
            try:
                return datetime.fromisoformat(v) 
            except ValueError:
                raise ValueError(f"Invalid date format: {v}")
        return v


def validate_data(df: pd.DataFrame, model: BaseModel) -> Dict[int, List[str]]:
    """
    Validate DataFrame rows against the Pydantic model.

    Args:
        df (pd.DataFrame): The DataFrame to validate.
        model (BaseModel): The Pydantic model to validate against.

    Returns:
        Dict[int, List[str]]: A dictionary with row indices as keys and lists of validation errors as values.
    """
    
    validated_data = []
    
    for index, row in df.iterrows():
        try:
            model(**row.to_dict())
        except ValidationError as e:
            print(f"Validation error for row {index}: {e}")
        else:
            validated_data.append(row)
    
    return validated_data