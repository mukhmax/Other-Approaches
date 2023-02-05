from pydantic import BaseModel, ValidationError
from errors import HttpError


class CreateAd(BaseModel):
    title: str
    text: str


def validate_create_ad(json_data):
    try:
        ad_schema = CreateAd(**json_data)
        return ad_schema.dict()
    except ValidationError as er:
        raise HttpError(status_code=400, message=er.errors())
