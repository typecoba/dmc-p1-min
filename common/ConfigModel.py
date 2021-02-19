from pydantic import BaseModel

class ConfigModel(BaseModel):
    info : str
    ep : str
    feed : str
    columns : str
    custom : str

