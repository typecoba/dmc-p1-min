from fastapi import status
from pydantic import BaseModel
from http import HTTPStatus as ststus
from starlette.responses import JSONResponse
import datetime

class ResponseModel():    
    statusCode: int
    statusName: str
    message: str
    data: dict
    datetime: str
    def __init__(self, statusCode=200, statusName="OK", message=None, data=None):
        self.statusCode = statusCode
        self.statusName = statusName
        self.message = message
        self.data = data
        self.datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def getJson(self):
        return JSONResponse({"statusCode":self.statusCode,
                             "statusName":self.statusName,
                             "message":self.message,
                             "data":self.data,
                             "datetime":self.datetime},
                             status_code=self.statusCode)
