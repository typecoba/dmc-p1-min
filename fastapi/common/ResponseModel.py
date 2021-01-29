from fastapi import status
from pydantic import BaseModel
from http import HTTPStatus as ststus
from starlette.responses import JSONResponse
import datetime, json

class ResponseModel():    
    message: str
    content: any
    
    # 일단 심플하게 구성
    def __init__(self, message=None, content=None):
        self.message = message        
        self.content = content        

    def get(self):
        # none값 반환을 위해 json serialized
        return json.dumps({'message':self.message, 'content':self.content}).encode('utf-8')