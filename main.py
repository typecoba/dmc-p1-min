from fastapi import FastAPI, Request
from http import HTTPStatus as status
from router import router
from starlette.exceptions import HTTPException as StarletteHTTPException
from middleware.ResponseMiddleware import ResponseMiddleware

import logging
import time
import datetime

import requests
import uvicorn
import json
from starlette.responses import JSONResponse

from common.Logger import Logger
from common.Properties import Properties

'''
관리파일 분리
-router
-middleware
-repository
-util
'''

app = FastAPI()

# 라우터
app.include_router(router.router)

# exception handler
@app.exception_handler(StarletteHTTPException)
async def exception_handler(request: Request, exc: Exception):
    print(exc.detail)
    return JSONResponse({"message": exc.detail, "content": None}, status_code=exc.status_code)

# response middleware
app.middleware('http')(ResponseMiddleware())

# run server
if __name__ == '__main__':        
    prop = Properties()    
    uvicorn.run('main:app', 
                host=prop.SERVER_API_HOST,
                port=prop.getServerPort(), 
                workers=prop.SERVER_API_WORKERS, 
                reload=prop.SERVER_AUTO_RELOAD) # True if local else False