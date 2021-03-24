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
async def exception_handler(request, exc):
    return JSONResponse({"message": exc.detail, "content": None}, status_code=exc.status_code)

# response middleware
app.middleware('http')(ResponseMiddleware())

# run server
if __name__ == '__main__':
    # local환경 reload=True
    # reload=True 시 single process로 설정됨    
    prop = Properties()
    print(prop.SERVER_AUTO_RELOAD)
    uvicorn.run('main:app', host='127.0.0.1', port=8000, workers=6, reload=prop.SERVER_AUTO_RELOAD)