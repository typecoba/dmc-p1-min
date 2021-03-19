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
    uvicorn.run('main:app', host='127.0.0.1', port=8000, workers=6, reload=True) # reload=True 시 single process로 돌아감