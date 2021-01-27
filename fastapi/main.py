from fastapi import FastAPI, Request
from http import HTTPStatus as status
import router
from starlette.exceptions import HTTPException as StarletteHTTPException
from common.ResponseModel import ResponseModel
import logging
import time
import requests


from starlette.responses import JSONResponse

'''
관리파일 분리
-router
-properties
response model
exception handler
mongoose
'''
app = FastAPI()
app.include_router(router.router)

# exception handler
@app.exception_handler(StarletteHTTPException)
async def exception_handler(request, exc):    
    response = ResponseModel(statusCode=exc.status_code, statusName=requests.status_codes._codes[exc.status_code][0] , message=exc.detail)
    return response.get() # exception handler에선 get으로 return 명시해주어야함?

# benchmark middleware
@app.middleware("http")
async def benchmark_middleware(request: Request, call_next) : 
        starttime = time.time()
        response = await call_next(request)
        duration = time.time() - starttime
        response.headers['Server-Timing'] = format(duration,'0.3f') # second        
        return response