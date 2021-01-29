from fastapi import FastAPI, Request
from http import HTTPStatus as status
import router
from starlette.exceptions import HTTPException as StarletteHTTPException
from common.ResponseModel import ResponseModel
import logging
import time, datetime

import requests
import uvicorn
import json
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
    jsonResponse = JSONResponse({        
        "message": exc.detail,
        "content": None,        
    },status_code=exc.status_code)

    return jsonResponse # exception handler에선 get으로 return 명시해주어야함?


'''
message, content받아 상태값 추가하여 일괄반환
json->byte->string->dict
'''
# response middleware
@app.middleware("http")
async def response_middleware(request: Request, call_next) : 
    starttime = time.time()
    
    ##### start
    # json string byte로 받아 dict로 변환
    mResponse = await call_next(request)
    content = b""
    async for chunk in mResponse.body_iterator: 
        content += chunk    
    responseModel = json.loads(content.decode('utf-8'))    
    ##### end
    duration = time.time() - starttime
    
    jsonResponse = JSONResponse({
        "statusCode":mResponse.status_code,
        "statusName":requests.status_codes._codes[mResponse.status_code][0],
        "message": responseModel['message'],
        "content": responseModel['content'],
        "responseTime":datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), # 반환시간
        "processTime":format(duration, '0.3f') # 처리시간(초)
    },status_code=mResponse.status_code)

    return jsonResponse

# run server
if __name__ == '__main__':
    # reload=True 시 single process로 돌아감
    uvicorn.run('main:app', host='127.0.0.1', port=8000, reload=True, workers=4)#