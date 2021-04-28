from fastapi import Request
import time, json, requests, datetime
from starlette.responses import JSONResponse
from starlette.types import ASGIApp
import logging, os
from common.Logger import Logger


"""
message, content받아 상태값 추가하여 일괄반환
json->byte->string->dict
"""
class ResponseMiddleware():    
        
    async def __call__(self, request: Request, call_next):
        # root log
        logger = Logger() # root logger
        logger.info(f'**Request {request.url.path}')

        # docs페이지 예외(...)
        if request.url.path == '/docs' or request.url.path == '/openapi.json' :
            return await call_next(request)
        

        # start
        starttime = time.time()
        # json string byte로 받아 dict로 변환
        response = await call_next(request) #response
        content = b""
        async for chunk in response.body_iterator:
            content += chunk
        content = json.loads(content.decode('utf-8')) # ResponseModel 형태
        # end
        duration = format(time.time() - starttime, '0.3f')
                        
        # 공통내용
        responseDict = {
            "statusCode": response.status_code,
            "statusName": requests.status_codes._codes[response.status_code][0],
            "responseTime": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),  # 반환시간
            "processTime": duration,  # 처리시간(초)            
        }

        # 내용추가
        if content != None : 
            responseDict['message'] = content['message']
            responseDict['content'] = content['content']

        logger.info(f'**Response {responseDict}') # response.status_code
        
        return JSONResponse(responseDict, status_code=response.status_code)
