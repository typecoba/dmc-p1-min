from fastapi import Request
import time, json, requests, datetime
from starlette.responses import JSONResponse

"""
message, content받아 상태값 추가하여 일괄반환
json->byte->string->dict
"""
class ResponseMiddleware():    
    async def __call__(self, request: Request, call_next):
        starttime = time.time()

        # start
        # json string byte로 받아 dict로 변환
        mResponse = await call_next(request)
        content = b""
        async for chunk in mResponse.body_iterator:
            content += chunk
        responseModel = json.loads(content.decode('utf-8'))
        
        # end
        duration = time.time() - starttime
        
        jsonResponse = JSONResponse({
            "statusCode": mResponse.status_code,
            "statusName": requests.status_codes._codes[mResponse.status_code][0],
            "message": responseModel['message'],
            "responseTime": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),  # 반환시간
            "processTime": format(duration, '0.3f'),  # 처리시간(초)
            "content": responseModel['content']
        }, status_code=mResponse.status_code)

        return jsonResponse
