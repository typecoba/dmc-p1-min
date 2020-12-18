from fastapi import FastAPI
from http import HTTPStatus as status
import router
from starlette.exceptions import HTTPException as StarletteHTTPException
from common.ResponseModel import ResponseModel
import logging

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
    return ResponseModel(exc.status_code, exc.detail).getJson()
