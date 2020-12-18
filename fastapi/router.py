from fastapi import APIRouter, HTTPException
from common.ResponseModel import ResponseModel
from http import HTTPStatus

router = APIRouter()

# facebook기준 ep, feed 명칭으로 통일함

# home
@router.get('/', status_code=HTTPStatus.OK)
async def home():
    return ResponseModel(HTTPStatus.OK.value, HTTPStatus.OK.phrase,"p1")

# error test
@router.get('/error')
async def error():
    raise HTTPException(status_code=400) # exception발생

# 카탈로그 config 확인
@router.get('/config/{catalog_id}')
async def get_catalog_config():
    return {'message': 'get_catalog_config'}

# ep 정보 확인
@router.get('/ep/info/{catalog_id}')
async def get_ep_info():
    return {'message': 'get_ep_info'}

# ep 다운로드
@router.get('/ep/download/{catalog_id}')
async def get_ep_download():
    return {'message': 'get_ep_download'}

# ep 내용확인    
@router.get('/ep/detail/{catalog_id}')
async def get_ep_detail():
    return {'message': 'get_ep_detail'}

# ep 변환
@router.get('/ep/convert/{catalog_id}')
async def get_ep_convert():
    return {'message': 'get_ep_convert'}

# 피드 정보 확인
@router.get('/feed/info/{catalog_id}')
async def get_feed_info():
    return {'message': 'get_feed_info'}

# 피드 내용 확인
@router.get('/feed/detail/{catalog_id}')
async def get_feed_detail():
    return {'message': 'get_feed_detail'}

# 피드 다운로드
@router.get('/feed/download/{catalog_id}')
async def get_feed_download():
    return {'message': 'get_feed_download'}

# 피드 업로드
@router.get('/feed/upload/{catalog_id}')
async def get_feed_upload():
    return {'message': 'get_feed_upload'}