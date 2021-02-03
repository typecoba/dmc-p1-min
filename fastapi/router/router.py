from fastapi import APIRouter, HTTPException
from common.ResponseModel import ResponseModel
from repository.ConfigRepository import ConfigRepository
from common.FileService import FileService
from common.ConvertProcess import ConvertProcess
from http import HTTPStatus
from starlette.config import Config
from starlette.responses import FileResponse
import time
import os
import asyncio

router = APIRouter()
configRepository = ConfigRepository()
fileService = FileService()

# facebook기준 ep, feed 명칭으로 통일함

# home
@router.get('/', status_code=HTTPStatus.OK)
async def home():
    return ResponseModel(message='feedconvert_min_home')

# error test
@router.get('/error')
async def error():
    raise HTTPException(status_code=400, detail='error page')  # exception발생


# 카탈로그 config 확인
@router.get('/config', status_code=HTTPStatus.OK)
async def getCatalogConfigs():
    catalogConfig = configRepository.findAll()
    return ResponseModel(content=catalogConfig)


@router.get('/config/{catalog_id}', status_code=HTTPStatus.OK)
async def getCatalogConfig(catalog_id):
    catalogConfig = configRepository.findOne(catalog_id)
    return ResponseModel(content=catalogConfig)


# ep 정보 확인
@router.get('/ep/info/{catalog_id}')
async def getEpInfo(catalog_id):
    catalogConfig = configRepository.findOne(catalog_id)
    result = fileService.getInfo(catalogConfig['ep']['url'])
    return ResponseModel(content=result)


# ep 브라우저 다운로드
@router.get('/ep/export/{catalog_id}')
async def getEpExport(catalog_id):
    catalogConfig = configRepository.findOne(catalog_id)

    # ep download
    if os.path.isfile(catalogConfig['ep']['fullPath']) == False:  # 파일없는경우 다운로드
        fileService.download(
            catalogConfig['ep']['url'], catalogConfig['ep']['fullPath'])

    # ep export
    response = FileResponse(catalogConfig['ep']['fullPath'],
                            media_type='application/octet-stream',
                            filename=catalogConfig['ep']['fullPath'].split('/')[-1])
    return response


@router.get('/ep/download/{catalog_id}')
async def getDownload(catalog_id):
    catalogConfig = configRepository.findOne(catalog_id)
    await fileService.download(catalogConfig['ep']['url'], catalogConfig['ep']['fullPath'])
    # await fileService.aDownload(catalogConfig['ep']['url'], catalogConfig['ep']['fullPath'])
    return ResponseModel(message='download complete')

# ep 내용확인
# @router.get('/ep/detail/{catalog_id}')
# async def getEpDetail():
#     return {'message': 'get_ep_detail'}

# ep 변환(만)
@router.get('/ep/convert2feed/{catalog_id}')
async def getEpConvert2feed(catalog_id):
    catalogConfig = configRepository.findOne(catalog_id)
    convertProcess = ConvertProcess(catalogConfig)
    convertProcess.execute()
    return ResponseModel(message='convert complete')

# 피드 정보 확인
@router.get('/feed/info/{catalog_id}')
async def getFeedInfo():
    catalogConfig = configRepository.findOne(catalog_id)
    result = fileService.getInfo(catalogConfig['feed']['fullPath'])
    return ResponseModel(content=result)


# 피드 내용 확인
# @router.get('/feed/detail/{catalog_id}')
# async def getFeedDetail():
#     return {'message': 'get_feed_detail'}

# 피드 브라우저 다운로드
@router.get('/feed/export/{catalog_id}')
async def getFeedExport(catalog_id):
    catalogConfig = configRepository.findOne(catalog_id)
    if os.path.isfile(catalogConfig['feed']['fullPath']) == False:
        # return ResponseModel(HTTPStatus.BAD_REQUEST.value, HTTPStatus.BAD_REQUEST.phrase, message='feed file not found')
        raise HTTPException(400, 'feed file not found')

    # file export
    response = FileResponse(catalogConfig['feed']['fullPath'],
                            media_type='application/octet-stream',
                            filename=catalogConfig['feed']['fullPath'].split('/')[-1])
    return response


# scheduled feed convert process
# @router.get('/schedule/convertprocess/{catalog_id}')
# async def getConvertProcess(catalog_id):
#     catalogConfig = configRepository.findOne(catalog_id)
#     convertProcess = ConvertProcess(catalogConfig)
#     convertProcess.execute()
#     return ResponseModel()

# 피드 api 업로드
# @router.get('/feed/upload/{catalog_id}')
# async def getFeedUpload():
#     return {'message': 'get_feed_upload'}


@router.get('/test/async')
async def test_sync():
    print('test async')
    await asyncio.sleep(5)
    print('end await')
    return ResponseModel(message='test end')