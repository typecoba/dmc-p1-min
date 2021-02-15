from fastapi import APIRouter, HTTPException
from common.ResponseModel import ResponseModel
from common.FileService import FileService
from common.ConvertProcess import ConvertProcess
from common.Logger import Logger
from common.FacebookAPI import FacebookAPI
from repository.ConfigRepository import ConfigRepository
from http import HTTPStatus
from starlette.config import Config
from starlette.responses import FileResponse
import time
import os
import asyncio
import json

router = APIRouter()
configRepository = ConfigRepository()
fileService = FileService()
facebookAPI = FacebookAPI()

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
    # exception
    if os.path.isfile(catalogConfig['ep']['path']) == False:        
        raise HTTPException(400, 'feed file not found')
    # file export
    response = FileResponse(catalogConfig['ep']['path'],
                            media_type='application/octet-stream',
                            filename=catalogConfig['ep']['path'].split('/')[-1])
    return response


@router.get('/ep/download/{catalog_id}')
async def getDownload(catalog_id):
    catalogConfig = configRepository.findOne(catalog_id)        
    await fileService.download(catalogConfig['ep']['url'], catalogConfig['ep']['path'], catalogConfig['ep']['backupPath'])

    fileInfo = fileService.getInfo(catalogConfig['ep']['url'])    
    return ResponseModel(message='download complete', content=fileInfo)

# ep 내용확인
# @router.get('/ep/detail/{catalog_id}')
# async def getEpDetail():
#     return {'message': 'get_ep_detail'}

# ep 변환(만) 단위테스트
@router.get('/ep/convert2feed/{catalog_id}')
async def getEpConvert2feed(catalog_id):
    catalogConfig = configRepository.findOne(catalog_id)    
    await ConvertProcess(catalogConfig).execute()
    return ResponseModel(message='convert complete')

# 피드 정보 확인
@router.get('/feed/info/{catalog_id}')
async def getFeedInfo():
    catalogConfig = configRepository.findOne(catalog_id)
    result = fileService.getInfo(catalogConfig['feed']['path'])
    return ResponseModel(content=result)


# 피드 내용 확인
# @router.get('/feed/detail/{catalog_id}')
# async def getFeedDetail():
#     return {'message': 'get_feed_detail'}

# 피드 브라우저 다운로드
@router.get('/feed/export/{catalog_id}')
async def getFeedExport(catalog_id):
    catalogConfig = configRepository.findOne(catalog_id)
    # exception
    if os.path.isfile(catalogConfig['feed']['path']) == False:        
        raise HTTPException(400, 'feed file not found')
    # file export
    response = FileResponse(catalogConfig['feed']['path'],
                            media_type='application/octet-stream',
                            filename=catalogConfig['feed']['path'].split('/')[-1])
    return response

@router.get('/feed/upload/{catalog_id}')
async def getFeedUpload(catalog_id):
    catalogConfig = configRepository.findOne(catalog_id)    
    response = await facebookAPI.upload(catalogConfig['info']['feed_id'], catalogConfig['feed']['path'])
    return ResponseModel(content=json.loads(response))
    


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
    await asyncio.sleep(3)
    print('end await')
    return ResponseModel(message='test end')

