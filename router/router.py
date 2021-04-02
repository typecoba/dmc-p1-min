from fastapi import APIRouter, HTTPException, Request
from common.ResponseModel import ResponseModel
from common.FileService import FileService
from common.ConvertProcess import ConvertProcess
from common.Logger import Logger
from common.FacebookAPI import FacebookAPI
from common.Utils import Utils
from common.Properties import Properties
from repository.ConfigRepository import ConfigRepository
from http import HTTPStatus
from starlette.config import Config
from starlette.responses import FileResponse
import time
from datetime import datetime
import os
import asyncio
import json
import pycron
import aiofiles
from dateutil import parser


router = APIRouter()
configRepository = ConfigRepository()
fileService = FileService()
facebookAPI = FacebookAPI()
utils = Utils()
properties = Properties()

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
async def getConfigs():
    result = configRepository.findAll()
    return ResponseModel(content=result)


@router.post('/config', status_code=HTTPStatus.OK)
async def postConfig(request : Request):
    data = await request.body() # validation은 추후
    data = json.loads(data.decode('ascii'))
    try : 
        result = configRepository.insertOne(data)
        return ResponseModel(content=str(result.inserted_id))
    except Exception as e:        
        raise HTTPException(status_code=400, detail=str(e))
    

@router.get('/config/{catalog_id}', status_code=HTTPStatus.OK)
async def getConfig(catalog_id):
    result = configRepository.findOne(catalog_id)
    return ResponseModel(content=result)


@router.put('/config/{catalog_id}', status_code=HTTPStatus.OK)
async def putConfig(request : Request, catalog_id):     
    oriValue = {f'catalog.{catalog_id}':{'$exists':True}}
    newValue = json.loads((await request.body()).decode('ascii'))
    
    try :
        result = configRepository.updateOne(oriValue, newValue)
        return ResponseModel(content=result.matched_count)

    except Exception as e:    
        raise HTTPException(status_code=400, detail=str(e))


@router.delete('/config/{catalog_id}', status_code=HTTPStatus.OK)
async def delConfig(catalog_id):
    # status값 업데이트 처리
    oriValue = {f'catalog.{catalog_id}': {'$exists':True}}
    newValue = {'$set' : {'info.status':'deleted'}}
    
    try:
        result = configRepository.updateOne(oriValue, newValue)
        return ResponseModel(message='deleted')

    except Exception as e :
        raise HTTPException(status_code=400, detail=str(e))


# ep 정보 확인
@router.get('/ep/info/{catalog_id}')
async def getEpInfo(catalog_id):
    config = configRepository.findOne(catalog_id)
    result = {}
    result['ep'] = fileService.getInfo(config['ep']['url'])
    if 'ep_update' in config :
        result['ep_update'] = fileService.getInfo(config['ep_update']['url'])

    return ResponseModel(content=result)


# ep 브라우저 다운로드
@router.get('/ep/export/{catalog_id}')
async def getEpExport(catalog_id):
    config = configRepository.findOne(catalog_id)
    # exception
    if os.path.isfile(config['ep']['fullPath']) == False:
        raise HTTPException(400, 'feed file not found')
    # file export
    response = FileResponse(config['ep']['fullPath'],
                            media_type='application/octet-stream',
                            filename=config['ep']['fullPath'].split('/')[-1])
    return response


# 
@router.get('/ep/download/{catalog_id}')
async def getDownload(catalog_id):
    return  await fileService.getEpDownload(catalog_id=catalog_id)


# ep_update 경로 추가
@router.get('/ep/download/{catalog_id}/update')
async def getDownloadUpdate(catalog_id):    
    return await fileService.getEpDownload(catalog_id=catalog_id, isUpdate=True)

    


# ep 내용확인
# @router.get('/ep/detail/{catalog_id}')
# async def getEpDetail():
#     return {'message': 'get_ep_detail'}


# ep 변환(만) 단위테스트
@router.get('/ep/convert2feed/{catalog_id}')
async def getEpConvert2feed(catalog_id):
    config = ConfigRepository().findOne(catalog_id)
    
    # exception
    if config['info']['status'] == properties.STATUS_CONVERTING :
        raise HTTPException(status_code=400, detail=f'convert already started at {config["info"]["moddate"]}...')
    
    try:
        configRepository.updateOne({f'catalog.{catalog_id}' : {'$exists':True}}, {'$set':{'info.status':properties.STATUS_CONVERTING, 'info.moddate':Utils.nowtime()}})
        await ConvertProcess(config).execute(catalog_id=catalog_id)
        configRepository.updateOne({f'catalog.{catalog_id}' : {'$exists':True}}, {'$set':{'info.status':'', 'info.moddate':Utils.nowtime()}})
        return ResponseModel(message='convert complete')

    except Exception as e :
        configRepository.updateOne({f'catalog.{catalog_id}' : {'$exists':True}}, {'$set':{'info.status':'', 'info.moddate':Utils.nowtime()}})
        raise HTTPException(status_code=400, detail=str(e))
        

# ep_update 변환 단위테스트
@router.get('/ep/convert2feed/{catalog_id}/update')
async def getEpConvert2feed(catalog_id):
    config = configRepository.findOne(catalog_id)

    # exception
    if 'ep_update' not in config :
        raise HTTPException(400, 'ep_update not found in config')

    if config['info']['status'] == properties.STATUS_CONVERTING : # status값을 상수로 만들어야겠다..
        raise HTTPException(400, 'convert already started at {config["info"]["moddate"]}...')
    
    try :
        configRepository.updateOne({f'catalog.{catalog_id}' : {'$exists':True}}, {'$set':{'info.status':properties.STATUS_CONVERTING}})
        await ConvertProcess(config).execute(catalog_id=catalog_id, isUpdate=True)
        configRepository.updateOne({f'catalog.{catalog_id}' : {'$exists':True}}, {'$set':{'info.status':''}})
        return ResponseModel(message='convert complete')

    except Exception as e :
        configRepository.updateOne({f'catalog.{catalog_id}' : {'$exists':True}}, {'$set':{'info.status':''}})
        raise HTTPException(status_code=400, detail=str(e))
    
        
        


# 피드 정보 확인
@router.get('/feed/info/{catalog_id}/{feed_id}')
async def getFeedInfo(catalog_id, feed_id):
    config = configRepository.findOne(catalog_id)
    filePath = config['catalog'][catalog_id]['feed'][feed_id]['fullPath']
    result = fileService.getInfo(filePath)
    return ResponseModel(content=result)


# 피드 내용 확인
# @router.get('/feed/detail/{catalog_id}')
# async def getFeedDetail():
#     return {'message': 'get_feed_detail'}


# 피드 브라우저 다운로드
@router.get('/feed/export/{catalog_id}/{feed_id}')
async def getFeedExport(catalog_id, feed_id):
    config = configRepository.findOne(catalog_id)
    # exception
    feedFullPath = config['catalog'][catalog_id]['feed'][feed_id]['fullPath']
    if os.path.isfile(feedFullPath) == False:
        raise HTTPException(400, 'feed file not found')
    # file export
    response = FileResponse(feedFullPath,
                            media_type='application/octet-stream',
                            filename=feedFullPath.split('/')[-1])
    return response


# feed segment 분할
@router.get('/feed/segmentation/{catalog_id}')
async def getFeedSegmentation(catalog_id):
    config = configRepository.findOne(catalog_id)
    # exception
    feedFullPath = config['catalog'][catalog_id]['feed_temp']
    if os.path.isfile(feedFullPath) == False:
        raise HTTPException(400, 'feed file not found')

    convertProcess = ConvertProcess(config)
    convertProcess.feedSegmentation(catalog_id=catalog_id)
    return ResponseModel()


# facebook api upload / update
@router.get('/feed/upload/{catalog_id}')
async def getFeedUpload(catalog_id):
    config = configRepository.findOne(catalog_id)
    # feed별 업로드
    for feed_id, feed in config['catalog'][catalog_id]['feed'].items():
        if config['info']['media'] == 'facebook':
            feedPublicPath = feed['publicPath']
            await facebookAPI.upload(feed_id=feed_id, feed_url=f'{feedPublicPath}.zip', isUpdate=False)

    return ResponseModel(message='Facebook API upload complete')

@router.get('/feed/upload/{catalog_id}/update')
async def getFeedUploadUpdate(catalog_id):
    config = configRepository.findOne(catalog_id)
    if 'ep_update' not in config:
        raise HTTPException(400, 'ep_update not in config')

    # feed별 업데이트
    for feed_id, feed in config['catalog'][catalog_id]['feed'].items():
        if config['info']['media'] == 'facebook':
            feedPublicPath = feed['publicPath']
            await facebookAPI.upload(feed_id=feed_id, feed_url=f'{feedPublicPath}.zip', isUpdate=True)
    
    return ResponseModel(message='Facebook API upload (update only) complete')



# scheduled feed convert process
# 10분마다 호출하여 cron 체크 후 실행
@router.get('/schedule/convertProcess')
async def getSchedule():
    configs = configRepository.findAll()
    properties = Properties()
    isUpload = True if properties.SERVER_PREFIX == 'prod' else False # 운영서버일경우에만 api upload
    
    for config in configs: # config 전체
        # ep cron
        if pycron.is_now(config['ep']['cron']) : # cron check            
            convertProcess = ConvertProcess(config)            
            for catalog_id, catalogDict in config['catalog'].items() : # catalog 전체
                print(catalogDict['name'], catalog_id)
                await convertProcess.execute(catalog_id=catalog_id, isUpdate=False, isUpload=isUpload) # catalog_id 기준으로 실행


        # ep_update cron
        if ('ep_update' in config) and pycron.is_now(config['ep']['cron']) :            
            convertProcess = ConvertProcess(config)            
            for catalog_id, catalogDict in config['catalog'].items() :
                print(catalogDict['name'], catalog_id)
                await convertProcess.execute(catalog_id=catalog_id, isUpdate=True, isUpload=isUpload)
                        
    return ResponseModel(content='scheduled')



@router.get('/test/apiupload')
async def test_apiupload():
    feed_id = '236164118048821'
    # feed_url = 'http://api.dmcf1.com/feed/268046537186348/feed_268046537186348_2499714026735797.tsv.zip'
    feed_url = 'http://api.dmcf1.com/feed/141118536454632/watermark_141118536454632.json.gz'
    isUpdate = False
    await facebookAPI.upload(feed_id, feed_url, isUpdate)
    return ResponseModel()

@router.get('/test/download')
async def test_download():
    fromPath = 'http://api.dmcf1.com/ep/ssg/ssg_facebookNoCkwhereEpAll.csv'
    toPath = 'C:/Users/shsun/Documents/workspace/project/p1/f1_feed_change_min/data/ep/ep_ssg_facebook.csv'
    print(toPath)
    await fileService.download(fromPath, toPath)

@router.get('/test/ping')
async def test_ping():
    logger = Logger()
    ip = Utils.getIP()
    logger.info(f'ping ok - {ip}')    
    return ResponseModel(message='ping ok', content=ip)

