from fastapi import APIRouter, HTTPException, Request
from common.ResponseModel import ResponseModel
from common.FileService import FileService
from common.ConvertProcess import ConvertProcess
from common.Logger import Logger
from common.FacebookAPI import FacebookAPI
from common.Utils import Utils
from common.Properties import Properties
from repository.ConfigRepository import ConfigRepository
from repository.ProductRepository import ProductRepository
from http import HTTPStatus
from starlette.config import Config
from starlette.responses import FileResponse
import time
from datetime import datetime
import os
import asyncio
import aiohttp
import json
import pycron
import aiofiles
from dateutil import parser
from multiprocessing import Process, Queue


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
    result['ep'] = fileService.get_info(config['ep']['url'])
    if 'ep_update' in config :
        result['ep_update'] = fileService.get_info(config['ep_update']['url'])

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
    config = configRepository.findOne(catalog_id)
    return fileService.download_ep(url=str(config['ep']['url']), path=str(config['ep']['fullPath']))
    

# ep_update 경로 추가
@router.get('/ep/download/{catalog_id}/update')
async def getDownloadUpdate(catalog_id): 
    config = configRepository.findOne(catalog_id)
    return fileService.download_ep(url=config['ep_update']['url'], path=config['ep_update']['fullPath'])



# ep 내용확인
# @router.get('/ep/detail/{catalog_id}')
# async def getEpDetail():
#     return {'message': 'get_ep_detail'}


# ep 변환(만) 단위테스트
@router.get('/ep/convert2feed/{catalog_id}')
async def getEpConvert2feed(catalog_id):
    try:
        config = ConfigRepository().findOne(catalog_id)
    
        # exception
        if config['info']['status'] == properties.STATUS_CONVERTING :
            raise HTTPException(status_code=400, detail=f'convert already started at {config["info"]["moddate"]}...')
            
        configRepository.updateOne({f'catalog.{catalog_id}' : {'$exists':True}}, {'$set':{'info.status':properties.STATUS_CONVERTING, 'info.moddate':Utils.nowtime()}})
        ConvertProcess(config).execute(catalog_id=catalog_id)
        configRepository.updateOne({f'catalog.{catalog_id}' : {'$exists':True}}, {'$set':{'info.status':'', 'info.moddate':Utils.nowtime()}})
        return ResponseModel(message='convert complete')

    except Exception as e :
        configRepository.updateOne({f'catalog.{catalog_id}' : {'$exists':True}}, {'$set':{'info.status':'', 'info.moddate':Utils.nowtime()}})
        raise HTTPException(status_code=100, detail=str(e))
        

# ep_update 변환 단위테스트
@router.get('/ep/convert2feed/{catalog_id}/update')
async def getEpConvert2feed(catalog_id):
    config = configRepository.findOne(catalog_id)

    # exception
    if 'ep_update' not in config :
        raise HTTPException(400, 'ep_update not found in config')

    if config['info']['status'] == properties.STATUS_CONVERTING : # status값을 상수로 만들어야겠다..
        
        raise HTTPException(400, 'convert process not finished or force stopping')
    
    try :
        configRepository.updateOne({f'catalog.{catalog_id}' : {'$exists':True}}, {'$set':{'info.status':properties.STATUS_CONVERTING}})
        ConvertProcess(config).execute(catalog_id=catalog_id, is_update=True)
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
    result = fileService.get_info(filePath)
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


# feed segment 분할만.. 삭제예정
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
            facebookAPI.upload(feed_id=feed_id, feed_url=feed['publicPath'], isUpdateEp=False)

    return ResponseModel(message='Facebook API upload complete')

@router.get('/feed/upload/{catalog_id}/update')
async def getFeedUploadUpdate(catalog_id):
    config = configRepository.findOne(catalog_id)
    if 'ep_update' not in config:
        raise HTTPException(400, 'ep_update not in config')

    # feed별 업데이트
    for feed_id, feed in config['catalog'][catalog_id]['feed'].items():
        if config['info']['media'] == 'facebook':            
            facebookAPI.upload(feed_id=feed_id, feed_url=feed['publicPath'], isUpdateEp=True)
    
    return ResponseModel(message='Facebook API upload (update only) complete')



###
# 10분마다 호출하여 cron 체크 후 실행
# 대용량 처리시 메모리 점유율 문제(모니터링-개선)
# 
# config단위 : 광고주단위 시간대별 동시처리 필요
# ㄴ convertProcess.execute단위 : mem = chucksize * len(catalog_id)
#   ㄴ feed write단위 : mem = feed_df.info(memory_usage) * len(feed_id) 
@router.get('/schedule/convertProcess')
async def getScheduleConvertProcess() :
    configs = configRepository.findAll()
    is_upload = True if properties.SERVER_PREFIX == 'prod' else False # 운영서버일경우에만 api upload
    
    # config 단위 멀티스레드
    processList = []
    for config in configs :
        if ('ep' in config) and (config['ep']['cron'] != '') and pycron.is_now(config['ep']['cron']):
            is_update = False
            processList.append( Process(target=convertProcessExecute, args=(config, is_update, is_upload))) 
            processList[-1].start()
        
        if ('ep_update' in config) and (config['ep_update']['cron'] != '') and pycron.is_now(config['ep_update']['cron']):             
            is_update = True
            processList.append( Process(target=convertProcessExecute, args=(config, is_update, is_upload)))
            processList[-1].start()
    
    for process in processList :
        process.join()

    return ResponseModel(message=f'convertProcess count ({len(processList)})')        


def convertProcessExecute(config, is_update, is_upload) :    
    convertProcess = ConvertProcess(config)
    for key, value in config['catalog'].items() :
        convertProcess.execute(catalog_id=key, is_update=is_update, is_upload=is_upload)






### 
# 10분마다 호출하여 cron 체크 후 실행
# 1. 스케쥴 실행 (비동기 + 멀티프로세스)

# @router.get('/schedule/convertProcess')
# async def getScheduleConvertProcess():
#     configs = configRepository.findAll()    
#     port = properties.getServerPort()
#     url = f'http://localhost:{port}/schedule/convertProcess_execute'
#     headers = {'Content-Type': 'application/json'}
#     scheduledConfigs = []
    
#     # ep/ep_update 중 schedule에 해당하는것만 체크    
#     for config in configs :
#         if ('ep' in config) and (config['ep']['cron'] != '') and pycron.is_now(config['ep']['cron']):
#             scheduledConfigs.append(config)
#         if ('ep_update' in config) and (config['ep_update']['cron'] != '') and pycron.is_now(config['ep_update']['cron']):             
#             scheduledConfigs.append(config)
    
#     # 코루틴 사용 for
#     # aiohttp 사용 내부 router 호출
#     futures = [asyncio.ensure_future( aiohttp_post(url, json=json.dumps(config), headers=headers) ) for config in scheduledConfigs]    

#     return ResponseModel(message=f'convertProcess in scheduled ({len(scheduledConfigs)})')

### 
# 2. 스케쥴에서 내부 router 호출해서 multiprocess로 실행
# config 데이터 post로 받아 ep/ep_update 크론 확인하여 convert process 실행

# @router.post('/schedule/convertProcess_execute')
# async def postScheduleConvertProcess_execute(request: Request):
#     # json -> dict
#     config = json.loads(await request.json()) # fastapi request 클래스 사용
#     isUpload = True if properties.SERVER_PREFIX == 'prod' else False # 운영서버일경우에만 api upload
#     isUpdateEp = False if (config['ep']['cron']!='') and pycron.is_now(config['ep']['cron']) else True # ep와 ep_update는 동시에 스케줄링 되지 않는다는 전제
    
#     # convert process
#     convertProcess = ConvertProcess(config)            
#     for catalog_id, catalogDict in config['catalog'].items() :
#         await convertProcess.execute(catalog_id=catalog_id, isUpdateEp=isUpdateEp, isUpload=isUpload)



# async def aiohttp_post(url, data=None, json=None, headers=None) :
#     timeout = aiohttp.ClientTimeout(total=60*60*24)
#     async with aiohttp.ClientSession(timeout=timeout) as session : # timeout 0
#         async with session.post(url=url, data=data, json=json, headers=headers) as response:
#             return await response.text()