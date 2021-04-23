from urllib import request
import os
from datetime import datetime, timedelta
from dateutil import parser
import shutil

import asyncio
import aiohttp
import aiofiles

from starlette.config import Config
from fastapi import HTTPException
from common.Logger import Logger
from common.Utils import Utils
from common.Properties import Properties
from common.ResponseModel import ResponseModel
from repository.ConfigRepository import ConfigRepository
import zipfile
from pytz import timezone



class FileService():
    def __init__(self):
        pass        

    def setLogger(self, logger=None):
        self.logger = logger    
    
    def getInfo(self, filePath=None):        
        if 'http' in filePath :
            # file path
            with request.urlopen(filePath) as f:
                response = f.info()
                f.close()

            # str-> datetime -> timezone 적용 -> formatting
            mdatetime = parser.parse(response['Last-Modified']).astimezone(timezone('Asia/Seoul')).strftime('%Y-%m-%d %H:%M:%S %z')

            result = {
                'url': filePath,
                'size': int(response['Content-Length']),
                'size_formated' : Utils.sizeof_fmt(int(response['Content-Length'])),
                'last_moddate': mdatetime
            }

        else : # 파일인경우
            # file check
            if os.path.exists(filePath) == False: 
                return None
            size = os.path.getsize(filePath) # 파일 크기
            mtime = os.path.getmtime(filePath)  # 수정시간
            # exists = os.path.exists(filePath) # 파일 존재여부
            # ctime = os.path.getctime(filePath)  # 생성시간
            # atime = os.path.getatime(filePath)  # 마지막 엑세스시간
            
            # timestamp -> datetime -> timezone 적용 -> formatting
            mdatetime = datetime.fromtimestamp(mtime, timezone('Asia/Seoul')).strftime('%Y-%m-%d %H:%M:%S %z')
            result = {
                'path': filePath,
                'size': size,
                'size_formated': Utils.sizeof_fmt(size),
                'last_moddate': mdatetime
            }
        
        return result


    
    # 파일이 없으면 첫부분정도만 확인할수 있나?
    # 파일이 있으면 중간부터 확인할 수 있나?
    # def getEpDetail():


    '''
    [epdownload check & download]
    1. 원본 ep 와 local ep 시간비교
    2. config status 이용하여 download lock
    3. ep update 플래그로 config 연동
    
    *** convert process 내부에서 연동되므로 exception을 함수 외부로 빼야하나?

    '''    
    async def getEpDownload(self, catalog_id=None, isUpdate=False): # type = '' or 'update'
        configRepository = ConfigRepository()        
        config = configRepository.findOne(catalog_id)        

        # ep / ep_update flag
        epKey = 'ep_update' if isUpdate == True else 'ep'

        # ep_update 체크
        if isUpdate == True and 'ep_update' not in config:
            # raise HTTPException(400, 'ep_update not found in config')
            return ResponseModel(message='ep_update not found in config')

        # 원본파일 변동 확인
        if os.path.isfile(config[epKey]['fullPath']) : # 다운받은 파일이 있는 경우
            epOriInfo = self.getInfo(config[epKey]['url'])          # 오리지널 ep info
            epOriSize = epOriInfo['size']                           # 오리지널 ep size
            epOriModDate = parser.parse(epOriInfo['last_moddate'])  # 오리지널 ep 생성시간
            epInfo = self.getInfo(config[epKey]['fullPath'])        # 로컬 ep info
            epSize = epInfo['size']                                 # 로컬 ep size
            epModDate = parser.parse(epInfo['last_moddate'])        # 로컬 ep 생성시간
            # print('epmoddate',epModDate, 'eporimoddate',epOriModDate)
            # print(epModDate > epOriModDate)
            # print('epSize', epSize, 'epOriSize', epOriSize)
            # print(epSize==epOriSize)

            if epModDate >= epOriModDate and epSize == epOriSize :                
                return ResponseModel(message='file not changed', content={ 'server': epOriInfo, 'local': epInfo })

        # 서버단위 중복 다운로드 방지
        if config[epKey]['status'] == Properties.STATUS_DOWNLOADING:            
            return ResponseModel(message='already start download...', content=None)


        # ep download
        try:
            configRepository.updateOne({'catalog.{catalog_id}' : {'$exists': True}}, {'$set':{f'{epKey}.status':Properties.STATUS_DOWNLOADING}})

            # 다운로드
            if 'http' in config[epKey]['url']: # download
                result = await self.download(config[epKey]['url'], config[epKey]['fullPath'])
            else: # 경로가 file 인경우 copy
                result = await self.copy(config[epKey]['url'], config[epKey]['fullPath'])

            configRepository.updateOne({'catalog.{catalog_id}' : {'$exists': True}}, {'$set':{f'{epKey}.status':'', f'{epKey}.moddate':Utils.nowtime()}})
            # 파일백업
            # fileService.zipped(config[epKey]['fullPath'], config[epKey]['backupPath'])
            return ResponseModel(message='download complete', content=result)            

        except Exception as e :
            configRepository.updateOne({'catalog.{catalog_id}' : {'$exists': True}}, {'$set':{f'{epKey}.status':''}})            
            # raise HTTPException(status_code=400, detail=str(e))
            return ResponseModel(message=str(e))
        
                
        



    # aiohttp
    async def download(self, fromUrl, toPath):
        os.makedirs(os.path.dirname(toPath), exist_ok=True) # 경로확인/생성
        async with aiohttp.ClientSession() as session:
            async with session.get(fromUrl, timeout=None) as response:

                chunk_size = 1024*1024*10 # 10MB
                async with aiofiles.open(toPath, 'wb') as f:
                    while True:
                        chunk = await response.content.read(chunk_size)
                        if not chunk : break
                        await f.write(chunk)

                    result = self.getInfo(toPath)
                    # self.logger.info('Download complete : ' + str(result))
                    response.close()
                    return result


    # urllib.request
    def download_temp(self, fromUrl, toPath):
        os.makedirs(os.path.dirname(toPath), exist_ok=True)        
        request.urlretrieve(fromUrl, toPath)
        result = self.getInfo(toPath)
        return result

        
    # copy        
    async def copy(self, fromPath, toPath):
        os.makedirs(os.path.dirname(toPath), exist_ok=True) # 경로확인/생성
        chunk_size = 1024*1024*10 # 10MB
        async with aiofiles.open(fromPath, 'rb') as fromFile:
            async with aiofiles.open(toPath, 'wb') as toFile:
                while True:
                    chunk = await fromFile.read(chunk_size)
                    if not chunk : break
                    await toFile.write(chunk)
                
                result = self.getInfo(toPath)
                self.logger.info('Copy ' + str(result))
                return result


    def zipped(self, fromPath, toPath):
        os.makedirs(os.path.dirname(toPath), exist_ok=True) # 경로확인/생성    
        zip = zipfile.ZipFile(toPath, 'w', zipfile.ZIP_DEFLATED)
        zip.write(fromPath, arcname=os.path.basename(fromPath)) # 압축내용에 경로제거
        self.logger.info('Zipped : '+ str(self.getInfo(toPath)))

        # 7일 이전 삭제 (db로 관리해야할듯)        
        # delPath = '{toPath}.{date}.zip'.format(toPath=toPath, date=(datetime.now() + timedelta(days=-keepDay)).strftime('%Y%m%d'))
        # if os.path.isfile(delPath):
        #     os.remove(delPath)

    def delete(self, filePath):
        if os.path.exists(filePath) == False: 
            raise HTTPException(status_code=400, detail='file not found')
        else:
            os.remove(filePath)
            self.logger.info('Delete : ' + filePath)    
