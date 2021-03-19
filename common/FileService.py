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
        self.logger = Logger() # 기본로거 root

    def setLogger(self, logger=None):
        self.logger = logger    
    
    def getInfo(self, filePath=None):        
        if 'http' in filePath :
            # file path
            with request.urlopen(filePath) as f:
                response = f.info()
                f.close()

            # str-> datetime -> timezone 적용 -> formatting
            mdatetime = parser.parse(response['Last-Modified']).astimezone(timezone('Asia/Seoul')).strftime('%Y-%m-%d %H:%M:%S %Z')

            result = {
                'url': filePath,
                'size': Utils.sizeof_fmt(int(response['Content-Length'])),                
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
            mdatetime = datetime.fromtimestamp(mtime, timezone('Asia/Seoul')).strftime('%Y-%m-%d %H:%M:%S %Z')
            result = {
                'path': filePath,
                'size': Utils.sizeof_fmt(size),
                'last_moddate': mdatetime
            }
        
        return result


    '''
    EP의 경우 http download만 있음
    download도 chunk로 받아야함
    test위해 local file등과같은 경우도 처리

    비동기 지원해야함
    '''
    # 파일이 없으면 첫부분정도만 확인할수 있나?
    # 파일이 있으면 중간부터 확인할 수 있나?
    # def getEpDetail():

    async def getEp(self, fromUrl, toPath):
        os.makedirs(os.path.dirname(toPath), exist_ok=True) # 경로확인/생성
        
        if 'http' in fromUrl: # download            
            return await self.download(fromUrl, toPath)
        else: # file copy
            return await self.copy(fromUrl, toPath)
    

    # epdownload check & download
    async def getEpDownload(self, catalog_id):
        configRepository = ConfigRepository()
        
        config = configRepository.findOne(catalog_id)

        # 원본파일 변동 확인
        if os.path.isfile(config['ep']['fullPath']) : # 다운받은 파일이 있는 경우
            epOriInfo = self.getInfo(config['ep']['url'])           # 오리지널 ep info
            epOriSize = epOriInfo['size']                           # 오리지널 ep size
            epOriModDate = parser.parse(epOriInfo['last_moddate'])  # 오리지널 ep 생성시간
            epInfo = self.getInfo(config['ep']['fullPath'])         # 로컬 ep info
            epSize = epInfo['size']                                 # 로컬 ep size
            epModDate = parser.parse(epInfo['last_moddate'])        # 로컬 ep 생성시간

            if epModDate > epOriModDate : #  or epSize == epOriSize:
                content = {
                    'server':{'url':config['ep']['url'], 'moddate': epOriModDate},
                    'local': {'path':config['ep']['fullPath'], 'moddate': epModDate}
                }
                return ResponseModel(message='file not changed', content=content)

        # 서버단위 중복 다운로드 방지
        if config['ep']['status'] == Properties.STATUS_DOWNLOADING:            
            return ResponseModel(message='already start download...', content=None)


        # ep download
        try:
            configRepository.updateOne({'catalog.{catalog_id}' : {'$exists': True}}, {'$set':{'ep.status':Properties.STATUS_DOWNLOADING}})

            # 다운로드
            if 'http' in config['ep']['url']: # download
                result = await self.download(config['ep']['url'], config['ep']['fullPath'])
            else: # file copy
                result = await self.copy(config['ep']['url'], config['ep']['fullPath'])

            configRepository.updateOne({'catalog.{catalog_id}' : {'$exists': True}}, {'$set':{'ep.status':'', 'ep.moddate':Utils.nowtime()}})
            # 파일백업
            # fileService.zipped(config['ep']['fullPath'], config['ep']['backupPath'])

            return ResponseModel(message='download complete', content=result)            

        except Exception as e :
            configRepository.updateOne({'catalog.{catalog_id}' : {'$exists': True}}, {'$set':{'ep.status':''}})
            raise HTTPException(status_code=400, detail=str(e))
                
        



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
                    self.logger.info('Download complete : ' + str(result))
                    response.close()
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
