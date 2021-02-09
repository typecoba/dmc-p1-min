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
from common.Util import *
import zipfile

from asyncio.tasks import events

class FileService():    
    def __init__(self):
        self.logger = Logger().get() # 기본로거 root
        pass

    def setLogger(self, logger=None):
        self.logger = logger


    
    def getInfo(self, filePath=None):
        if 'http' in filePath :
            # file path
            with request.urlopen(filePath) as f:
                result = f.info()
                f.close()
            
            result = {
                'url': filePath,
                'size': Util.sizeof_fmt(int(result['Content-Length'])),
                'credate': parser.parse(result['Date']).strftime('%Y-%m-%d %H:%M:%S'),
                'moddate': parser.parse(result['Last-Modified']).strftime('%Y-%m-%d %H:%M:%S'),                
            }


        else : # 파일인경우
            if os.path.exists(filePath) == False: 
                raise HTTPException(status_code=400, detail='file not found')

            # exists = os.path.exists(filePath) # 파일 존재여부
            size = os.path.getsize(filePath) # 파일 크기
            ctime = os.path.getctime(filePath)  # 생성시간
            mtime = os.path.getmtime(filePath)  # 수정시간            
            atime = os.path.getatime(filePath)  # 마지막 엑세스시간            
            
            result = {
                    'path': filePath,
                    'size': Util.sizeof_fmt(size),
                    'credate': datetime.utcfromtimestamp(ctime).strftime('%Y-%m-%d %H:%M:%S'),
                    'moddate': datetime.utcfromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S'),
                    # 'accessdate': datetime.utcfromtimestamp(atime).strftime('%Y-%m-%d %H:%M:%S'), 
                    }            
        
        return result


    '''
    EP의 경우 http download만 있음
    download도 chunk로 받아야함
    test위해 local file등과같은 경우도 처리

    비동기 지원해야함
    '''        
    def download_temp(self, originalPath, downloadPath):
            if ('http://' in originalPath) == False : # url이 아닌 경우
                self.logger.info('-'*10+'Copy Start')
                self.logger.info(self.getInfo(originalPath))
                shutil.copy(originalPath, downloadPath) # 로컬파일 복사
                self.logger.info('-'*10+'Copy End')

            else : # url 경로            
                req = request.urlopen(originalPath)
                chunk_size = 1024*1024*10 # 일단 10Mb씩
                
                self.logger.info('-'*10+'Download Start')
                self.logger.info(self.getInfo(originalPath))
                
                with open(downloadPath, 'wb') as f:
                    while True:
                        chunk = req.read(chunk_size)                                        
                        if not chunk: break
                        f.write(chunk)
                    f.close()

                    self.logger.info('-'*10+'Download Complete')            
            

    # 파일이 없으면 첫부분정도만 확인할수 있나?
    # 파일이 있으면 중간부터 확인할 수 있나?
    # def getEpDetail():

    
    # aiohttp 
    async def download(self, fromUrl, toPath, backupPath):        
        self.logger.info('Download '+str(self.getInfo(fromUrl)))

        async with aiohttp.ClientSession() as session:
            async with session.get(fromUrl) as response:

                chunk_size = 1024*1024*10 # 10Mb
                async with aiofiles.open(toPath, 'wb') as f:
                    while True:
                        chunk = await response.content.read(chunk_size)
                        if not chunk : break
                        await f.write(chunk)
                        
                    # 파일백업
                    self.backup(toPath, backupPath)
        
        
                


    def backup(self, fromPath, toPath):        
        # 파일관리 (압축/7일 보관)
        zip = zipfile.ZipFile(toPath, 'w')
        zip.write(fromPath, compress_type=zipfile.ZIP_DEFLATED)       
        self.logger.info('Backup '+ str(self.getInfo(toPath)))
        # 7일 이전 삭제 (db로 관리해야할듯)        
        # delPath = '{toPath}.{date}.zip'.format(toPath=toPath, date=(datetime.now() + timedelta(days=-keepDay)).strftime('%Y%m%d'))
        # if os.path.isfile(delPath):
        #     os.remove(delPath)