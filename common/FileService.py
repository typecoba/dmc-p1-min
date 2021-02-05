from urllib import request
import os
from datetime import datetime 
import shutil

import asyncio
import aiohttp
import aiofiles

from starlette.config import Config
from fastapi import HTTPException
from common.Logger import Logger

class FileService():    
    def __init__(self):        
        self.logger = Logger('root').get() # 공통로거 생성
        pass

    def setLogger(self, logger=None):
        self.logger = logger

    def getInfo(self, filePath=None):
        if 'http' in filePath :
            # file path
            with request.urlopen(filePath) as f:
                result = f.info()
                f.close()

        else : # 파일인경우
            if os.path.exists(filePath) == False: 
                raise HTTPException(status_code=400, detail='file not found')

            exists = os.path.exists(filePath) # 파일 존재여부
            size = os.path.getsize(filePath) # 파일 크기
            ctime = os.path.getctime(filePath)  # 생성시간
            mtime = os.path.getmtime(filePath)  # 수정시간            
            atime = os.path.getatime(filePath)  # 마지막 엑세스시간

            # lastdate = os.path.getattime(filePath) # 마지막 엑세스 시간          
            result = {
                    'exists': exists, 
                    'size': size,                     
                    'credate': datetime.utcfromtimestamp(ctime).strftime('%Y-%m-%d %H:%M:%S'),
                    'moddate': datetime.utcfromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S'),
                    'accessdate': datetime.utcfromtimestamp(atime).strftime('%Y-%m-%d %H:%M:%S'), 
                    }
        
        return result


    '''
    server download Ep의 경우만 있음
    download도 chunk로 받아야함
    url이 기본이지만 local file등과같은 경우도 처리
    '''    
    async def download(self, originalPath, downloadPath):    

        if ('http://' in originalPath) == False : # url이 아닌 경우
            self.logger.info('-'*10+'Copy Start')
            self.logger.info(self.getInfo(originalPath))
            shutil.copy(originalPath, downloadPath) # 로컬파일 복사
            self.logger.info('-'*10+'Copy End')

        else : # url 경로            
            req = request.urlopen(originalPath)
            chunk_size = 1024*1000*10 # 일단 10Mb씩
            
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


    # aiofile test
    # file 읽기, 쓰기에 멀티프로세스 사용해야 함..
    async def aDownload(self, fromPath, toPath):
        async with aiohttp.ClientSession() as session:
            async with session.get(fromPath) as resp:                
                async with aiofiles.open(toPath, 'w', encoding='utf-8') as f:
                    await f.write(await resp.text())
                    f.close()
        
        
        
            

