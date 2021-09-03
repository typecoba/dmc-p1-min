from urllib import request
from urllib.error import URLError, HTTPError
import requests
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
import json
# import boto3
# from tqdm import tqdm
import math




class FileService():
    def __init__(self):
        pass        

    def setLogger(self, logger=None):
        self.logger = logger    
    
    def getInfo(self, filePath=None):
        if filePath==None :            
            # raise HTTPException(400, 'filePath is required')
            return None
                
        if 'http' in filePath : # url인경우
            # file path
            try :
                with request.urlopen(filePath) as f:                
                    fileInfo = f.info()
                    f.close()
            except HTTPError as e :                
                # raise HTTPException(400, f'url is not open at {filePath}' )
                return None
            

            # str-> datetime -> timezone 적용 -> formatting
            mdatetime = parser.parse(fileInfo['Last-Modified']).astimezone(timezone('Asia/Seoul')).strftime('%Y-%m-%d %H:%M:%S %z')

            result = {
                'url': filePath,
                'size': int(fileInfo['Content-Length']),
                'size_formated' : Utils.sizeof_fmt(int(fileInfo['Content-Length'])),
                'last_moddate': mdatetime
            }

        else : # 파일인경우
            # file check
            if os.path.exists(filePath) == False:                 
                # raise HTTPException(400, f'file not found at {filePath}')
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
    4. 실패/생략시 exception이 아니라 logging
    *** convert process 내부에서 연동되므로 exception을 함수 외부로 빼야하나?

    '''    
    def getEpDownload(self, catalog_id=None, isUpdateEp=False): # type = '' or 'update'
        configRepository = ConfigRepository()
        config = configRepository.findOne(catalog_id)

        try :            
            # ep / ep_update flag
            epKey = 'ep_update' if isUpdateEp == True else 'ep'

            # ep_update 체크
            if isUpdateEp == True and 'ep_update' not in config: 
                return ResponseModel(message='ep_update not found in config', content='')

            # 원본/로컬파일 비교 체크
            epOriInfo = self.getInfo(config[epKey]['url'])          # 오리지널 ep info
            epInfo = self.getInfo(config[epKey]['fullPath'])        # 로컬 ep info
            if epInfo != None : # 다운받은 파일이 있는경우
                epOriSize = epOriInfo['size']                           # 오리지널 ep size
                epOriModDate = parser.parse(epOriInfo['last_moddate'])  # 오리지널 ep 생성시간
                epSize = epInfo['size']                                 # 로컬 ep size
                epModDate = parser.parse(epInfo['last_moddate'])        # 로컬 ep 생성시간                

                if epModDate >= epOriModDate and epSize == epOriSize : # 로컬ep시간이 최신이고 사이즈 같은경우
                    return ResponseModel(message='file not changed', content={ 'server': epOriInfo, 'local': epInfo })                    

            # 서버단위 중복 다운로드 체크
            if config[epKey]['status'] == Properties.STATUS_DOWNLOADING :                
                return ResponseModel(message='already start download')
            
            # 다운로드
            # 더 상세한 정보 logging 필요        
            configRepository.updateOne({'catalog.{catalog_id}' : {'$exists': True}}, {'$set':{f'{epKey}.status':Properties.STATUS_DOWNLOADING}})
            if 'http' in config[epKey]['url'] : # web
                result = self.download(config[epKey]['url'], config[epKey]['fullPath'])

            else : # local
                result = self.copy(config[epKey]['url'], config[epKey]['fullPath'])                
            configRepository.updateOne({'catalog.{catalog_id}' : {'$exists': True}}, {'$set':{f'{epKey}.status':'', f'{epKey}.moddate':Utils.nowtime()}})
            
            # 파일백업
            # fileService.zipped(config[epKey]['fullPath'], config[epKey]['backupPath'])
            return ResponseModel(message='download complete', content=result)

        except Exception as e : 
            configRepository.updateOne({'catalog.{catalog_id}' : {'$exists': True}}, {'$set':{f'{epKey}.status':'', f'{epKey}.moddate':Utils.nowtime()}})
            raise e            
        
        

    # aiohttp
    async def download_async(self, file_url:str, file_path:str):
        os.makedirs(os.path.dirname(file_path), exist_ok=True) # 경로확인/생성
        connector = aiohttp.TCPConnector(verify_ssl=False) # connector 인증서무시 명시
        async with aiohttp.ClientSession(connector=connector) as session: 
            async with session.get(file_url, timeout=None) as response:

                chunk_size = 1024*1024*10 # 10MB
                async with aiofiles.open(file_path, 'wb') as f:
                    while True:
                        chunk = await response.content.read(chunk_size)
                        if not chunk : break
                        await f.write(chunk)

                    result = self.getInfo(file_path)
                    self.logger.info('Download complete : ' + str(result))
                    response.close()
                    return result


    # requests    
    def download(self, file_url:str, file_path:str):
        # 경로생성
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'} #chrome
        with requests.get(file_url, headers=headers, stream=True, verify=False) as response :
            response.raise_for_status()
            with open(file_path, 'wb') as file :
                total_size = None
                chunk_size = 1024*10*10

                # 프로그래스바
                if 'Content-Length' in response.headers :
                    total_size = int(response.headers['Content-Length'])
                    # progress_bar = tqdm(total=total_size, position=0, leave=True, mininterval=0, miniters=1)

                for chunk in response.iter_content(chunk_size=chunk_size) :
                    if chunk :
                        file.write(chunk)
                    # if total_size != None:
                    #   progress_bar.update(len(chunk))
                
                print('\n')
                
                # 메모리비우기
                file.flush()
                os.fsync(file.fileno())
        
        return self.getInfo(file_path)

        
    # aiofiles        
    async def copy_async(self, path_from:str, path_to:str):
        os.makedirs(os.path.dirname(path_to), exist_ok=True) # 경로확인/생성
        chunk_size = 1024*1024*10 # 10MB
        async with aiofiles.open(path_from, 'rb') as fromFile:
            async with aiofiles.open(path_to, 'wb') as toFile:
                while True:
                    chunk = await fromFile.read(chunk_size)
                    if not chunk : break
                    await toFile.write(chunk)
                
                result = self.getInfo(path_to)
                self.logger.info('Copy ' + str(result))
                return result

    # shutil
    def copy(self, path_from:str, path_to:str) : 
        os.makedirs(os.path.dirname(path_to), exist_ok=True) # 경로확인/생성
        shutil.copy(path_from, path_to)


    def zipped(self, file_path:str, zip_path:str):
        os.makedirs(os.path.dirname(zip_path), exist_ok=True) # 경로확인/생성    
        zip = zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED)
        zip.write(file_path, arcname=os.path.basename(file_path)) # 압축내용에 경로제거
        self.logger.info('Zipped : '+ str(self.getInfo(zip_path)))

        # 7일 이전 삭제 (db로 관리해야할듯)        
        # delPath = '{toPath}.{date}.zip'.format(toPath=toPath, date=(datetime.now() + timedelta(days=-keepDay)).strftime('%Y%m%d'))
        # if os.path.isfile(delPath):
        #     os.remove(delPath)

    def delete(self, file_path:str):
        if os.path.exists(file_path) == False: 
            raise HTTPException(status_code=400, detail='file not found')
        else:
            os.remove(file_path)
            self.logger.info('Delete : ' + file_path)



    # def s3_upload(self, file_path, key_path):
    #     properties = Properties()
    #     s3Client = boto3.client('s3', 
    #         aws_access_key_id = properties.getAwsS3AccessKeyId(),
    #         aws_secret_access_key = properties.getAwsS3SecretAccessKey(),
    #         region_name = properties.getAwsS3Region())

    #     try : 
    #         s3Client.upload_file(file_path, properties.getAwsS3Bucket(), key_path)
    #         s3_url = f'https://{properties.getAwsS3Bucket()}.s3.{properties.getAwsS3Region()}.amazonaws.com/{key_path}'
    #         return s3_url

    #     except Exception as e :
    #         self.logger.info(e)
    #         return False