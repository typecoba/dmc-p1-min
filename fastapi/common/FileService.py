from urllib import request
import os
from datetime import datetime 
import shutil
from starlette.config import Config
from fastapi import HTTPException

class FileService():    
    def __init__(self):
        pass

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
            shutil.copy(originalPath, downloadPath) # 로컬파일 복사

        else : # url 경로            
            req = request.urlopen(originalPath)
            chunk_size = 1024*1000*10 # 일단 10Mb씩
            with open(downloadPath, 'wb') as f:
                while True:
                    chunk = req.read(chunk_size)                                        
                    if not chunk: break
                    f.write(chunk)

                f.close()
        




    # 파일이 없으면 첫부분정도만 확인할수 있나?
    # 파일이 있으면 중간부터 확인할 수 있나?
    # def getEpDetail():