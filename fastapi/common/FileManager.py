import urllib.request, os, time
from starlette.config import Config
from common.DatabaseManager import DatabaseManager

class FileManager():
    dbManager = DatabaseManager()

    def __init__(self):
        pass

    def getInfo(self, catalog_id=None):
        # file path
        config = self.dbManager.findConfig(catalog_id)
        epPath = config[0]['read']['path'] #하나/여러개를 분리해 명시하는게 좋을듯
        
        with urllib.request.urlopen(epPath) as f:
            meta = f.info()
            result = meta

        ''' 파일인경우
        exists = os.path.exists(filepath) # 파일 존재여부
        size = os.path.getsize(filepath) # 파일 크기
        access = os.path.access(filepath) # 파일 읽기가능 여부
        credate = time.ctime(os.path.getctime(filepath)) # 생성시간
        moddate = time.mtime(os.path.getmtime(filepath)) # 수정시간
        
        result = {'exists': exists,
                    'size': size,
                    'access': access,
                    'credate': credate,
                    'moddate': moddate}    
        '''
        f.close()
        return result

    def download(self, filepath=None):
        pass
