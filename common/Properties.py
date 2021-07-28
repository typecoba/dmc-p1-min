from starlette.config import Config
from common.Utils import Utils
import os
class Properties() :
    #server
    __server_host=''
    __server_port=''
    __server_domain='' # 파일접근 root 경로

    # config mongodb
    __config_db_host=''
    __config_db_port=''
    __config_db_database=''
    __config_db_collection=''

    # pixel mongodb
    __pixel_db_host=''
    __pixel_db_port=''
    __pixel_db_database=''
    __pixel_db_collection=''

    # path
    __root=''
    __epPath=''
    __epBackupPath=''
    __feedPath=''
    __feedBackupPath=''
    __logPath=''
    __convertLogPath=''

    # access token
    __facebookAccessToken=''

    # 상수
    STATUS_DOWNLOADING = 'DOWNLOADING'
    STATUS_CONVERTING = 'CONVERTING'
    SERVER_AUTO_RELOAD = False # 개발환경 자동리로드
    SERVER_API_HOST = '0.0.0.0' # uvicorn 실행 host
    SERVER_API_WORKERS = 6 # uvicorn 서버 멀티프로세스 수
    SERVER_PREFIX = 'local' # prod / dev / local
    

    '''
    더 스마트한 방법이 없을까?
    '''
    def __init__(self):    
        prop = Config('property.env')        
        self.__facebookAccessToken = prop('facebook_api_access_token')

        # ip check
        ip = Utils.getIP()
        rootPath = ''
        if ip == prop('prod_server_host') : # prod
            self.SERVER_PREFIX = 'prod'
            self.__server_domain = 'http://api.dmcf1.com'

        elif ip == prop('dev_server_host') : # dev
            self.SERVER_PREFIX = 'dev'
            self.__server_domain = f'http://{prop("dev_server_host")}'

        else : # 나머진 local로 간주
            self.SERVER_PREFIX = 'local'
            self.SERVER_AUTO_RELOAD = True
            rootPath = os.getcwd().replace('\\','/') # local에만 프로젝트 root 절대경로
            self.__server_domain = rootPath 
            
        
        self.__server_host = prop(f'{self.SERVER_PREFIX}_server_host')
        self.__server_port = prop(f'{self.SERVER_PREFIX}_server_port')        
        #
        self.__config_db_host = prop(f'{self.SERVER_PREFIX}_config_db_host')
        self.__config_db_port = prop(f'{self.SERVER_PREFIX}_config_db_port')
        self.__db_config_database = prop(f'{self.SERVER_PREFIX}_config_db_database_name')
        self.__db_config_collection = prop(f'{self.SERVER_PREFIX}_config_db_collection_name')
        #
        self.__pixel_db_host = prop(f'{self.SERVER_PREFIX}_pixel_db_host')
        self.__pixel_db_port = prop(f'{self.SERVER_PREFIX}_pixel_db_port')
        self.__db_pixel_database = prop(f'{self.SERVER_PREFIX}_pixel_db_database_name')
        self.__db_pixel_collection = prop(f'{self.SERVER_PREFIX}_pixel_db_collection_name')
        #
        self.__epPath =         rootPath + prop(f'{self.SERVER_PREFIX}_ep_path')
        self.__epBackupPath =   rootPath + prop(f'{self.SERVER_PREFIX}_ep_backup_path')
        self.__feedPath =       rootPath + prop(f'{self.SERVER_PREFIX}_feed_path')
        self.__feedBackupPath = rootPath + prop(f'{self.SERVER_PREFIX}_feed_backup_path')
        self.__logPath =        rootPath + prop(f'{self.SERVER_PREFIX}_log_path') # 프로젝트 root
        self.__convertLogPath = rootPath + prop(f'{self.SERVER_PREFIX}_convert_log_path') # 프로젝트 root
    
    
    def getServerHost(self):
        return self.__server_host

    def getServerPort(self):
        return int(self.__server_port) # port는 int로 받음
    
    def getServerDomain(self):
        return self.__server_domain

    def getConfigDBHost(self):
        return self.__config_db_host
        
    def getConfigDBPort(self):
        return self.__config_db_port
    
    def getConfigDBDatabase(self):
        return self.__db_config_database
    
    def getConfigDBCollection(self):
        return self.__db_config_collection
    
    def getPixelDBHost(self):
        return self.__pixel_db_host
        
    def getPixelDBPort(self):
        return self.__pixel_db_port
    
    def getPixelDBDatabase(self):
        return self.__db_pixel_database
    
    def getPixelDBCollection(self):
        return self.__db_pixel_collection

    def getEpPath(self):
        return self.__epPath
    
    def getEpBackupPath(self):
        return self.__epBackupPath
    
    def getFeedPath(self):
        return self.__feedPath
    
    def getFeedBackupPath(self):
        return self.__feedBackupPath
    
    def getLogPath(self):
        return self.__logPath

    def getConvertLogPath(self):
        return self.__convertLogPath
    
    def getFacebookAccessToken(self):
        return self.__facebookAccessToken