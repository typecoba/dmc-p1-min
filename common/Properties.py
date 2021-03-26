from starlette.config import Config
from common.Utils import Utils
import os
class Properties() :
    # database
    __ip=''
    __host=''
    __port=''
    __database=''
    __collection=''

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

    

    '''
    더 스마트한 방법이 없을까?
    '''
    def __init__(self):
        prop = Config('property.env')
        self.__facebookAccessToken = prop('facebook_api_access_token')

        # ip check
        ip = Utils.getIP()
        rootPath = ''
        prefix = ''
        if ip == '192.168.0.181' : # prod
            prefix = 'prod'
            self.SERVER_AUTO_RELOAD = False

        elif ip == '10.94.1.215' : # dev
            prefix = 'dev'
            self.SERVER_AUTO_RELOAD = False

        else : # local
            prefix = 'local'
            self.SERVER_AUTO_RELOAD = True
            rootPath = os.getcwd().replace('\\','/') # 프로젝트 root 절대경로
        
        self.__host = prop(f'{prefix}_db_host')
        self.__port = prop(f'{prefix}_db_port')
        self.__database = prop(f'{prefix}_db_database_name')
        self.__collection = prop(f'{prefix}_db_collection_name')
        #
        self.__epPath =         rootPath + prop(f'{prefix}_ep_path')
        self.__epBackupPath =   rootPath + prop(f'{prefix}_ep_backup_path')
        self.__feedPath =       rootPath + prop(f'{prefix}_feed_path')
        self.__feedBackupPath = rootPath + prop(f'{prefix}_feed_backup_path')
        self.__logPath =        os.getcwd().replace('\\','/') + prop(f'{prefix}_log_path') # 프로젝트 root
        self.__convertLogPath = os.getcwd().replace('\\','/') + prop(f'{prefix}_convert_log_path') # 프로젝트 root

    
    def getHost(self):
        return self.__host
        
    def getPort(self):
        return self.__port
    
    def getDatabase(self):
        return self.__database
    
    def getCollection(self):
        return self.__collection
    
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