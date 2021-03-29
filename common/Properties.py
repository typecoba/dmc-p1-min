from starlette.config import Config
from common.Utils import Utils
import os
class Properties() :
    #server
    __server_host=''
    __server_port=''

    # mongodb
    __db_host=''
    __db_port=''
    __db_database=''
    __db_collection=''

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
        
        self.__server_host = prop(f'{prefix}_server_host')
        self.__server_port = prop(f'{prefix}_server_port')
        #
        self.__db_host = prop(f'{prefix}_db_host')
        self.__db_port = prop(f'{prefix}_db_port')
        self.__db_database = prop(f'{prefix}_db_database_name')
        self.__db_collection = prop(f'{prefix}_db_collection_name')
        #
        self.__epPath =         rootPath + prop(f'{prefix}_ep_path')
        self.__epBackupPath =   rootPath + prop(f'{prefix}_ep_backup_path')
        self.__feedPath =       rootPath + prop(f'{prefix}_feed_path')
        self.__feedBackupPath = rootPath + prop(f'{prefix}_feed_backup_path')
        self.__logPath =        os.getcwd().replace('\\','/') + prop(f'{prefix}_log_path') # 프로젝트 root
        self.__convertLogPath = os.getcwd().replace('\\','/') + prop(f'{prefix}_convert_log_path') # 프로젝트 root

    
    def getServerHost(self):
        return self.__server_host

    def getServerPort(self):
        return self.__server_port

    def getDBHost(self):
        return self.__db_host
        
    def getDBPort(self):
        return self.__db_port
    
    def getDBDatabase(self):
        return self.__db_database
    
    def getDBCollection(self):
        return self.__db_collection
    
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