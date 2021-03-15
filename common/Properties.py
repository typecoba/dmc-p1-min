from starlette.config import Config
import socket
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
    
    '''
    더 스마트한 방법이 없을까?
    '''

    def __init__(self):
        prop = Config('property.env')
        self.__facebookAccessToken = prop('facebook_api_access_token')

        # ip check
        ip = socket.gethostbyname(socket.getfqdn())
        if ip == '192.168.0.181' : # prod
            pass

        elif ip == '10.94.1.215' : # dev
            self.__host = prop('dev_db_host')
            self.__port = prop('dev_db_port')
            self.__database = prop('dev_db_database_name')
            self.__collection = prop('dev_db_collection_name')

            #
            self.__epPath = f'{prop("dev_ep_path")}'
            self.__epBackupPath = f'{prop("dev_ep_backup_path")}'
            self.__feedPath = f'{prop("dev_feed_path")}'
            self.__feedBackupPath = f'{prop("dev_feed_backup_path")}'
            self.__logPath = f'{prop("dev_log_path")}'
            self.__convertLogPath = f'{prop("dev_convert_log_path")}'
            pass

        else : # local
            self.__host = prop('local_db_host')
            self.__port = prop('local_db_port')
            self.__database = prop('local_db_database_name')
            self.__collection = prop('local_db_collection_name')
            
            # 프로젝트 루트 기준 생성
            root = os.getcwd().replace('\\','/')
            self.__epPath = f'{root}/{prop("local_ep_path")}'
            self.__epBackupPath = f'{root}/{prop("local_ep_backup_path")}'
            self.__feedPath = f'{root}/{prop("local_feed_path")}'
            self.__feedBackupPath = f'{root}/{prop("local_feed_backup_path")}'
            self.__logPath = f'{root}/{prop("local_log_path")}'
            self.__convertLogPath = f'{root}/{prop("local_convert_log_path")}'
            pass

    
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
        return self.__feedAccessToken
    
