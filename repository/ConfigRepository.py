from fastapi import HTTPException
from pymongo import MongoClient
from starlette.config import Config
from datetime import datetime, timedelta
import os
from common.Logger import Logger

class ConfigRepository():

    # config
    config = Config('config.env')
    host = config('db_config_host')
    port = config('db_config_port')
    db = config('db_config_database_name')
    col = config('db_config_collection_name')

    epPath=config('path_ep')
    epBackupPath=config('path_ep_backup')
    feedPath=config('path_feed')
    feedBackupPath=config('path_feed_backup')
    convertLogPath=config('path_convert_logs')

    mongo = None

    # pixel crawl data
    def __init__(self):
        self.mongo = self.getClient(self.host, int(self.port))
        

    # connect
    def getClient(self, host, port):
        try:
            result = MongoClient(host, port)
        except:
            raise HTTPException(status_code=400, detail="mongo client connect error")
        return result

    # config read
    def findAll(self):
        catalogConfig = list(self.mongo[self.db][self.col].find({}, {'_id': False}))
        for row in catalogConfig:
            self.setPath(row)
        return catalogConfig

    def findOne(self, catalog_id=None):
        catalogConfig = self.mongo[self.db][self.col].find_one({'info.catalog_id': catalog_id}, {'_id': False})
        self.setPath(catalogConfig)
        return catalogConfig

    # 파일저장 Path 생성
    def setPath(self, catalogConf):
        root = os.getcwd().replace('\\', '/')
   

        catalog_id = catalogConf['info']['catalog_id']
        feed_id = catalogConf['info']['feed_id']
        file_format = catalogConf['ep']['format']
        dateDay = datetime.now().strftime('%Y%m%d')
        dateMonth = datetime.now().strftime('%Y%m')

        # ep                
        epFileName = f'ep_{catalog_id}_{feed_id}.{file_format}'            
        epFullPath = f'{root}/{self.epPath}/{epFileName}'
        epBackupPath = f'{root}/{self.epBackupPath}/{epFileName}.{dateDay}.zip' # 일별

        # feed
        feedFileName = f'feed_{catalog_id}_{feed_id}.tsv'
        feedFullPath = f'{root}/{self.feedPath}/{feedFileName}'
        feedBackupPath = f'{root}/{self.feedBackupPath}/{feedFileName}.{dateDay}.zip' # 일별

        # convert log
        logFileName = f'log_convert_{catalog_id}.{dateMonth}.log' # 월별
        logFullPath = f'{root}/{self.convertLogPath}/{logFileName}'

        catalogConf['ep']['path']= epFullPath
        catalogConf['ep']['backupPath']=epBackupPath
        catalogConf['feed'] = {'path': feedFullPath, 'backupPath': feedBackupPath}
        catalogConf['log'] = {'path': logFullPath}
