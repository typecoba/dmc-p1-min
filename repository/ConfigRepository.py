from fastapi import HTTPException
from pymongo import MongoClient
from starlette.config import Config
from datetime import datetime, timedelta
import os
from common.Logger import Logger

class ConfigRepository():

    # dev config local
    envConfig = Config('config.env')
    host = envConfig('db_config_host')
    port = envConfig('db_config_port')
    db = envConfig('db_config_database_name')
    col = envConfig('db_config_collection_name')
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
    def setPath(self, config):
        root = os.getcwd().replace('\\', '/')
        catalog_id = config['info']['catalog_id']
        feed_id = config['info']['feed_id']
        file_format = config['ep']['format']
        media = config['info']['media']
        date = datetime.now().strftime('%Y%m%d')
        
        # ep                
        epFileName = f'ep_{catalog_id}_{feed_id}.{file_format}'            
        epPath = f'{root}/data/{media}/ep/{epFileName}'
        epBackupPath = f'{root}/data/{media}/ep/backup/{epFileName}.{date}.zip'

        # feed
        feedFileName = f'feed_{catalog_id}_{feed_id}.tsv'
        feedPath = f'{root}/data/{media}/feed/{feedFileName}'
        feedBackupPath = f'{root}/data/{media}/feed/backup/{feedFileName}.{date}.zip'

        # log
        logFileName = f'log_{catalog_id}_{feed_id}.log'
        logPath = f'{root}/data/{media}/log/{logFileName}'
        logBackupPath = f'{root}/data/{media}/log/backup/{logFileName}.{date}.zip'

        config['ep']['path']= epPath
        config['ep']['backupPath']=epBackupPath
        config['feed'] = {'path': feedPath, 'backupPath': feedBackupPath}
        config['log'] = {'path': logPath}
