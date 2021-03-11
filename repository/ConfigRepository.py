from fastapi import HTTPException
from pymongo import MongoClient
from starlette.config import Config
from datetime import datetime, timedelta
import os
from common.Logger import Logger

class ConfigRepository():

    # config
    env = Config('config.env')
    host = env('db_config_host')
    port = env('db_config_port')
    db = env('db_config_database_name')
    col = env('db_config_collection_name')

    epPath=env('path_ep')
    epBackupPath=env('path_ep_backup')
    feedPath=env('path_feed')
    feedBackupPath=env('path_feed_backup')
    convertLogPath=env('path_convert_logs')

    mongo = None

    # pixel crawl data
    def __init__(self):
        self.mongo = self.getClient(self.host, int(self.port))
        self.configMongo = self.mongo[self.db][self.col]
        

    # connect
    def getClient(self, host, port):
        try:
            result = MongoClient(host, port)
        except:
            raise HTTPException(status_code=400, detail="mongo client connect error")
        return result

    # config read
    def findAll(self):
        config = list(self.configMongo.find({}, {'_id': False}))
        for row in config:
            self.setPath(row)
        return config


    def findOne(self, catalog_id=None):
        config = self.configMongo.find_one({f'catalog.{catalog_id}': {'$exists':True}}, {'_id': False})
        if config != None :
            self.setPath(config)
            return config
        else :
            raise HTTPException(status_code=400, detail='config not found')
                
    
    def insertOne(self, config=None):
        try:            
            return self.configMongo.insert_one(config)
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))


    def updateOne(self, oriValue, newValue):
        try:
            return self.configMongo.update_one(oriValue, newValue) # upsert=True
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))


    # 파일저장 Path 생성
    def setPath(self, config=None):
        root = os.getcwd().replace('\\', '/')

        # catalog_id = config['catalog']['id']
        epName = config['info']['name']
        file_format = config['ep']['format']
        dateDay = datetime.now().strftime('%Y%m%d')
        dateMonth = datetime.now().strftime('%Y%m')

        # ep
        epPath = f'{root}/{self.epPath}'
        epFileName = f'ep_{epName}.{file_format}'
        epFullPath = epPath + epFileName
        config['ep']['fullPath'] = epFullPath

        # catalog > feed
        for catalog_id, catalog in config['catalog'].items(): 
            # feed
            for feed_id, feed in catalog['feed'].items():                
                feedPath = f'{root}/{self.feedPath}{catalog_id}/' # catalog_id 폴더        
                config['catalog'][catalog_id]['feed'][feed_id] = {'fullPath':f'{feedPath}feed_{catalog_id}_{feed_id}.tsv'} # 서버 www접근폴더로 설정해야함            
        
            # config['catalog'][catalog_id]['feed_temp'] = f'{feedPath}feed_{catalog_id}_temp.tsv' # convert된 임시파일            

            # update (update only)
            if 'ep_update' in config :
                # ep_update
                epUpdatePath = f'{root}/{self.epPath}'
                epUpdateFileName = f'ep_{epName}_update.{file_format}'
                epUpdateFullPath = epUpdatePath + epUpdateFileName
                config['ep_update']['fullPath'] = epUpdateFullPath
                
                # feed_update
                feedUpdatePath = f'{root}/{self.feedPath}{catalog_id}/'
                feedUpdateFileName = f'feed_{catalog_id}_update.tsv'
                feedUpdateFullPath = feedUpdatePath + feedUpdateFileName                                            
                config['catalog'][catalog_id]['feed_update'] = {'fullPath': feedUpdateFullPath}

        # log
        logPath = f'{root}/{self.convertLogPath}'
        logFileName = f'log_convert_{epName}.{dateMonth}.log' # 월별
        logFullPath = logPath + logFileName
        config['log'] = {'fullPath': logFullPath}