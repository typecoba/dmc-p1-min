from fastapi import HTTPException
from pymongo import MongoClient
from starlette.config import Config
from datetime import datetime, timedelta
import os
from common.Logger import Logger
from common.Properties import Properties

class ConfigRepository():
    prop = None # properties 객체
    mongo = None # mongo client

    # pixel crawl data
    def __init__(self):
        self.prop = Properties()
        self.mongo = self.getClient(self.prop.getHost(), int(self.prop.getPort()))
        self.configMongo = self.mongo[self.prop.getDatabase()][self.prop.getCollection()]
        
        

    # connect
    def getClient(self, host, port):
        try:
            result = MongoClient(host, port)
            return result
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))
        

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
        
        # catalog_id = config['catalog']['id']
        epName = config['info']['name']
        epFormat = config['ep']['format']
        dateDay = datetime.now().strftime('%Y%m%d')
        dateMonth = datetime.now().strftime('%Y%m')

        # ep
        epFileName = f'ep_{epName}.{epFormat}'
        epFullPath = f'{self.prop.getEpPath()}/{epFileName}'
        config['ep']['fullPath'] = epFullPath

        # catalog > feed
        for catalog_id, catalogDict in config['catalog'].items():
            feedPath = f'{self.prop.getFeedPath()}/{catalog_id}' # catalog_id 폴더
            feedAllFileName = f'feed_{catalog_id}.tsv'
            feedAllUpdateFileName = f'feed_{catalog_id}_update_all.tsv'
            
            # 피드가 한개인경우엔 의미없음
            catalogDict['feed_all'] = {'fullPath': f'{feedPath}/{feedAllFileName}', 
                                       'fullPath_update': f'{feedPath}/{feedAllUpdateFileName}'}
            # feed
            for feed_id, feed in catalogDict['feed'].items():
                feedFileName = f'feed_{catalog_id}_{feed_id}.tsv'
                config['catalog'][catalog_id]['feed'][feed_id] = {'fullPath':f'{feedPath}/{feedFileName}'} # 서버 www접근폴더로 설정해야함
                

        # update (update only)
        if 'ep_update' in config :
            # ep_update
            config['ep_update']['fullPath'] = f'{self.prop.getEpPath()}/ep_{epName}_update.{epFormat}'
            
            for catalog_id, catalogDict in config['catalog'].items():
                for feed_id, feed in catalogDict['feed'].items():                    
                    # feed_update
                    updateFeedPath = f'{self.prop.getFeedPath()}/{catalog_id}'
                    updateFeedFileName = f'feed_{catalog_id}_{feed_id}_update.tsv'              
                    config['catalog'][catalog_id]['feed'][feed_id]['fullPath_update'] = f'{updateFeedPath}/{updateFeedFileName}'

        # convert log
        logFileName = f'log_convert_{epName}.{dateMonth}.log' # 월별
        config['log'] = {'fullPath': f'{self.prop.getLogPath()}/{logFileName}'}