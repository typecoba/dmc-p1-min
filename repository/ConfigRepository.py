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

    # config data
    def __init__(self):
        self.prop = Properties()
        self.mongo = self.getClient(self.prop.getConfigDBHost(), int(self.prop.getConfigDBPort()))
        self.configMongo = self.mongo[self.prop.getConfigDBDatabase()][self.prop.getConfigDBCollection()]
                

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
            raise HTTPException(status_code=400, detail=f'config not found at {catalog_id}')
                
    
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
        epName = config['info']['name']          
        epFormat = config['ep']['format'] if config['ep']['zipformat'] == '' else config['ep']['zipformat'] # ep원본 format으로 떨어지게 함
        media = config['info']['media'] # 매체
        dateDay = datetime.now().strftime('%Y%m%d')
        dateMonth = datetime.now().strftime('%Y%m')
        if media in ['facebook', 'google'] : # 페북(tsv,csv,xml), 구글(tsv,xml)  *ep 공유하기도 하기때문에 tsv로 통일            
            feedFormat = 'tsv'
            zipFormat = '.gz'
        elif media=='criteo' : # 크리테오 (csv,xml) *압축하지 않는다
            feedFormat = 'csv'
            zipFormat = ''
 
        # [ep]
        epFileName = f'ep_{epName}.{epFormat}'
        epFullPath = f'{self.prop.getEpPath()}/{epName}/{epFileName}'
        epUpdateFullPath = f'{self.prop.getEpPath()}/{epName}/ep_{epName}_update.{epFormat}'
        config['ep']['fullPath'] = epFullPath
        if 'ep_update' in config : # ep_update 있는경우
            config['ep_update']['fullPath'] = epUpdateFullPath

        # [ep][segment]
        # ep단위로 feed갯수만큼 한번만 분할하여 catalog끼리 활용
        # *feed 갯수는 카탈로그끼리 동일해야함
        segmentPath = []
        feedCount = len(config['catalog'][next(iter(config['catalog']))]['feed'])
        for i in range(feedCount) :
            segmentPath.append(os.path.dirname(epFullPath)+f'/segment_{i}.csv')
        config['ep']['segmentPath'] = segmentPath
        
        segmentPath = []
        if 'ep_update' in config : # ep_update 있는경우
            for i in range(feedCount) :
                segmentPath.append(os.path.dirname(epUpdateFullPath)+f'/segment_{i}_update.csv')
            config['ep_update']['segmentPath'] = segmentPath
        

        # [catalog][feed]
        for catalog_id, catalogDict in config['catalog'].items():
            feedPath = f'{self.prop.getFeedPath()}/{media}/{catalog_id}' # catalog_id 폴더            
            
            if self.prop.SERVER_PREFIX == 'local':
                publicFeedPath = feedPath
            else :
                publicFeedPath = f'{self.prop.getServerDomain()}/feed/{media}/{catalog_id}' # 외부접근용 도메인 경로

            for feed_id, feed in catalogDict['feed'].items():
                feedFileName = f'{media}_{catalog_id}_{feed_id}.{feedFormat}'
                feedUpdateFileName = f'{media}_{catalog_id}_{feed_id}_update.{feedFormat}'                
                config['catalog'][catalog_id]['feed'][feed_id]['fullPath'] = f'{feedPath}/{feedFileName}'
                config['catalog'][catalog_id]['feed'][feed_id]['publicPath'] = f'{publicFeedPath}/{feedFileName}{zipFormat}' # 외부접근 Path (domain/path)
                
                if 'ep_update' in config: # ep_update 있는경우
                    config['catalog'][catalog_id]['feed'][feed_id]['fullPath_update'] = f'{feedPath}/{feedUpdateFileName}'
                    config['catalog'][catalog_id]['feed'][feed_id]['publicPath_update'] = f'{publicFeedPath}/{feedUpdateFileName}{zipFormat}' # 외부접근 Path (domain/path)

        

        # convert log
        logFileName = f'log_convert_{epName}.{dateMonth}.log' # 월별
        config['log'] = {'fullPath': f'{self.prop.getConvertLogPath()}/{logFileName}'}