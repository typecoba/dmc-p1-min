from fastapi import HTTPException
from pymongo import MongoClient
from starlette.config import Config
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
            raise HTTPException(
                status_code=400, detail="mongo client connect error")
        return result

    # config read
    def findAll(self):
        catalogConfig = list(
            self.mongo[self.db][self.col].find({}, {'_id': False}))
        for row in catalogConfig:
            self.setFullPath(row)
        return catalogConfig

    def findOne(self, catalog_id=None):
        catalogConfig = self.mongo[self.db][self.col].find_one(
            {'info.catalog_id': catalog_id}, {'_id': False})
        self.setFullPath(catalogConfig)
        return catalogConfig

    # 파일저장 fullPath 생성
    def setFullPath(self, config):
        root = os.getcwd().replace('\\', '/')

        epFullPath = '{root}/data/{media}/ep/ep_{catalog_id}_{feed_id}.{file_format}'.format(
            root=root,
            media=config['info']['media'],
            catalog_id=config['info']['catalog_id'],
            feed_id=config['info']['feed_id'],
            file_format=config['ep']['format']
        )

        feedFullPath = '{root}/data/{media}/feed/feed_{catalog_id}_{feed_id}.{file_format}'.format(
            root=root,
            media=config['info']['media'],
            catalog_id=config['info']['catalog_id'],
            feed_id=config['info']['feed_id'],
            file_format='tsv'
        )
        config['ep']['fullPath'] = epFullPath
        config['feed'] = {'fullPath': feedFullPath}
