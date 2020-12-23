from fastapi import HTTPException
from pymongo import MongoClient
from starlette.config import Config

class DatabaseManager():

    # dev local
    config = Config(".env")    
    host = config('host')
    port = config('port')
    database = config('database_name')
    collection = config('collection_name')
    mongo = None
    
    def __init__(self):
        self.mongo = self.getClient()

    # connect
    def getClient(self):
        try:
            result = MongoClient( self.host, int(self.port) )
        except:
            raise HTTPException(status_code=400, detail="mongo client connect error")
        return result

    # config read
    def findConfig(self, catalog_id=None):
        # _id는 json selialized 에러나기때문에 제외        
        
        if catalog_id == None :
            query = {}
        else :
            query = {'info.catalog_id':catalog_id}            
        
        return list(self.mongo[self.database][self.collection].find(query, {'_id':False}))

    
