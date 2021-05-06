from common.Properties import Properties
from pymongo import MongoClient
from fastapi import HTTPException
import datetime

# 픽셀삽입을 통해 수집되는 제품데이터 로드
class ProductRepository():
    prop = None # properties 객체
    mongo = None # mongo client

    def __init__(self):
        self.prop = Properties()
        self.mongo = self.getClient(self.prop.getPixelDBHost(), int(self.prop.getPixelDBPort()))
        
    # connect
    def getClient(self, host, port):
        try:
            result = MongoClient(host, port)
            return result
        except Exception as e :
            raise HTTPException(status_code=400, detail=str(e))


    def selectProduct(self, catalog_id, period=0):
        collection = f'product_{catalog_id}'
        configMongo = self.mongo[self.prop.getPixelDBDatabase()][collection]

        targetTime = (datetime.datetime.now() - datetime.timedelta(days=period)) # 현재시간으로부터 {period} 일전
        products = list(configMongo.find({'log_time':{'$gt':targetTime}}, {'_id':False}).sort('log_time', 1))
        return products