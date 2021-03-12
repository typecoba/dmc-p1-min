import aiohttp
import requests
from urllib import parse
from starlette.config import Config
from common.Logger import Logger

class FacebookAPI():
    # config
    config = Config('config.env')
    access_token = config('access_token')

    def __init__(self):        
        self.logger = Logger() # 기본로거
        pass

    def setLogger(self, logger=None):
        self.logger = logger

    '''
    isUpdate='false' 전체 삭제 후 업로드(default)
    isUpdate='true' 업데이트만 
    '''
    async def upload(self, feed_id, feed_url, isUpdate):
        feed_url = 'http://api.dmcf1.com/feed/141118536454632/watermark_141118536454632.json.gz' # test
        api_url = f'https://graph.facebook.com/v9.0/{feed_id}/uploads'        
        update_only= 'true' if isUpdate else 'false'                            
        params ={'update_only': update_only ,'access_token': self.access_token, 'url': feed_url}

        self.logger.info('Upload '+str({'api_url':api_url, 'params':params}))                    
        async with aiohttp.ClientSession() as session:
            async with session.post(api_url, data=params) as response:
                result = await response.text()
                self.logger.info('Result '+result)
                return result
        
    def upsert(self):
        pass