import aiohttp
import requests
from urllib import parse
from common.Logger import Logger
from common.Properties import Properties

class FacebookAPI():
    # config
    prop = Properties()    

    def __init__(self):
        self.logger = Logger()

    def setLogger(self, logger=None):
        self.logger = logger

    '''
    isUpdateEp='false' 전체 삭제 후 업로드(default)
    isUpdateEp='true' 업데이트만 
    '''
    async def upload_async(self, feed_id='', feed_url='', isUpdateEp=False):
        api_url = f'https://graph.facebook.com/v9.0/{feed_id}/uploads'
        update_only= 'true' if isUpdateEp==True else 'false'
        params ={'update_only': update_only ,'access_token': self.prop.getFacebookAccessToken(), 'url': feed_url}

        self.logger.info('Upload '+str({'api_url':api_url, 'params':params}))
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False), trust_env=True) as session: # ssl인증확인 false
            async with session.post(api_url, data=params) as response:
                result = await response.text()
                self.logger.info('Result '+result)
                return result  
    
    def upload(self, feed_id='', feed_url='', isUpdateEp=False):
        api_url = f'https://graph.facebook.com/v9.0/{feed_id}/uploads'
        update_only= 'true' if isUpdateEp==True else 'false'
        params ={'update_only': update_only ,'access_token': self.prop.getFacebookAccessToken(), 'url': feed_url}

        self.logger.info('Upload '+str({'api_url':api_url, 'params':params}))
        response = requests.get(url=api_url, params=params)
        self.logger.info('Result '+response.text)
        return response.text