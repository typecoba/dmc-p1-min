import logging, logging.handlers, logging.config
import os, datetime
# from log4mongo.handlers import MongoHandler

'''
logging모듈은 내부적으로 싱글톤으로 만들어짐
custom logger 선언시 하나의 인스턴스에 handler를 계속 추가하게되므로
핸들러 처리 필요함
'''
class Logger():
    # logger = None
    # streamHandler = None
    # fileHandler = None
    
    def __init__(self, name=None, filePath=None):
        rootPath = os.getcwd().replace('\\', '/')
        date = datetime.datetime.now().strftime('%Y%m%d')

        config = {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'minimum': {'format':'%(asctime)s %(message)s'},
                'default': {'format':'%(asctime)s [%(name)s] %(levelname)s : %(message)s'}
            },
            'handlers': {
                'console':{
                    'class':'logging.StreamHandler',
                    'formatter':'minimum',
                    'level':'NOTSET',
                },
                'file_root': {
                    'class':'logging.FileHandler',
                    'filename':f'{rootPath}/data/logs/server_log.{date}.log',
                    'formatter':'default',
                    'level':'NOTSET',
                },
            },
            'root':{'handlers':['console','file_root'],'level':'NOTSET'},
        }

        # file_convert handler 동적생성
        if name!=None and filePath != None :
            config['handlers']['file_convert'] = {
                'class':'logging.FileHandler',
                'filename':filePath,
                'formatter':'default',
                'level':'NOTSET'
            }
            config['loggers']={
                name:{'handlers':['file_convert'],'level':'NOTSET'}
            }

        # config 
        logging.config.dictConfig(config)

        # logger
        # name 기준으로 싱글톤으로 생성
        self.logger = logging.getLogger(name) 
        # self.logger.addHandler(MongoHandler(host='localhost'))
        

    def info(self, msg=None):
        self.logger.info(msg)