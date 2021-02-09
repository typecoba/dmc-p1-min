import logging, logging.handlers, logging.config
import os, datetime
'''
logging모듈은 내부적으로 싱글톤으로 만들어짐
custom logger 선언시 하나의 인스턴스에 handler를 계속 추가하게되므로
핸들러 처리 필요함
'''
class Logger():
    rootPath = os.getcwd().replace('\\', '/')
    date = datetime.datetime.now().strftime('%Y%m%d')

    config = {
        'version': 1,
        'formatters': {
            'minimum': {'format':'%(asctime)s %(message)s'},
            'default': {'format':'%(asctime)s [%(name)s] : %(message)s'}
        },
        'handlers': {
            'console':{
                'class':'logging.StreamHandler',
                'formatter':'minimum',
                'level':'INFO',
            },
            'file': {
                'class':'logging.FileHandler',
                'filename':f'{rootPath}/data/logs/server_log.{date}.log',
                'formatter':'default',
                'level':'INFO',
            },
        },
        'root':{'handlers':['console','file'], 'level':'INFO'},
        'loggers':{
            'parent':{'level':'INFO'}, 
            'parent.child':{'level':'DEBUG'}
        }
    }

    def __init__(self, name=None):        
        logging.config.dictConfig(self.config)

        self.logger = logging.getLogger(name) # name 기준으로 싱글톤으로 생성                            
        # self.streamHandler = None
        # self.fileHandler = None

        # self.messageFormatter = logging.Formatter('%(message)s')
        # self.simpleFormatter = logging.Formatter('%(asctime)s : %(message)s')
        # self.defaultFormatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s] : %(message)s')

        # if len(self.logger.handlers) == 0 :
        #     self.logger.setLevel(logging.INFO)
            
        #     # streamHandler
        #     self.streamHandler = logging.StreamHandler()
        #     self.streamHandler.setFormatter(self.simpleFormatter)            
        #     self.logger.addHandler(self.streamHandler)

        #     # fileHandler            
        #     if filePath != None :
        #         self.fileHandler = logging.FileHandler(filePath)
        #         self.fileHandler.setFormatter(self.simpleFormatter)
        #         self.logger.addHandler(self.fileHandler)        
    
    def get(self):
        return self.logger

    
    def info(self, msg=None):
        # if self.fileHandler != None :            
        #     self.streamHandler.setFormatter(self.simpleFormatter)
        #     self.fileHandler.setFormatter(self.simpleFormatter)
        #     self.streamHandler.terminator = '\n'
        #     self.fileHandler.terminator = '\n'
        self.logger.info(msg)

    # 메세지만 연결
    def join(self, msg=None):
        # if self.fileHandler != None :            
        #     self.streamHandler.setFormatter(self.messageFormatter)
        #     self.fileHandler.setFormatter(self.messageFormatter)
        #     self.fileHandler.terminator = ''
        #     self.fileHandler.terminator = ''
        self.logger.info(msg)
