import logging, logging.handlers

'''
logging모듈은 내부적으로 싱글톤으로 만들어짐
custom logger 선언시 하나의 인스턴스에 handler를 계속 추가하게되므로
핸들러 처리 필요함
'''
class Logger():
    def __init__(self, name='root', filePath=None):
        # print(name, filePath)
        self.logger = logging.getLogger(name)        
        
        if len(self.logger.handlers) == 0 :            
            self.logger.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')
            # streamHandler
            self.streamHandler = logging.StreamHandler()
            self.streamHandler.setFormatter(formatter)            
            self.logger.addHandler(self.streamHandler)

            # fileHandler
            # 실시간 쓰기 따로 구현해야하나?
            if filePath != None :
                self.fileHandler = logging.FileHandler(filePath)
                self.fileHandler.setFormatter(formatter)
                self.logger.addHandler(self.fileHandler)

        else :
            pass
    
    def get(self):
        return self.logger

    def info(self, msg=None):        
        if self.fileHandler != None :
            formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')
            self.streamHandler.setFormatter(formatter)
            self.fileHandler.setFormatter(formatter)
            self.fileHandler.terminator = '\n'
        self.logger.info(msg)

    def join(self, msg=None):
        if self.fileHandler != None :
            formatter = logging.Formatter('%(message)s')
            self.streamHandler.setFormatter(formatter)
            self.fileHandler.setFormatter(formatter)
            self.fileHandler.terminator = ''
        self.logger.info(msg)
