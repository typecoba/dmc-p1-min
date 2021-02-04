import logging


class Logger():
    def __init__(self, name, logFileFullPath=None):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        self.formatter = logging.Formatter('[%(asctime)s] (%(levelname)s) : %(message)s')

        self.stream_handler = logging.StreamHandler()
        self.stream_handler.setFormatter(self.formatter)
        
        self.logger.addHandler(self.stream_handler)

        if logFileFullPath != None :
            file_handler = logging.FileHandler(logFileFullPath)
            self.logger.addHandler(file_handler)
    
    def get(self):
        return self.logger