import pandas as pd
import os
import gc
import zipfile
from common.ConvertFilter import ConvertFilter
from common.FileService import FileService
from common.Logger import Logger
from common.FacebookAPI import FacebookAPI
import requests
from starlette.config import Config

'''
execute 단위로 비동기 코루틴 생성
'''

class ConvertProcess():
    
    def __init__(self, config=None):
        '''
        config관련 정보 분리해야함
        config를 router에서 최초 한번 불러와서 전달하는게 좋을듯
        '''

        # convert pipeline logger
        # self.catalog_id = config['catalog']['id']  
        epName = config['info']['name']
        logPath = config['log']['fullPath']
        self.logger = Logger(name=f'log_{epName}', filePath=logPath) # logger self.__class__.__qualname__

        self.config = config                
        self.fileService = FileService() # 파일 매니저 클래스        
        self.fileService.setLogger(self.logger) # 파이프라인 공통로거 삽입
        self.facebookAPI = FacebookAPI() # facebook api
        self.facebookAPI.setLogger(self.logger)    


    # download -> (epLoad -> filter -> feedWrite) -> feedUpload
    async def execute(self, catalog_id=None):
        self.logger.info('==Feed Convert Process Start==')
        self.convertFilter = ConvertFilter(catalog_id, self.config) # 필터 클래스
        
        # 1. download
        # await self.fileService.download(self.config['ep']['url'], self.config['ep']['fullPath'])

        # 2. convert
        if 'filter' in self.config :
            self.logger.info('Custom Filter : ' + str(self.config['filter']))
        
        
        ## 전체 count 파악
        self.chunkCount = 0
        for num, chunkDF in enumerate(self.epLoad()):
            self.chunkCount = num+1 # 마지막 chunk num만 확인            
            [[chunkDF]]
            gc.collect()

        ## convert 진행
        for num, chunkDF in enumerate(self.epLoad()): # chunk load
            # filter
            chunkDF = self.convertFilter.run(chunkDF) 

            # write
            self.feedWrite(num, self.config['catalog'][catalog_id]['feed_temp'], chunkDF) # write
                        
            # log            
            percent = format(num/self.chunkCount*100, '.1f')
            self.logger.info(f'..{percent}%')

            # memory clean
            del[[chunkDF]]
            gc.collect()
            # break
        

        # segment분할 (추후기능추가?)
        self.feedSegmentation(catalog_id=catalog_id)

        # feed 백업
        
        
        # 압축, tempfile삭제, tsv파일 삭제, api upload
        # for feed_id, feedDict in self.config['feed']['id'].items() :            
            # self.fileService.zipped(feedDict['fullPath'], feedDict['fullPath']+".zip") # 압축
            # self.fileService.delete(feedDict['fullPath']) # tsv삭제
            # await self.facebookAPI.upload(feed_id, feedDict['fullPath']+".zip"))
        # self.fileService.delete(self.config['feed']['tempFilePath'])

        # feed upload
        
        
        self.logger.info('==Feed Convert Process End==')        
            

    # pixel데이터 다운로드 (to ep)
    def pixelDataDownLoad(self):
        pass

    # ep데이터 로드
    def epLoad(self):
        ''' 
        chunksize 단위로 로드
        title에 구분자포함되어 에러나는경우 skip.. 원본ep 문제
        컬럼 정리를 위해 원본 컬럼 리스트를 세팅해 로드
        '''
        # 원본 컬럼리스트
        columns = pd.read_csv(self.config['ep']['fullPath'],
                                nrows=1, #한줄만 읽음
                                sep=self.config['ep']['sep'], # 명시
                                # lineterminator='\r',
                                encoding=self.config['ep']['encoding'])
        columns = list(columns) 
        # print(columns)

        result = pd.read_csv(self.config['ep']['fullPath'],
                            nrows=None,
                            chunksize=100000, # 일단 10만
                            header=0, # header row                            
                            dtype=str, # string type 인식
                            sep=self.config['ep']['sep'], # 명시
                            # lineterminator='\r',
                            error_bad_lines=False, # error skip
                            usecols=columns, # chunk에도 컬럼명 표기
                            encoding=self.config['ep']['encoding'])
        return result

    def feedLoad(self, feedPath):
        # 원본 컬럼리스트
        columns = pd.read_csv(feedPath,
                                nrows=1, #한줄만 읽음
                                sep='\t', # 명시
                                # lineterminator='\r',
                                encoding='utf-8')
        columns = list(columns)
        
        result = pd.read_csv(feedPath,
                            nrows=None,
                            chunksize=100000, # 일단 10만
                            header=0, # header row                            
                            dtype=str, # string type 인식
                            sep='\t', # 명시
                            # lineterminator='\r',
                            error_bad_lines=False, # error skip
                            usecols=columns, # chunk에도 컬럼명 표기
                            encoding='utf-8')
        return result
        
    

    #
    def feedWrite(self, num, feedPath, df):
        os.makedirs(os.path.dirname(feedPath), exist_ok=True) # 경로확인/생성
        if num == 0:
            mode='w' # 새로쓰기
            header=True 
        else:
            mode='a' # 이어쓰기
            header=False
        
        df.to_csv(feedPath, 
                    index=False, # 자체 인덱스제거
                    sep='\t', 
                    mode=mode,
                    header=header, # 컬럼명 
                    encoding='utf-8')    


    '''
    중복제거
    피드 조건별 분할
    -중복제거하려면 어짜피 메모리에 다올려야함
    -컬럼정리된 파일을 운용하는게 메모리 덜 먹음..(8G->2G)
    '''
    def feedSegmentation(self, type='default', catalog_id=None):
        if catalog_id==None : return None
        
        feedDict = self.config['catalog'][catalog_id]['feed']
        self.logger.info(f'FeedSegmentation : {type:{type}, catalog_id:{catalog_id}, feed_id:{feedDict}}')

        if type=='default' :
            # 중복제거 test
            tempFeed = pd.read_csv(self.config['catalog'][catalog_id]['feed_temp'],
                        header=0, # header row
                        dtype=str, # string type 인식
                        sep='\t', # 명시
                        # lineterminator='\r',
                        error_bad_lines=False, # error skip
                        encoding='utf-8')
            
            tempFeed = tempFeed.drop_duplicates(['id'],keep='first',ignore_index=True)            
            countPerFeed = int(round(tempFeed.shape[0]/len(feedDict.keys())))

            startRow = 0
            endRow = startRow + countPerFeed
            for feed_id in feedDict.keys() :
                print(startRow,endRow)

                feedDF = tempFeed[startRow:endRow] # 분할
                feedDF.to_csv(feedDict[feed_id]['fullPath'], index=False, sep='\t', mode='w', encoding='utf-8') # feed write
                self.fileService.zipped(feedDict[feed_id]['fullPath'], feedDict[feed_id]['fullPath']+".zip") # zipped
                # self.fileService.delete(feedDict[feed_id]['fullPath']) # feed.tsv 삭제
                
                startRow = endRow+1
                endRow = startRow+countPerFeed

            # memory clean
            del[[tempFeed]]
            gc.collect()
