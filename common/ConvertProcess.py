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
    def execute(self, catalog_id=None):
        self.logger.info('==Feed Convert Process Start==')
        self.convertFilter = ConvertFilter(catalog_id, self.config) # 필터 클래스
        
        # 1. download
        # await self.fileService.download(self.config['ep']['url'], self.config['ep']['fullPath'])

        # 2. convert
        if 'filter' in self.config :
            self.logger.info('Custom Filter : ' + str(self.config['filter']))
        
                
        epLoad = self.chunkLoad(chunkSize=500000,
                                filePath=self.config['ep']['fullPath'],
                                seperator=self.config['ep']['sep'],
                                encoding=self.config['ep']['encoding'])
        

        # chunk별 feed분할쓰기
        feedIdList = list(self.config['catalog'][catalog_id]['feed'].keys())
        segmentIndexMap = self.getSegmentIndexMap(len(feedIdList)) # [[0, 1],[2, 3], [4, 5], [6, 7], [8, 9]]
        print(feedIdList)
        print(segmentIndexMap)

        ## convert 진행
        totalCount=0
        for num, chunkDF in enumerate(epLoad): # chunk load
            # filter
            chunkDF = self.convertFilter.run(chunkDF)            
        
            # makeSegment
            for i, feed_id in enumerate(feedIdList):
                segmentDF = chunkDF[chunkDF['id'].str[-1:].isin(segmentIndexMap[i])] # id끝자리 i
                # segmentDF 생성
                if num == 0 : # 초기화
                    globals()[f'segmentDF_{i}'] = segmentDF
                else :
                    globals()[f'segmentDF_{i}'] = pd.concat([globals()[f'segmentDF_{i}'], segmentDF], ignore_index=True) # dataframe 합치기
                
            # log
            totalCount = totalCount + chunkDF.shape[0]
            self.logger.info(f'..{format(totalCount,",")} row processed')

            # memory clean
            del[[chunkDF]]
            gc.collect()
            # break

        # 중복제거, feed write
        for i, feed_id in enumerate(feedIdList):
            globals()[f'segmentDF_{i}'] = globals()[f'segmentDF_{i}'].drop_duplicates(['id'], ignore_index=True)
            feedPath = self.config['catalog'][catalog_id]['feed'][feed_id]['fullPath']
            self.feedWrite(feedPath=feedPath, df=globals()[f'segmentDF_{i}'])
            self.fileService.zipped(feedPath, feedPath+".zip") # 압축

            # await self.facebookAPI.upload(feed_id, feedDict['fullPath']+".zip"))
            
            
            # memory clean
            del[[globals()[f'segmentDF_{i}']]]
            gc.collect()
                    
        
        self.logger.info('==Feed Convert Process End==')        
            

    # pixel데이터 다운로드 (to ep)
    def pixelDataDownLoad(self):
        pass

    # ep데이터 로드
    def chunkLoad(self, chunkSize=100000, filePath=None, seperator=None, encoding='utf-8') :
        ''' 
        chunksize 단위로 로드
        title에 구분자포함되어 에러나는경우 skip.. 원본ep 문제
        컬럼 정리를 위해 원본 컬럼 리스트를 세팅해 로드
        '''
        # 원본 컬럼리스트
        columns = pd.read_csv(filePath,
                                nrows=1, #한줄만 읽음
                                sep=seperator, # 명시
                                # lineterminator='\r',
                                encoding=encoding)
        columns = list(columns) 
        # print(columns)

        result = pd.read_csv(filePath,
                            nrows=None,
                            chunksize=chunkSize, # 일단 10만
                            header=0, # header row                            
                            dtype=str, # string type 인식
                            sep=seperator, # 명시
                            # lineterminator='\r',
                            error_bad_lines=False, # error skip
                            usecols=columns, # chunk에도 컬럼명 표기
                            encoding=encoding)
        return result
    

    #
    def feedWrite(self, num=0, feedPath=None, df=None):
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
    id끝자리기준 0-9를 피드갯수에 대해 분포시키기위한 index map 생성    
    '''
    def getSegmentIndexMap(self, feedCount=1): # 1,2,5,10으로만 분할
        # 알고리즘으로 풀려고 했지만 가장 직관적
        result = []
        if feedCount==1:
            result = [['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']]
        elif feedCount==2:
            result = [['0', '1', '2', '3', '4'],['5', '6', '7', '8', '9']]
        elif feedCount==5:
            result = [['0', '1'],['2', '3'],['4', '5'],['6', '7'],['8', '9']]
        elif feedCount==10:
            result = [['0'], ['1'], ['2'], ['3'], ['4'], ['5'], ['6'], ['7'], ['8'], ['9']]
        return result