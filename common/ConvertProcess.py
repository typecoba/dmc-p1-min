import pandas as pd
import os
import gc
import zipfile
from common.ConvertFilter import ConvertFilter
from common.FileService import FileService
from common.Logger import Logger
import requests
from starlette.config import Config

'''
execute 단위로 비동기 코루틴 생성
'''

class ConvertProcess():
    
    def __init__(self, catalogConfig):
        '''
        config관련 정보 분리해야함
        config를 router에서 최초 한번 불러와서 전달하는게 좋을듯
        '''
        # convert pipeline logger
        self.logger = Logger('convertLog', catalogConfig['log']['path']) # logger self.__class__.__qualname__

        self.catalogConfig = catalogConfig
        self.convertFilter = ConvertFilter(catalogConfig) # 필터 클래스
        self.fileService = FileService() # 파일 매니저 클래스        
        self.fileService.setLogger(self.logger) # 파이프라인 공통로거 삽입
        
        

    # download - epLoad - convert - feedWrite - feedUpload
    async def execute(self):
        # data download
        self.logger.info('==Feed Convert Process Start==')
        await self.fileService.download(self.catalogConfig['ep']['url'], self.catalogConfig['ep']['path'], self.catalogConfig['ep']['backupPath'])
        
        # chunk load                
        self.logger.info('Convert '+str(self.catalogConfig['custom']))        
        rowcount = 0
        for num, chunkDF in enumerate(self.epLoad()):            
            chunkDF = self.convertFilter.run(chunkDF) # convert            
            self.feedWrite(num, chunkDF) # write
            
            # log
            rowcount = rowcount + len(chunkDF)
            self.logger.join(str(rowcount)+'...')

            # memory clean
            del[[chunkDF]]
            gc.collect()
            # break       
        self.logger.join('\n') 
        

        # feed 백업
        self.fileService.backup(self.catalogConfig['feed']['path'], self.catalogConfig['feed']['backupPath'])

        # feed upload        
        # self.feedUpload()
        
        self.logger.info('==Feed Convert Process End====')
        


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
        columns = pd.read_csv(self.catalogConfig['ep']['path'],
                                nrows=1, #한줄만 읽음
                                sep=self.catalogConfig['ep']['sep'], # 명시
                                # lineterminator='\r',
                                encoding=self.catalogConfig['ep']['encoding'])
        columns = list(columns) 
        # print(columns)

        result = pd.read_csv(self.catalogConfig['ep']['path'],
                            nrows=None,
                            chunksize=100000, # 일단 10만
                            header=0, # header row                            
                            dtype=str, # string type 인식
                            sep=self.catalogConfig['ep']['sep'], # 명시
                            # lineterminator='\r',
                            error_bad_lines=False, # error skip
                            usecols=columns, # chunk에도 컬럼명 표기
                            encoding=self.catalogConfig['ep']['encoding'])
        return result
        
    
    # write는 pandas에서 구현되므로 process 클래스 내부에 작성하기로 함
    def feedWrite(self, num, df):
        if num == 0:
            mode='w' # 새로쓰기
            header=True 
        else:
            mode='a' # 이어쓰기
            header=False
        
        df.to_csv(self.catalogConfig['feed']['path'], 
                    index=False, # 자체 인덱스제거
                    sep='\t', 
                    mode=mode,
                    header=header, # 컬럼명 
                    encoding='utf-8')

    
    def feedUpload(self):
        pass