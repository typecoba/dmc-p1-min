import pandas as pd
import os
import gc
import zipfile
from common.ConvertFilter import ConvertFilter
from common.FileService import FileService
import requests
from starlette.config import Config

class ConvertProcess():
    
    def __init__(self, catalogConfig):
        '''
        config관련 정보 분리해야함
        config를 router에서 최초 한번 불러와서 전달하는게 좋을듯
        '''        
        self.catalogConfig = catalogConfig
        self.convertFilter = ConvertFilter(catalogConfig) # 필터 클래스
        self.fileService = FileService() # 파일 매니저 클래스

    # download - epLoad - convert - feedWrite - feedUpload
    def execute(self):
        # data download
        # self.fileService.download(self.catalogConfig['ep']['url'], self.catalogConfig['ep']['fullPath'])
        
        # chunk load                
        for num, chunkDF in enumerate(self.epLoad()):
            # convert
            chunkDF = self.convertFilter.run(chunkDF)            

            # feed write
            self.feedWrite(num, chunkDF)
            
            # log 임시
            print(len(chunkDF), end='..', flush=True)

            # memory clean
            del[[chunkDF]]
            gc.collect()            
            # break

        # 압축할 필요가 있나?

        # feed upload
        # self.feedUpload()

        print('\ncomplete')


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
        columns = pd.read_csv(self.catalogConfig['ep']['fullPath'],
                                nrows=1, #한줄만 읽음
                                sep=self.catalogConfig['ep']['sep'], # 명시
                                # lineterminator='\r',
                                encoding=self.catalogConfig['ep']['encoding'])
        columns = list(columns) 
        # print(columns)

        result = pd.read_csv(self.catalogConfig['ep']['fullPath'],
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
        
        df.to_csv(self.catalogConfig['feed']['fullPath'], 
                    index=False, # 자체 인덱스제거
                    sep='\t', 
                    mode=mode,
                    header=header, # 컬럼명 
                    encoding='utf-8')                

    
    def feedUpload(self):
        pass