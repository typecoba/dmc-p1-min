import pandas as pd
import os
import gc
import zipfile
from common.ConvertFilter import ConvertFilter
from common.FileService import FileService
from common.Logger import Logger
from common.FacebookAPI import FacebookAPI
from common.Properties import Properties
import requests
import csv
from multiprocessing import Process, Queue, Pool
import numpy as np
# from tqdm import tqdm

'''
execute 단위로 비동기 코루틴 생성
'''

class ConvertProcess():
    
    def __init__(self, config=None):
        '''
        config관련 정보 분리해야함
        config를 router에서 최초 한번 불러와서 전달하는게 좋을듯
        '''
        self.config = config        
        self.logger = Logger(name=f'log_{config["info"]["name"]}', filePath=config['log']['fullPath']) # logger self.__class__.__qualname__
    
        self.fileService = FileService() # 파일 매니저 클래스
        self.fileService.setLogger(self.logger) # 파이프라인 공통로거 삽입
        self.facebookAPI = FacebookAPI() # facebook api
        self.facebookAPI.setLogger(self.logger)
        self.properties = Properties()

    def execute(self, catalog_id:str=None, is_update:bool=False, is_upload:bool=False) :        
        self.logger.info(f'==Convert Execute Start {self.config["info"]["name"]} {catalog_id}==')

        # 1. 원본 ep 다운로드
        self.logger.info('[ 1.EP DOWNLOAD ]')
        response = self.fileService.getEpDownload(catalog_id=catalog_id, isUpdateEp=is_update)
        self.logger.info(response.get())
        
        # 2. chunk로 읽어 피드 수 만큼 균등하게 분리 (대용량피드 상품수 제한 대응)
        self.logger.info('[ 2.Feed Segmentation ]')        
        self.feed_segment(catalog_id, is_update)
        
        # 3. 멀티프로세스 처리 (분할된 피드별 중복제거 / 필터링 / 압축 / 업로드)
        self.logger.info('[ 3.Filtering / Zip / Upload ]')                
        feed_ids = list(self.config['catalog'][catalog_id]['feed'].keys())
        pool = Pool( min(1, len(feed_ids)) ) # 분할된 feed 갯수기준 최대 1개
        args = []
        for i, feed_id in enumerate(feed_ids):
            args.append((catalog_id, feed_id, is_update, is_upload)) # 매개변수 리스트        
        pool.starmap(self.feed_filtering_upload, args) # pool을 통해 실행
        pool.close()
        pool.join()

        self.logger.info(f'==Convert Execute End {self.config["info"]["name"]} {catalog_id}==')
    
    
    def feed_segment(self, catalog_id:str=None, is_update:bool=False):
        #
        feed_ids = list(self.config['catalog'][catalog_id]['feed'].keys())
        # 0~9 배열 균등분배 후 list출력
        index_map = list(map(str,range(10))) # ['0','1','2','3','4','5','6','7','8','9']
        segment_index_map = [list(data) for data in np.array_split(index_map, len(feed_ids))] # [['0','1'],['2','3'],['4','5'],['6','7'],['8','9']]        
        #
        update_suffix = '_update' if is_update == True else ''
        chunk_size = 500000
        file_path=self.config[f'ep{update_suffix}']['fullPath']
        seperator=self.config[f'ep{update_suffix}']['sep']
        encoding=self.config[f'ep{update_suffix}']['encoding']
        compression= 'infer' if self.config[f'ep{update_suffix}']['zipformat'] == '' else self.config[f'ep{update_suffix}']['zipformat']
        #
        columns = list(self.config['columns'].keys()) # 필요컬럼만         
        loaded_cnt=0        

        # chunk load        
        ep_load = pd.read_csv(file_path,
            nrows=None,
            chunksize=chunk_size,
            header=0, # header row
            dtype=str, # string type 인식                            
            # converters={'id': lambda x: print(x)},
            sep=seperator, # 명시
            # lineterminator='\r',
            compression=compression,
            error_bad_lines=False, # error skip
            usecols=columns, # chunk에도 컬럼명 표기
            iterator=True,
            encoding=encoding)
                
        # segmentation        
        for i, chunk_df in enumerate(ep_load): # chunk load
            # 피드갯수에 따라 ID 기준 세그먼트 분리하여 쓰기
            for j, feed_id in enumerate(feed_ids):
                segment_df = chunk_df[chunk_df['id'].str[-1:].isin(segment_index_map[j])] # id끝자리 j
                
                # write                
                feed_path_temp = self.config['catalog'][catalog_id]['feed'][feed_id][f'fullPath{update_suffix}']
                feed_path_temp = feed_path_temp.replace('.',  '_temp.') # 중복제거 전 임시

                # print(segmentDF['id'][:5])
                mode = 'w' if i==0 else 'a' # 피드별 파일쓰기
                self.feedWrite(mode=mode, feedPath=feed_path_temp, df=segment_df)
            
            # log
            loaded_cnt = loaded_cnt + chunk_df.shape[0]
            self.logger.info(f'..{format(loaded_cnt,",")} row segmented')

            # memory clean
            del[[chunk_df]]
            gc.collect()            

    
    
    def feed_filtering_upload(self, catalog_id:str, feed_id:str, is_update:bool=False, is_upload:bool=False):
        self.convertFilter = ConvertFilter(self.config, catalog_id, is_update) # 필터 클래스
        self.convertFilter.setLogger(self.logger)
        #
        update_suffix = '_update' if is_update == True else ''
        feed_path = self.config['catalog'][catalog_id]['feed'][feed_id][f'fullPath{update_suffix}'] # 중복제거 후 최종
        feed_path_temp = feed_path.replace('.',  '_temp.') # 중복제거 전 임시
        feed_public_path = self.config['catalog'][catalog_id]['feed'][feed_id][f'publicPath{update_suffix}'] # ftp공개 주소
        #                
        if '.tsv' in feed_path :
            seperator = '\t'
        elif '.csv' in feed_path :
            seperator = ','
        chunk_size = 500000
        columns = list(self.config['columns'].keys()) # 필요컬럼만

        # 중복제거 id 컬럼 mask 사용
        feed_ids = pd.read_csv(feed_path_temp, usecols=['id'], encoding='utf-8', sep=seperator, dtype=str) # dtype 명시            
        mask = ~feed_ids.duplicated(subset=['id'], keep='first') # id컬럼기준 masking 생성            
                        
        feed_load = pd.read_csv(feed_path_temp,
            nrows=None,
            chunksize=chunk_size,
            header=0, # header row
            dtype=str, # string type 인식                            
            # converters={'id': lambda x: print(x)},
            sep=seperator, # 명시
            # lineterminator='\r',
            # compression=compression,
            error_bad_lines=False, # error skip
            usecols=columns, # chunk에도 컬럼명 표기
            encoding='utf-8')
        
        for j, chunk_df in enumerate(feed_load) :
            # index for mask
            chunk_df.index = mask[j*chunk_size : j*chunk_size + len(chunk_df.index)]
            chunk_df = chunk_df.loc[True]
            
            # filter 
            chunk_df = self.convertFilter.run(chunk_df)

            # feed_all 쓰기 (첫번째 피드 첫번째 행 이후 이어쓰기) (머천센터등 필요)
            # mode = 'w' if (i==0 and j==0) else 'a'
            # self.feedWrite(mode=mode, feedPath=feedAllPath, df=chunkDF.loc[True]) # 제거
            
            # feed 쓰기 (피드별 새로쓰기)
            mode = 'w' if j==0 else 'a'
            self.feedWrite(mode=mode, feedPath=feed_path, df=chunk_df)

            # memory clean
            del[[chunk_df]]
            gc.collect()            
                
        
        # 압축 / tsv 제거 / 업로드
        if self.config['info']['media'] != 'criteo' : 
            self.fileService.zipped(feed_path, feed_path+".zip")
            # self.fileService.delete(feed_path)
            # self.fileService.delete(feed_path_temp)
        
        if is_upload and self.config['info']['media'] == 'facebook': # 운영서버 & facebook 피드인경우
            self.logger.info('[ 4.UPLOAD ]')
            self.facebookAPI.upload(feed_id=feed_id, feed_url=feed_public_path, isUpdateEp=is_update) # api 업로드
        

    '''
    # download -> chunk epLoad -> segment -> drop duplication-> filter -> feedWrite -> feedUpload
    # isUpdateEp : True인경우 ep저장 subfix, api upload시 플래그 전달
    # isUpload : True인경우 api upload 실행
    def execute_temp(self, catalog_id=None, isUpdateEp=False, isUpload=False):        
        self.logger.info(f'==Convert Execute Start {self.epName} {catalog_id}==')
        self.convertFilter = ConvertFilter(self.config, catalog_id, isUpdateEp) # 필터 클래스
        self.convertFilter.setLogger(self.logger)
        
        # [1. download]
        # self.logger.info('[ 1.EP DOWNLOAD ]')
        # try :
        #     responseModel = self.fileService.getEpDownload(catalog_id=catalog_id, isUpdateEp=isUpdateEp)
        #     self.logger.info(responseModel.get())
        # except Exception as e :            
        #     self.logger.info(str(e)) # 여기에서 exception이 제대로 찍혀야함

        # [2. convert]
        self.logger.info('[ 2.CONVERT - filtering ]')
        feedIdList = list(self.config['catalog'][catalog_id]['feed'].keys())

        # 0~9 배열 균등분배 후 list출력
        indexMap = list(map(str,range(10))) # ['0','1','2','3','4','5','6','7','8','9']
        segmentIndexMap = [list(data) for data in np.array_split(indexMap, len(feedIdList))] # [['0','1'],['2','3'],['4','5'],['6','7'],['8','9']]
        # print(segmentIndexMap)
        
        if isUpdateEp : 
            update_suffix = '_update'
        else:
            update_suffix = ''
                    
        
        epLoad = self.chunkLoad(
            chunkSize=500000,
            filePath=self.config[f'ep{update_suffix}']['fullPath'],
            seperator=self.config[f'ep{update_suffix}']['sep'],
            encoding=self.config[f'ep{update_suffix}']['encoding'],
            compression= 'infer' if self.config[f'ep{update_suffix}']['zipformat'] == '' else self.config[f'ep{update_suffix}']['zipformat']
        )
        
        ## convert 진행
        
        totalCount=0
        for i, chunkDF in enumerate(epLoad): # chunk load            
            # filter
            chunkDF = self.convertFilter.run(chunkDF)

            # 피드갯수에 따라 ID 기준 세그먼트 분리하여 쓰기
            for j, feed_id in enumerate(feedIdList):
                segmentDF = chunkDF[chunkDF['id'].str[-1:].isin(segmentIndexMap[j])] # id끝자리 j
                
                # write                
                feedPath_temp = self.config['catalog'][catalog_id]['feed'][feed_id][f'fullPath{update_suffix}']
                feedPath_temp = feedPath_temp.replace('.',  '_temp.') # 중복제거 전 임시

                # print(segmentDF['id'][:5])
                mode = 'w' if i==0 else 'a' # 피드별 파일쓰기
                self.feedWrite(mode=mode, feedPath=feedPath_temp, df=segmentDF)
            
            # log
            totalCount = totalCount + chunkDF.shape[0]
            self.logger.info(f'..{format(totalCount,",")} row processed')

            # memory clean
            del[[chunkDF]]
            gc.collect()
            # break        
        

        # [3. upload] 피드별로 읽어 중복제거 / 압축 / 백업 / 업로드
        self.logger.info('[ 3.CONVERT - drop_duplicate/zip ]')
                
        # pool 사용하여 동시처리
        pool = Pool( min(5, len(feedIdList)) ) # 분할된 feed 갯수기준 최대 5개
        args = []
        for i, feed_id in enumerate(feedIdList):
            args.append((catalog_id, feed_id, update_suffix, isUpdateEp, isUpload))
        
        pool.starmap(self.dropDuplicateFeed, args)
        pool.close()
        pool.join()

                                    
        # all파일 압축 / 제거
        # feedAllPath = self.config['catalog'][catalog_id]['feed_all'][f'fullPath{update_suffix}']
        # if self.config['info']['media'] != 'criteo' : # 크리테오는 압축안함
        #     self.fileService.zipped(feedAllPath, feedAllPath+".zip")
            # self.fileService.delete(feedAllPath)
            # pass
    
        self.logger.info(f'==Convert Execute End {self.epName} {catalog_id}==')
            

    # 피드별 중복제거
    def dropDuplicateFeed(self, catalog_id, feed_id, update_suffix, isUpdateEp, isUpload):        
        feedPath = self.config['catalog'][catalog_id]['feed'][feed_id][f'fullPath{update_suffix}'] # 중복제거 후 최종
        feedPath_temp = feedPath.replace('.',  '_temp.') # 중복제거 전 임시
        feedPublicPath = self.config['catalog'][catalog_id]['feed'][feed_id][f'publicPath{update_suffix}'] # ftp공개 주소
        if '.tsv' in feedPath :
            sep = '\t'
        elif '.csv' in feedPath :
            sep = ','

        # print(feedPath)
        # print(feedPath_temp)
        # print(feedPublicPath)

        # 중복제거
        feedIdDF = pd.read_csv(feedPath_temp, usecols=['id'], encoding='utf-8', sep=sep, dtype=str) # dtype 명시            
        mask = ~feedIdDF.duplicated(subset=['id'], keep='first') # id컬럼기준 masking 생성            
        
        chunkSize = 500000
        chunkIter = self.chunkLoad(chunkSize=chunkSize, filePath=feedPath_temp, seperator=sep, encoding='utf-8')
        for j, chunkDF in enumerate(chunkIter) :
            # index for mask
            chunkDF.index = mask[j*chunkSize : j*chunkSize + len(chunkDF.index)]                

            # feed_all 쓰기 (첫번째 피드 첫번째 행 이후 이어쓰기) (머천센터등 필요)
            # mode = 'w' if (i==0 and j==0) else 'a'
            # self.feedWrite(mode=mode, feedPath=feedAllPath, df=chunkDF.loc[True]) # 제거
            
            # feed 쓰기 (피드별 새로쓰기)
            mode = 'w' if j==0 else 'a'
            self.feedWrite(mode=mode, feedPath=feedPath, df=chunkDF.loc[True])

            # memory clean
            del[[chunkDF]]
            gc.collect()
        
        # memory clean
        del[[feedIdDF, mask]]
        gc.collect()
        
        # 압축 / tsv 제거 / 업로드
        if self.config['info']['media'] != 'criteo' : 
            self.fileService.zipped(feedPath, feedPath+".zip")            
            # self.fileService.delete(feedPath)
            # self.fileService.delete(feedPath_temp)
        
        if isUpload and self.config['info']['media'] == 'facebook': # 운영서버 & facebook 피드인경우
            self.logger.info('[ 4.UPLOAD ]')
            self.facebookAPI.upload(feed_id=feed_id, feed_url=feedPublicPath, isUpdateEp=isUpdateEp) # api 업로드
    '''


    # pixel데이터 다운로드 (to ep)
    def pixelDataDownLoad(self):
        pass
    
    '''
    # ep데이터 로드
    def chunkLoad(self, chunkSize=100000, filePath=None, seperator=None, encoding='utf-8', compression='infer') :
        
        # chunksize 단위로 로드
        # title에 구분자포함되어 에러나는경우 skip.. 원본ep 문제
        # 컬럼 정리를 위해 원본 컬럼 리스트를 세팅해 로드
        
        # 원본 컬럼리스트
        columns = list(self.config['columns'].keys()) # 필요컬럼만 
        # print(columns)

        result = pd.read_csv(filePath,
                            nrows=None,
                            chunksize=chunkSize, # 일단 10만
                            header=0, # header row
                            dtype=str, # string type 인식                            
                            # converters={'id': lambda x: print(x)},
                            sep=seperator, # 명시
                            # lineterminator='\r',
                            compression=compression,
                            error_bad_lines=False, # error skip
                            usecols=columns, # chunk에도 컬럼명 표기
                            encoding=encoding)
        return result
    '''

    #
    def feedWrite(self, mode='w', feedPath=None, df=None):
        os.makedirs(os.path.dirname(feedPath), exist_ok=True) # 경로확인/생성
        if mode == 'w': # 새로쓰기
            header=True
        elif mode == 'a': # 이어쓰기
            header=False

        if feedPath.endswith('.csv') :
            sep = ','
        elif feedPath.endswith('.tsv') :
            sep = '\t'

        df.to_csv(feedPath, 
                    index=False, # 자체 인덱스제거
                    mode=mode,
                    sep=sep,
                    header=header, # 컬럼명 
                    encoding='utf-8')