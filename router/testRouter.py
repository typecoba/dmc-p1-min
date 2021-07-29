from fastapi.routing import APIRouter
from repository.ConfigRepository import ConfigRepository
from common.FileService import FileService
from common.FacebookAPI import FacebookAPI
from common.Utils import Utils
from common.Properties import Properties
import requests
from common.ResponseModel import ResponseModel
import asyncio
import aiohttp
from multiprocessing import Process, Queue
from common.ConvertProcess import ConvertProcess
import pycron
from random import randrange

router = APIRouter()
configRepository = ConfigRepository()
fileService = FileService()
facebookAPI = FacebookAPI()
utils = Utils()
properties = Properties()

@router.get('/test/')
async def test_home():
    return ResponseModel(message='test router')

    
@router.get('/test/apiupload')
async def test_apiupload():
    feed_id = '236164118048821'
    # feed_url = 'http://api.dmcf1.com/feed/268046537186348/feed_268046537186348_2499714026735797.tsv.zip'
    feed_url = 'http://api.dmcf1.com/feed/141118536454632/watermark_141118536454632.json.gz'
    isUpdateEp = False
    facebookAPI.upload(feed_id, feed_url, isUpdateEp)
    return ResponseModel()

@router.get('/test/download')
async def test_download():
    fromPath = 'http://api.dmcf1.com/ep/ssg/ssg_facebookNoCkwhereEpAll.csv'
    toPath = 'C:/Users/shsun/Documents/workspace/project/p1/f1_feed_change_min/data/ep/ep_ssg_facebook.csv'
    print(toPath)
    fileService.download(fromPath, toPath)

@router.get('/test/loadProduct')
async def test_loadProduct():
    productRepository = ProductRepository()
    product = productRepository.selectProduct(catalog_id='865294747216107',period=365)
    print(len(product))

@router.get('/test/ping')
async def test_ping():    
    ip = Utils.getIP()
    return ResponseModel(message='ping ok', content=ip)

#### 코루틴 사용 for test
@router.get('/test/asyncfor')
async def test_asyncfor(): 
    print('start')
    # asyncfor_main()
    fts = [asyncio.ensure_future( asyncfor_do(i) ) for i in enumerate(range(5))]
    await asyncio.gather(*fts)
    print('end')

async def asyncfor_do(i=None):    
    await asyncio.sleep(1)
    print('do ')

#### 비동기 + 멀티프로세스 test
#### fastapi 자체 loop로 코루틴, workers 옵션으로 멀티프로세스 실행
@router.get('/test/multiprocessfor')
async def test_multiprocessfor():

    # 코루틴으로 for 돌려서 비동기 http접근
    url = 'http://localhost:8000/test/multifunc'        
    futures = [asyncio.ensure_future( test_fetch(f'{url}/{i}') ) for i in enumerate(range(20))]
    await asyncio.gather(*futures)

    ResponseModel(message='',content='multiple resquest')

async def test_fetch(url):    
    async with aiohttp.ClientSession() as session :
        async with session.get(url) as response:
            return await response.text()

@router.get('/test/multifunc/{i}')
async def test_multifunc(i=0):    
    print(f'start {i}')
    await asyncio.sleep(3)    
    print(f'end {i}')    
    # ResponseModel(message='', content='do')


# 멀티프로세스 test2
@router.get('/test/multiprocessfor2')
async def test_multiprocessFor2() :    
    for i in range(20) :                 
        Process(target=test_multifunc3, args=(i,)).start()        
        print(f'process_{i} start')
    return ResponseModel()


def test_multifunc3(num) :
    result = 0
    rand = randrange(10000000)
    if num < 10 :
        numstr = f'0{num}'
    else :
        numstr = str(num)
    
    for i, val in enumerate(range(rand)) : 
        result = result + i
    print(f'process {numstr} end : result = {result}')