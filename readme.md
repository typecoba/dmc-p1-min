# 피드컨버트 간소화
## 프로젝트 내용
다이나믹 광고를 위한 상품정보 다운로드, 변환, 매체 업로드를 위한 모듈입니다. 기존 f1에서 쓰이던 피드컨버트 모듈의 복잡성을 줄이고 그간 발견된 문제점을 개선하여 요청사항을 유연하게 처리 가능하도록 설계 했습니다.

요청사항이 비교적 간단하며, 용량이 작은 ep데이터의 다운,변환,업로드,증분 업데이트를 목적으로 만들었으며 추후 몇가지 튜닝을 거쳐 최대 140GB ep 파일처리에도 쓰이고 있습니다.

---
## 프로젝트 구성(기술스택)
python3.8, fastapi, pandas, mongodb

---

## 배포방법
- 직접 배포하는 경우
    1. 181서버 /data/feedconvert_api/api
    2. git clone 혹은 파일 copy 후
    ```
    # 가상환경 생성
    python -m venv ./venv

    # 가상환경 실행(이하 가상환경 위에서 작동)
    cd /venv/Scripts
    source activate (linux)
    activate.bat (window)

    # 프로젝트 root에서 requirements 실행
    pip install -r requirements.txt

    # main.py 실행
    python main.py
    ```
- jenckins 사용    
    1. 팀 젠킨스 https://680b4a219919.ngrok.io/
    2. feedconvert_api 선택
    3. 빌드된 프로세스 중지
    4. build now 클릭
    5. console로 빌드되는 내용 확인    
    
---
## 기능설명
- /docs : api 도큐먼트
- /config/{catalog_id} : 카탈로그별 세팅내용 확인
- /ep/info/{catalog_id} : ep파일 정보 확인
- /ep/export/{catalog_id} : ep파일 local download
- /ep/export/{catalog_id}/update : 증분업데이트용 ep파일 local download
- /ep/download/{catalog_id} : ep파일 서버 다운로드
- /ep/convert2feed/{catalog_id} : 특정 카탈로그 피드컨버트 실행 (ep download + convert + update)
- /feed/info/{catalog_id}/{feed_id} : 생성된 특정 피드파일 정보확인
- /feed/export/{catalog_id}/{feed_id} : 생성된 특정 피드파일 local 다운로드
- /feed/upload/{catalog_id} : facebook api 통한 feed upload
- /feed/upload/{catalog_id}/update : facebook api 통한 증분업데이트 feed upload
- /schedule/conviertProcess : 
    config 세팅에 현제시간에 해당하는 카탈로그 convert2feed 실행
    181서버 crontab을 통해 10분마다 호출됨

---
## 사용법
- 카탈로그 추가
    1. mongodb config 추가
    2. /ep/doenload/{catalog_id} 다운로드확인
    3. /ep/convert2feed/{catalog_id} 피드컨버트 확인
    4. /feed/export{catalog_id}/{feed_id} 생성된 feed파일 확인

- config 최소구조
```
{        
    "info": {
        "media": "",
        "name": "", 
        "createdate": "",
        "moddate": "",
        "status": "",
        "remark": ""
    },    
    "ep": {
        "url": "",
        "format": "",
        "sep": "",
        "encoding": "",
        "cron": "",
        "moddate": "",
        "status": "",
        "fullPath": ""
    },
    "catalog": {
        "catalog_0": {
            "name": "",
            "moddate": "",
            "feed": {
                "feed_0": {}
            }
        }
    },
    "columns": {
        "id": "id"
    },
    "filter": {}
}
```
- config 설명
```
{        
    "info": {
        "media": "google",                       # *필수 매체명 google|facebook|criteo
        "name": "ssg_emart",                     # *필수 카탈로그
        "createdate": "",
        "moddate": "2021-11-22 10:22:08",
        "status": "",
        "remark": ""                             # 기타정보입력
    },
    "ep": {                                      # ep
        "url": "http://ep.txt",                  # *필수 ep url
        "format": "tsv",                         # *필수 파일포맷
        "sep": "\t",                             # *필수 구분자명시
        "encoding": "cp949",                     # *필수 인코딩
        "limit": 145000,                         # 최대row 강제
        "cron": "0 4 * * *",                     # *필수 작동시간 크론탭
        "moddate": "",                           # 수정일
        "status": "",                            # 상태값
        "fullPath": "/ep.tsv"                    # *생성됨 다운로드되는 경로
    },
    "ep_update": {                               # 증분 업데이트용 ep (필요시추가)
        "url": "http://ep_update.txt",           # *필수 ep url
        "format": "tsv",                         # *필수 파일포맷
        "sep": "\t",                             # *필수 구분자명시
        "encoding": "cp949",                     # *필수 인코딩
        "cron": "0 8,12,19 * * *",               # *필수 작동시간 크론탭
        "moddate": "",                           # 수정일
        "status": "",                            # 상태값
        "fullPath": "/ep_update.tsv"             # *생성됨 다운로드되는 경로
    },
    "catalog": {
        "503144101": {                           # *필수 카탈로그id를 key로 세팅
            "name": "emart",                     # 카탈로그 이름
            "moddate": "",                       # 수저일
            "feed": {
                "feed_0": {                      # *필수 feed_id 혹은 이름을 key로 세팅
                    "fullPath": "",              # *생성됨 생성된 피드 경로 
                    "publicPath": "",            # *생성됨 생성된 피드 공개 접근경로
                    "fullPath_update": "",       # *생성됨 생성된 증분업데이트 피드 경로
                    "publicPath_update": ""      # *생성됨 생성된 증분업데이트 피드 공개 접근경로
                }
            }
        }
    },
    "columns": {                                 # *필수 컬럼 세팅 ep -> feed
        "id": "id",                              # {ep에서 사용될컬럼} : {feed에 표기될 컬럼}
        "title": "title",
        "link": "link",
        "mobile_link": "mobile_link",
        "image_link": "image_link",
        "price": "price",
        "shipping": "shipping",
        "condition": "condition",
        "brand": "brand",
        "category_name1": "category_1",
        "category_name2": "category_2",
        "category_name3": "category_3",
        "category_name4": "category_4"
    },
    "filter": {                                  # 필터 include(포함), exclude(제거) 지원
        "exclude" : {
            "id" : [                             # 'id' 값중 ["2108367627","2108367793","2109728074"] 제거
                "2108367627", 
                "2108367793", 
                "2109728074"                
            ],
            "product_type" : [
                "783", 
                "839", 
                "784"                
            ]
        }
    }
}
```

- log 확인    
    - 월별로그 : /data/feedconvert_api/logs/
    - 컨버트로그 : /data/feedconvert_api/logs/convert_logs/

---
## 향후 업데이트
1. config validation
2. test case 작성
