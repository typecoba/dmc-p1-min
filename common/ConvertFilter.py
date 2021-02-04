import pandas as pd


class ConvertFilter():

    def __init__(self,catalogConfig):
        self.catalogConfig = catalogConfig        
        self.result = pd.DataFrame()


    # 실행함수
    def run(self, dataframe):        
        self.result = dataframe

        self.commonFilter() # 공통 filter                
        self.mediaFilter() # 매체별 공통 filter        
        self.customFilter() # 카탈로그별 filter
                        
        return self.result

        
    def commonFilter(self):
        # columns {'a':'b'}  'a'->'b'
        keys = list(self.catalogConfig['columns'].keys())
        self.result = self.result[keys] # 필요컬럼만 추출                
        self.result.rename(columns= self.catalogConfig['columns'], inplace=False) # key 수정

        # 공백제거, result.apply(lambda x: x.str.strip(), axis=1) 로 돌리면 너무느림
        for key in keys :
            self.result[key] = self.result[key].str.strip()

        # include
        if 'include' in self.catalogConfig['custom']:
            keys = self.catalogConfig['custom']['include'].keys()
            for key in keys :
                value = self.catalogConfig['custom']['include'][key]
                self.result = self.result.loc[self.result[key].isin(value)] #포함
        
        # exclude
        if 'exclude' in self.catalogConfig['custom']:
            keys = self.catalogConfig['custom']['exclude'].keys()
            for key in keys :
                value = self.catalogConfig['custom']['exclude'][key]
                self.result = self.result.loc[~self.result[key].isin(value)] #제외

        # replace
        if 'replace' in self.catalogConfig['custom']:
            keys = self.catalogConfig['custom']['replace'].keys()
            for key in keys : 
                value = self.catalogConfig['custom']['replace'][key]
                before = list(value.keys())[0]
                after = list(value.values())[0]                
                self.result[key] = self.result[key].str.replace(before, after, regex=True) # regex사용 replace
        

    # 매체별 기본값 등    
    def mediaFilter(self):
        if self.catalogConfig['info']['media'] == 'facebook' :            
            if 'availability' not in self.result :
                self.result['availability'] = 'in stock'
            # condition
            if 'condition' not in self.result : 
                self.result['condition'] = 'new'
            # description
            if 'description' not in self.result :
                self.result['description'] = self.result['title']


    # 특수한 개별로직인 경우 catalogConfig 와 매칭시켜 개별관리
    def customFilter(self):        
        if ('custom' in self.catalogConfig) == False :             
            return None
                
        media = self.catalogConfig['info']['media']
        catalog_id = self.catalogConfig['info']['catalog_id']
        name = self.catalogConfig['info']['name']

        if media == 'facebook' :
            if name == 'ssg_ep_test' and catalog_id == '268046537186348' :
                # link
                self.result['link'] = 'https://ad.adpool.co.kr/app/ssg/item/' + self.result['id']                

            elif name == 'hellonature' and catalog_id == '154972755345007':
                pass
        