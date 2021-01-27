import pandas as pd


class ConvertFilter():

    def __init__(self,catalogConfig):
        self.catalogConfig = catalogConfig        
        self.result = pd.DataFrame()


    # 실행함수
    def run(self, dataframe):        
        # 공통 filter
        self.commonFilter(dataframe)
        
        # 매체별 공통 filter
        self.mediaFilter()

        # 개별 filter
        self.customFilter()
                        
        return self.result

        
    def commonFilter(self, dataframe):
        # field {'a':'b'}  ep 'a' to feed 'b'
        
        keys = list(self.catalogConfig['field'].keys())
        self.result = dataframe[keys] # 필요컬럼만 추출                
        self.result.rename(columns= self.catalogConfig['field'], inplace=True) # key 수정    

        if ('custom' in self.catalogConfig) == False : 
            pass

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
        media = self.catalogConfig['info']['media']
        catalog_id = self.catalogConfig['info']['catalog_id']
        name = self.catalogConfig['info']['name']

        if media == 'facebook' :
            if name == 'ssg_ep_test' and catalog_id == '268046537186348' :
                # link
                self.result['link'] = 'https://ad.adpool.co.kr/app/ssg/item/'+str(self.result['id'])

            elif name == 'hellonature' and catalog_id == '154972755345007':
                pass
        