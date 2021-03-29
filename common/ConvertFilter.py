import pandas as pd
import common.Logger as Logger

class ConvertFilter():

    def __init__(self, catalog_id=None, config=None):
        self.config = config        
        self.catalog_id = catalog_id
        self.result = pd.DataFrame()
        # self.logger = Logger() # 기본로거
        self.logger = None

    def setLogger(self, logger=None):
        self.logger = logger        


    # 실행함수
    def run(self, dataframe):        
        # self.logger.info('Convert Filter : ' + str(self.config['filter']))
        self.result = dataframe # chunk data
        self.result = self.commonFilter(self.result) # 공통 filter                
        self.result = self.mediaFilter(self.result) # 매체별 공통 filter        
        self.result = self.customFilter(self.result) # 카탈로그별 filter                        
        return self.result

        
    def commonFilter(self, dataframe=None):

        # columns {'a':'b'}  'a'->'b'
        keys = list(self.config['columns'].keys())
        dataframe = dataframe[keys] # 필요컬럼만 추출        
        dataframe.rename(columns= self.config['columns'], inplace=False) # key 수정

        # 공백제거, result.apply(lambda x: x.str.strip(), axis=1) 로 돌리면 너무느림
        for key in keys :
            dataframe[key] = dataframe[key].str.strip()

        # include
        if 'include' in self.config['filter']:
            keys = self.config['filter']['include'].keys()
            for key in keys :
                value = self.config['filter']['include'][key]
                # dataframe = dataframe.loc[dataframe[key].isin(value)] #포함
                dataframe = dataframe[dataframe[key].str.contains('|'.join(value))]
        
        # exclude
        if 'exclude' in self.config['filter']:
            keys = self.config['filter']['exclude'].keys()
            for key in keys :
                value = self.config['filter']['exclude'][key]
                # dataframe = dataframe.loc[~dataframe[key].isin(value)] #제외                
                dataframe = dataframe[~dataframe[key].str.contains('|'.join(value))]

        # replace
        if 'replace' in self.config['filter']:
            for key, value in self.config['filter']['replace'].items() :                 
                before = list(value.keys())[0]
                after = list(value.values())[0]
                dataframe[key] = dataframe[key].str.replace(before, after, regex=True) # regex사용 replace
        
        return dataframe
        

    # 매체별 기본값 등    
    def mediaFilter(self, dataframe=None):
        if self.config['info']['media'] == 'facebook' :
            if 'availability' not in dataframe :
                dataframe['availability'] = 'in stock'
            # condition
            if 'condition' not in dataframe : 
                dataframe['condition'] = 'new'
            # description
            if 'description' not in dataframe :
                dataframe['description'] = dataframe['title'].str.lower() # 내용 없으면 title로 채움
            else : 
                dataframe['description'] = dataframe['description'].str.lower() # 대문자로만 있으면 리젝
        
        return dataframe


    # 특수한 개별로직인 경우 config 와 매칭시켜 개별관리
    # filter조건을 db활용해서 작성해야하는데...
    def customFilter(self, dataframe=None):
        if ('filter' in self.config) == False :
            return None        

        media = self.config['info']['media']        
        epName = self.config['info']['name']        
        
        ##### Facebook #####
        if media == 'facebook' :

            # SSG_EPPE / SSG_EPPE_IOS
            if epName == 'ssg_facebook' :                
                # link
                if self.catalog_id == '268046537186348' : # ssg_eppe
                    dataframe['link'] = 'https://ad.adpool.co.kr/app/ssg/item/' + dataframe['id']
                elif self.catalog_id == '225456985373646' : # ssg_eppe_ios
                    dataframe['link'] = 'http://m.ssg.com/item/itemView.ssg?itemId=' + dataframe['id'] +'&gateYn=Y&mobilAppSvcNo=3'
                

            elif epName == 'hellonature' :
                if self.catalog_id == '154972755345007': # hellonature_ba
                    dataframe['link'] = 'https://www.hellonature.co.kr/fdp001.do?goTo=dpItemView&itemCd=' + dataframe['id'] + \
                                        '&utm_source=facebook&utm_medium=display_usertargeting_mo&utm_campaign=mo&utm_term=usertargeting_dynamic_2&adBridge=1&appinstall=2'

                elif self.catalog_id == '312961026783855': # hellonature_da
                    dataframe['link'] = 'https://www.hellonature.co.kr/fdp001.do?goTo=dpItemView&itemCd=' + dataframe['id'] + \
                                        '&utm_source=facebook&utm_medium=display_retargeting_mo&utm_campaign=mo&utm_term=retargeting_dynamic_2&adBridge=1&appinstall=2'

                elif self.catalog_id == '2874651496189057': # hellonature_일반
                    dataframe['link'] = 'https://www.hellonature.co.kr/fdp001.do?goTo=dpItemView&itemCd=' + dataframe['id']

                elif self.catalog_id == '1082436905600544': # hellonature_ba_deep
                    dataframe['link'] = 'https://app.appsflyer.com/net.hellonature?pid=facebook_int&c=display_usertargeting_mo_aos_ba_1_210225&af_click_lookback=1d&is_retargeting=true&af_reengagement_window=1d&af_r=' + \
										'https%3A%2F%2Fgo.hellonature.co.kr%2F%3Fadlink%3Dhttps%3A%2F%2Fwww.hellonature.co.kr%2Ffdp001.do%3FgoTo%3DdpItemView%26itemCd%3D' + dataframe['id'] + \
                                        '%26utm_source%3Dfacebook%26utm_medium%3Ddisplay_usertargeting_mo%26utm_campaign%3Daos%26utm_term%3Dba_1_210225%26adBridge%3D1%26appinstall%3D1'
                                        
                elif self.catalog_id == '2364384170351822': # hellonature_re_deep
                    dataframe['link'] = 'https://app.appsflyer.com/net.hellonature?pid=facebook_int&c=display_retargeting_mo_aos_dynamic_1_210225&af_click_lookback=1d&is_retargeting=true&af_reengagement_window=1d&af_r=' + \
                                        'https%3A%2F%2Fgo.hellonature.co.kr%2F%3Fadlink%3Dhttps%3A%2F%2Fwww.hellonature.co.kr%2Ffdp001.do%3FgoTo%3DdpItemView%26itemCd%3D' + dataframe['id'] + \
                                        '%26utm_source%3Dfacebook%26utm_medium%3Ddisplay_retargeting_mo%26utm_campaign%3Daos%26utm_term%3Ddynamic_1_210225%26adBridge%3D1%26appinstall%3D1'
                        
            

        
        return dataframe
        