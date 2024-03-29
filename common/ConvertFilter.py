import pandas as pd
import common.Logger as Logger
from urllib import parse
import os

class ConvertFilter():

    def __init__(self, config=None, catalog_id=None, isUpdateEp=None):
        pd.options.mode.chained_assignment = None # pandas warning 관련 알림 제거
        self.config = config        
        self.catalog_id = catalog_id
        self.isUpdateEp = isUpdateEp
        self.result = pd.DataFrame()        
        self.logger = None

    def setLogger(self, logger=None):
        self.logger = logger


    # 실행함수
    def run(self, dataframe):
        # self.logger.info('Convert Filter : ' + str(self.config['filter']))
        self.result = dataframe # chunk data
        # 순서가 내용에 영향 줄 수 있음
        self.result = self.commonFilter(self.result) # 공통 filter
        self.result = self.mediaFilter(self.result) # 매체별 공통 filter               
        self.result = self.customFilter(self.result) # 카탈로그별 filter
        return self.result
    
    # 1. 공통 filter
    def commonFilter(self, dataframe=None):        
        dataframe.rename(columns=self.config['columns'], inplace=True) # key 수정            
        
        # 공백제거, result.apply(lambda x: x.str.strip(), axis=1) 로 돌리면 너무느림
        for key in list(dataframe.columns) :
            dataframe[key] = dataframe[key].apply(str).str.strip()
        # print(dataframe.columns)

        # include
        if 'include' in self.config['filter']:            
            for key,value in self.config['filter']['include'].items():
                dataframe = dataframe[dataframe[key].str.contains('|'.join(value), na=False)]
        # exclude
        if 'exclude' in self.config['filter']:            
            for key,value in self.config['filter']['exclude'].items() :                
                dataframe = dataframe[~dataframe[key].str.contains('|'.join(value), na=False)]
        # replace
        if 'replace' in self.config['filter']:
            for key,value in self.config['filter']['replace'].items() :                 
                before = list(value.keys())[0]
                after = list(value.values())[0]
                dataframe[key] = dataframe[key].str.replace(before, after, regex=True) # regex사용 replace
                
        return dataframe

    
    # 2. 매체별 피드사양 filter
    def mediaFilter(self, dataframe=None):
        
        # 공통        

        # google_product_category 값 변환
        dataframe['product_type'] = self.makeProductType(dataframe)

        # facebook
        # https://www.facebook.com/business/help/120325381656392?id=725943027795860
        if self.config['info']['media'] == 'facebook' :            
            dataframe['title'] = dataframe['title'].str[:150] # 필수

            if 'condition' not in dataframe : # 필수
                dataframe = dataframe.assign(condition='new')
            
            if 'availability' not in dataframe : # 필수
                dataframe = dataframe.assign(availability='in stock')                        

            if 'description' not in dataframe : # 필수
                dataframe = dataframe.assign(description=dataframe['title'].str.lower()) # 내용 없으면 title로 채움
            else : 
                dataframe = dataframe.assign(description=dataframe['description'].str.lower()) # 대문자로만 있으면 리젝되므로 소문자변환

        # google
        # https://support.google.com/merchants/answer/7052112?visit_id=637607880510744390-1548004461&rd=1
        if self.config['info']['media'] == 'google' :
            if 'availability' not in dataframe : # 필수
                dataframe = dataframe.assign(availability='in stock')

            if 'description' not in dataframe : # 필수
                dataframe = dataframe.assign(description=dataframe['title'].str.lower()) # 내용 없으면 title로 채움                

            # if 'condition' not in dataframe : # 필수 (새제품인경우 선택)
            #     dataframe = dataframe.assign(condition='new')

        # criteo
        # https://support.criteo.com/s/article?article=207571095-Criteo-Product-Feed-specification&language=ko
        if self.config['info']['media'] == 'criteo' :
            pass
            

                
        return dataframe



    # 3. 카탈로그별 filter
    def customFilter(self, dataframe=None):
        if ('filter' in self.config) == False :
            return dataframe
        
        ##### Facebook #####
        if self.config['info']['media'] == 'facebook' :

            # ssg
            if self.catalog_id == '268046537186348' : # ssg_eppe
                dataframe['link'] = 'https://ad.adpool.co.kr/app/ssg/item/' + dataframe['id']
            elif self.catalog_id == '225456985373646' : # ssg_eppe_ios
                dataframe['link'] = 'http://m.ssg.com/item/itemView.ssg?itemId=' + dataframe['id'] +'&gateYn=Y&mobilAppSvcNo=3'

            # hellonature                        
            elif self.catalog_id == '154972755345007': # hellonature_ba
                dataframe['link'] = 'https://www.hellonature.co.kr/fdp001.do?goTo=dpItemView&itemCd=' + dataframe['id'] + \
                                    '&utm_source=facebook&utm_medium=display_usertargeting_mo&utm_campaign=mo&utm_term=usertargeting_dynamic_2&adBridge=1&appinstall=2'
            elif self.catalog_id == '312961026783855': # hellonature_da
                dataframe['link'] = 'https://www.hellonature.co.kr/fdp001.do?goTo=dpItemView&itemCd=' + dataframe['id'] + \
                                    '&utm_source=facebook&utm_medium=display_retargeting_mo&utm_campaign=mo&utm_term=retargeting_dynamic_2&adBridge=1&appinstall=2'
            elif self.catalog_id == '2874651496189057': # hellonature_일반
                dataframe['link'] = 'https://www.hellonature.co.kr/fdp001.do?goTo=dpItemView&itemCd=' + dataframe['id']
            elif self.catalog_id == '1082436905600544': # hellonature_ba_aos
                dataframe['link'] = 'https://app.appsflyer.com/net.hellonature?pid=facebook_int&c=display_usertargeting_mo_aos_ba_1_210225&af_click_lookback=1d&is_retargeting=true&af_reengagement_window=1d&af_r=' + \
                                    'https%3A%2F%2Fgo.hellonature.co.kr%2F%3Fadlink%3Dhttps%3A%2F%2Fwww.hellonature.co.kr%2Ffdp001.do%3FgoTo%3DdpItemView%26itemCd%3D' + dataframe['id'] + \
                                    '%26utm_source%3Dfacebook%26utm_medium%3Ddisplay_usertargeting_mo%26utm_campaign%3Daos%26utm_term%3Dba_1_210225%26adBridge%3D1%26appinstall%3D1'
            elif self.catalog_id == '2364384170351822': # hellonature_re_aos
                dataframe['link'] = 'https://app.appsflyer.com/net.hellonature?pid=facebook_int&c=display_retargeting_mo_aos_dynamic_1_210225&af_click_lookback=1d&is_retargeting=true&af_reengagement_window=1d&af_r=' + \
                                    'https%3A%2F%2Fgo.hellonature.co.kr%2F%3Fadlink%3Dhttps%3A%2F%2Fwww.hellonature.co.kr%2Ffdp001.do%3FgoTo%3DdpItemView%26itemCd%3D' + dataframe['id'] + \
                                    '%26utm_source%3Dfacebook%26utm_medium%3Ddisplay_retargeting_mo%26utm_campaign%3Daos%26utm_term%3Ddynamic_1_210225%26adBridge%3D1%26appinstall%3D1'
            elif self.catalog_id == '276558370748503': # hellonature_ba_ios
                dataframe['link'] = 'https://app.appsflyer.com/id1098023186?pid=facebook_int&c=facebook_display_usertargeting_mo_ios_ba_1_210226&af_click_lookback=1d&idfa={aff_sub2}&af_lang={lang}&af_ua={useragent}&af_ip={ip}&is_retargeting=true&af_reengagement_window=1d&af_r=' + \
									'https%3A%2F%2Fgo.hellonature.co.kr%2F%3Fadlink%3Dhttps%3A%2F%2Fwww.hellonature.co.kr%2Ffdp001.do%3FgoTo%3DdpItemView%26itemCd%3D' + dataframe['id'] + \
									'%26utm_source%3Dfacebook%26utm_medium%3Ddisplay_retargeting_mo%26utm_campaign%3Dios%26utm_term%3Ddynamic_1_210226%26adBridge%3D1%26appinstall%3D1'
            elif self.catalog_id == '1428958314113899': # hellonature_re_ios
                dataframe['link'] = 'https://app.appsflyer.com/id1098023186?pid=facebook_int&c=facebook_display_retargeting_mo_ios_dynamic_1_210311&af_click_lookback=1d&idfa={aff_sub2}&af_lang={lang}&af_ua={useragent}&af_ip={ip}&is_retargeting=true&af_reengagement_window=1d&af_r=' + \
									'https%3A%2F%2Fgo.hellonature.co.kr%2F%3Fadlink%3Dhttps%3A%2F%2Fwww.hellonature.co.kr%2Ffdp001.do%3FgoTo%3DdpItemView%26itemCd%3D' + dataframe['id'] + \
									'%26utm_source%3Dfacebook%26utm_medium%3Ddisplay_retargeting_mo%26utm_campaign%3Dios%26utm_term%3Ddynamic_1_210311%26adBridge%3D1%26appinstall%3D1'

            # hmall
            # hmall 카테고리 제외건은 공통필터에서 적용 - config['filter']['exclude']['google_product_category']
            # 증분업데이트시 'out of stock'의 누락데이터 채우기 *일반적인 경우가 되면 분리필요
            if self.catalog_id in ['321875988705706', '517196555826417', '3089747424480784', '958772364735294', '275946927559143'] :
                if  self.isUpdateEp == True : # 증분업데이트
                    dataframe.loc[dataframe['availability']=='in stock', 'condition'] = 'new'
                    dataframe.loc[dataframe['availability']=='out of stock', ['title','description']] = 'undefined'

                if self.catalog_id == '321875988705706': # hmall 전체상품
                    # dataframe = dataframe[dataframe['custom_label_0']!='DP'] # 제외값 (21.07.27 제거)
                    dataframe['link'] = dataframe.apply(lambda x : # series에 quote 함수 써야해서 apply lambda로 돌림
                        'https://PC5tOwFSxk6rMl5hMJ6LPA.adtouch.adbrix.io/api/v1/click/nQjrNdGHjEu2SkLy7xJuVQ?deeplink_custom_path=' + \
                        parse.quote('hmallmobile://front/pda/smItemDetailR.do?pReferCode=s58&ItemCode=' + x['id'] + '&pTcCode=0000002823&utm_source=insta&utm_medium=cpm_da&utm_campaign=retargeting'),
                        axis=1
                    )
                elif self.catalog_id == '517196555826417': # hmall 방송상품
                    dataframe = dataframe[dataframe['custom_label_0']=='HSB'] # 방송상품
                    dataframe['link'] = dataframe.apply(lambda x :
                        'https://PC5tOwFSxk6rMl5hMJ6LPA.adtouch.adbrix.io/api/v1/click/ZRRzM7clGk6mIJ2RXzXOdA?deeplink_custom_path=' + \
                        parse.quote('hmallmobile://front/pda/smItemDetailR.do?pReferCode=s58&ItemCode=' + x['id'] + '&pTcCode=0000002823&utm_source=insta&utm_medium=cpm_da&utm_campaign=retargeting'),
                        axis=1
                    )
                elif self.catalog_id == '3089747424480784': # hmallBA
                    # dataframe = dataframe[dataframe['custom_label_0']=='HSB'] # 방송상품 (21.07.30 추가/ 21.08.31 제거)
                    dataframe['link'] = dataframe.apply(lambda x :
                        'https://PC5tOwFSxk6rMl5hMJ6LPA.adtouch.adbrix.io/api/v1/click/KqyqArugBkWPx4ZvVQTJdg?deeplink_custom_path=' + \
                        parse.quote('hmallmobile://front/pda/smItemDetailR.do?pReferCode=s58&ItemCode=' + x['id'] + '&pTcCode=0000002823&utm_source=insta&utm_medium=cpm_da&utm_campaign=retargeting'),
                        axis=1
                    )
                elif self.catalog_id == '958772364735294': # hmall 패션                    
                    dataframe['link'] = dataframe.apply(lambda x :
                        'https://PC5tOwFSxk6rMl5hMJ6LPA.adtouch.adbrix.io/api/v1/click/Q6IpUr0vy0mFIEtSRcgvlg?deeplink_custom_path=' + \
                        parse.quote('hmallmobile://front/pda/smItemDetailR.do?pReferCode=s58&ItemCode=' + x['id'] + '&pTcCode=0000002823&utm_source=insta&utm_medium=cpm_da&utm_campaign=retargeting'),
                        axis=1
                    )
                elif self.catalog_id == '275946927559143': # hmall 뷰티                    
                    dataframe['link'] = dataframe.apply(lambda x :
                        'https://PC5tOwFSxk6rMl5hMJ6LPA.adtouch.adbrix.io/api/v1/click/bGr60eYF6kysyCsnxS2r5g?deeplink_custom_path=' + \
                        parse.quote('hmallmobile://front/pda/smItemDetailR.do?pReferCode=s58&ItemCode=' + x['id'] + '&pTcCode=0000002823&utm_source=insta&utm_medium=cpm_da&utm_campaign=retargeting'),
                        axis=1
                    )

            # 미궁365
            if self.catalog_id == '805092590204028' : 
                dataframe['brand'] = dataframe['brand'].fillna('미궁365') # brand 빈값 채우기
                dataframe.loc[dataframe['condition']=='신상품', 'condition'] = 'new'



        if self.config['info']['media'] == 'google' :
            # hnsmall_update 증분처리만 간소화에서 진행 *카탈로그 관리자 조건과 동일 (2021.11.16)
            if self.catalog_id in ['137258367'] :
                dataframe['price'] = dataframe['price']+' KRW' # price
                dataframe['condition'] = 'new' # condition
                dataframe['availability'] = 'in stock' # availability
                dataframe['shipping'] = 'KR:::'+dataframe['delivery_charge']+'.00 KRW' # shipping
                utm = 'channel_code=21323&utm_source=Google_Shopping&utm_medium=SA&utm_campaign=Dynamic'
                dataframe['link'] = 'http://www.hnsmall.com/display/goods.do?goods_code=' + dataframe['id'] + '&' + utm
                dataframe['mobile_link'] = 'http://m.hnsmall.com/mktcpgn/goods/view/' + dataframe['id'] + '?' + utm
                dataframe['product_type'] = dataframe['product_type'].replace(regex=r'&gt;', value='@')
                dataframe['custom_label_0'] = '0' if str(dataframe['delivery_charge'])!='0' else '1' # custom_label_0
                dataframe['web_should_fallback'] = 'TRUE' # web_should_fallback
                    
            # ssg_aos
            if self.catalog_id == '449637976' :
                android_app_link = 'ssg://execute/page_open/self?url=http://m.ssg.com/item/itemView.ssg?itemId='+ dataframe['id'] +'&gateYn=Y&mobilAppSvcNo=3'                
                dataframe['display_ads_link'] = android_app_link
                dataframe['price'] = dataframe['price'] + ' KRW'
                dataframe['shipping'] = 'KR:::' +dataframe['shipping']+ ' KRW'
                # condition값 치환은 GMC를 활용하는것으로 함
            
            # ssg_emart
            if self.catalog_id == '503144101':
                custom_link = 'http://m.emart.ssg.com/item/itemView.ssg?itemId='+dataframe['id']+'&gateYn=Y&mobilAppSvcNo=1'
                dataframe['display_ads_link'] = custom_link
                dataframe['ads_redirect'] = custom_link
                dataframe['link'] = custom_link
                dataframe['mobile_link'] = custom_link
                dataframe['price'] = dataframe["price"] + ' KRW'
                dataframe['shipping'] = 'KR:::' +dataframe["shipping"]+ ' KRW'

            



        if self.config['info']['media'] == 'criteo' :
            # 미궁365
            if self.catalog_id in 'migung365' : 
                dataframe['brand'] = dataframe['brand'].fillna('미궁365') # brand 빈값 채우기
                dataframe.loc[dataframe['condition']=='신상품', 'condition'] = 'new'

        return dataframe



    def makeProductType(self, dataframe):
        result = pd.Series()
        if 'product_type' in dataframe:
            result = dataframe['product_type']

        if 'google_product_category' in dataframe :             
            # google_product_category 값 주는경우 변환
            # google_product_category 목록 - https://www.google.com/basepages/producttype/taxonomy-with-ids.ko-KR.txt
            # facebook_product_category 목록 - https://www.facebook.com/micro_site/url/?click_from_context_menu=true&country=KR&destination=https%3A%2F%2Fwww.facebook.com%2Fproducts%2Fcategories%2Fko_KR.txt&event_type=click&last_nav_impression_id=0zON316vXR7GH7KpP&max_percent_page_viewed=67&max_viewport_height_px=914&max_viewport_width_px=1782&orig_http_referrer=https%3A%2F%2Fwww.google.com%2F&orig_request_uri=https%3A%2F%2Fwww.facebook.com%2Fbusiness%2Fhelp%2F526764014610932&primary_cmsid=526764014610932&primary_content_locale=ko_KR&region=apac&scrolled=true&session_id=2cCNFUzUS1y24Kwhg&site=fb4b&extra_data[view_type]=v3_initial_view&extra_data[site_section]=help&extra_data[placement]=%2Fbusiness%2Fhelp%2F526764014610932
            proot = os.getcwd().replace('\\','/') #프로젝트 루트
            productDF = pd.read_csv(f'{proot}/data/taxonomy-with-ids.ko-KR.txt', sep='-', lineterminator='\n', skiprows=1, names=['num', 'category'], dtype=str, encoding='utf-8')
            productDict = dict(zip(productDF['num'].str.strip(), productDF['category'].str.strip())) # dict로 변환 [{'num':'category'}]            
            result = dataframe['google_product_category'].map(productDict) # 값 치환

        if 'category_0' in dataframe : # category_0123 형식으로 들어오는 경우
            # category_1~4 연결해서 입력
            for i in range(4) : # 0-3                
                if f'category_{i+1}' in dataframe :                    
                    if result.size == 0 : 
                        result = dataframe[f'category_{i+1}'].copy()                        
                    else :   
                        result = result.str.cat(dataframe[f'category_{i+1}'], sep='@', na_rep='') # @구분자로 연결                    
            result = result.replace(regex=r'@+$', value='') # 하위카테고리 없는경우 정리
            # print(result)
            
        return result    
        