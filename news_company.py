# 접속 및 파싱
import requests
import lxml.html
import cssselect
import collections

# 데이터 저장
# from ruri_data import Ruri_Data

# 딜레이
from time import sleep

# 시간측정
import time
class News_company: 
    def add_news_company(self, link, innerlink):
        if link == 'fillblanks':
            news_company = 'fillblanks'
        elif link != 'fillblanks':
            print('링크발견')
            news_company = []
            # 주요 언론사 사전
            news_dict = { '뉴스' : 'news',                     
                        '경향신문' : 'khan',
                        '국민일보' : 'kmib',
                        '동아일보' : 'news.dong',
                        '문화일보': 'munhwa',
                        '서울신문': 'seoul.co.kr',
                        '세계일보': 'segye',
                        '조선일보': 'chosun',
                        '중앙일보': 'joins',
                        '한겨레' : 'hani.co.kr',
                        '한국일보' : 'hankookilbo',
                        '뉴스1': 'news1.kr',
                        '뉴시스' : 'newsis',
                        '연합뉴스': 'yna.co.kr',
                        '연합뉴스TV' : 'yonhapnewstv',
                        '채널A' : 'ichannela',
                        '한국경제TV' : 'wowtv.co.kr',
                        'JTBC' : 'jtbc',
                        'SBS' : 'sbs',
                        'KBS' : 'kbs',
                        'MBC' : 'imnews',
                        'MBN' : 'mbn',
                        'TV조선' : 'tvchosun',
                        'YTN' : 'ytn',
                        '이데일리' : 'edaily',
                        '머니투데이' : 'mt.co.kr',
                        '오마이뉴스' : 'ohmynews',
                        '노컷뉴스' : 'nocutnews'
                        }

            news_dict_keys = list(news_dict.keys())
            news_dict_values = list(news_dict.values())                   
            headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36'}
            naver = 'news.naver' #
            daum= 'v.daum' # 그냥 daum으로 쓰면 cafe.daum에서 걸림
            
            for j in range(len(link)): # 리스트 형태 안의 링크의 갯수만큼 반복
                temp = 'Not News'
                for w in range(len(news_dict_keys)):
                    if news_dict_values[w] in link[j]:
                        temp = news_dict_keys[w]
                        
                if (daum in link[j]): # 뉴스링크(문자열)에 'daum' 키워드가 있다면.
                    res = requests.get(link[j], headers=headers) # 그 링크로 접속
                    html = res.text
                    root = lxml.html.fromstring(html)
                    print('daum 뉴스', link[j])

                    try:
                        selector = root.cssselect('div em a img')[0]
                        alt = selector.get('alt') # 뉴스언론사 이름 가져오기(예: '중앙일보', '연합뉴스', '한겨례' 형태로 가져옴)

                        if alt in news_dict_keys: #선정한 언론사 목록(key)에 alt값이 있다면(메이저 언론사)
                            temp = alt  # 언론사 목록에 이름 그대로 추가
                        else: # 선정한 언론사 목록에 alt값이 없다면(마이너 언론사)
                            temp = '기타 언론사'
                        print(temp)
                    except: 
                        temp = 'Selector Not Found'
                        print(temp)
                        
                if (naver in link[j]): # 이번엔 네이버
                    res = requests.get(link[j], headers=headers) 
                    html = res.text
                    root = lxml.html.fromstring(html)
                    print('naver 뉴스', link[j])
                    # 네이버는 모바일과 데스크톱의 선택자가 전혀 다르다..
                    # 주소에서 m.이 있을 시 모바일
                    try:
                        if 'm.news' in link[j]:
                            selector = root.cssselect('div a img')[0] # 모바일
                        else:
                            selector = root.cssselect('td div div a img')[0] # 데스크탑
                        alt = selector.get('alt') 
                        if alt in news_dict_keys: 
                            temp = alt  
                        else: 
                            temp = '기타 언론사'
                        print(temp)
                    except: # 스포츠 뉴스나 특별 뉴스는 선택자 형태가 또 다르다!...
                        temp = 'Selector Not Found'
                        print(temp)

                if temp == '뉴스': # news 라는 키워드때문에 '뉴스' 로 걸러지긴 했는데 언론사 리스트에 없다면 기타 언론사 
                    temp = '기타 언론사'


                news_company.append(temp)
            
            # # 모든 작업이 끝나고 다시 링크로 돌아가기 (그래야 다른 요소를 뽑는 것이 가능)
            # res = requests.get(innerlink, headers=headers) # 그 링크로 접속
            # html = res.text
            # root = lxml.html.fromstring(html)
            
        return news_company