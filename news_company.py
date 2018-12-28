# 접속 및 파싱

import requests
import lxml.html
import cssselect
import collections
import socket
from lxml import etree

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
                daum= 'daum' 
                
                for j in range(len(link)): # 리스트 형태 안의 링크의 갯수만큼 반복
                    temp = 'Not News' #기본값(아무것도 걸리지 않는 유튜브나 기타 사이트는 이걸로 전달)

                    for w in range(len(news_dict_keys)): # 언론사 사전 목록과 링크 비교
                        if news_dict_values[w] in link[j]:
                            temp = news_dict_keys[w] # 준비된 사전의 value와 일치하면 그 언론사
                            
                    if (daum in link[j]): # 뉴스링크(문자열)에 'daum' 키워드가 있다면.
                        try:
                            errorpass = False # 접속에러용 기본값
                            res = requests.get(link[j], headers=headers) # 그 링크로 접속
                            html = res.text
                            root = lxml.html.fromstring(html)

                        except ConnectionResetError as e: # 각종 접속 에러 잡기 에러 걸리면 errorpass = True되면서 finally에서 처리
                            errorpass = True
                            print('%s에서 에러 발생'% 'ConnectionResetError')
                            print('%s 오류 다음 페이지에서 재접속' % e)

                        except requests.ConnectionError as e:
                            errorpass = True
                            print('%s에서 에러 발생'% 'requests.ConnectionError')
                            print('%s 오류 다음 페이지에서 재접속' % e)

                        except requests.exceptions.ConnectionError as e:
                            errorpass = True
                            print('%s에서 에러 발생'% 'requests.exceptions.ConnectionError')
                            print('%s 오류 다음 페이지에서 재접속' % e)

                        except requests.exceptions.ChunkedEncodingError as e:
                            errorpass = True
                            print('%s에서 에러 발생'% 'requests.exceptions.ChunkedEncodingError')
                            print('%s 오류 다음 페이지에서 재접속' % e)
                                        

                        except etree.ParserError as e:
                            errorpass = True
                            print('%s 오류로 다음 페이지에서 재접속' % e)
                            
                        finally:
                            # 만일 에러가났다면,
                            if errorpass == True:
                                temp = 'Requests Error' # 언론사 목록을 Requests 에러로 돌린다 (url 문제일 확률이 제일 높음)
                            
                            else: # 에러가 없다면
                                try:
                                    selector = root.cssselect('div em a img')[0]
                                    alt = selector.get('alt') # 뉴스언론사 이름 가져오기(예: '중앙일보', '연합뉴스', '한겨례' 형태로 가져옴)
                                    if alt in news_dict_keys: #선정한 언론사 목록(key)에 alt값이 있다면(메이저 언론사)
                                        temp = alt  # 언론사 목록에 이름 그대로 추가
                                    else: # 선정한 언론사 목록에 alt값이 없다면(마이너 언론사)
                                        temp = '기타 언론사'

                                except IndexError: #네이버의 경우 스포츠 뉴스가 선택자가 달라서 잡히지 않는 경우 발생 그에 대비  
                                    temp = 'Selector Not Found'
  
                    if (naver in link[j]): # 이번엔 네이버
                        try:
                            errorpass = False # 접속에러용 기본값
                            res = requests.get(link[j], headers=headers) 
                            html = res.text
                            root = lxml.html.fromstring(html)
      
                        except ConnectionResetError as e: # 각종 접속 에러 잡기 에러 걸리면 errorpass = True되면서 finally에서 처리
                            errorpass = True
                            print('%s에서 에러 발생'% 'ConnectionResetError')
                            print('%s 오류 다음 페이지에서 재접속' % e)

                        except requests.ConnectionError as e:
                            errorpass = True
                            print('%s에서 에러 발생'% 'requests.ConnectionError')
                            print('%s 오류 다음 페이지에서 재접속' % e)

                        except requests.exceptions.ConnectionError as e:
                            errorpass = True
                            print('%s에서 에러 발생'% 'requests.exceptions.ConnectionError')
                            print('%s 오류 다음 페이지에서 재접속' % e)

                        except requests.exceptions.ChunkedEncodingError as e:
                            errorpass = True
                            print('%s에서 에러 발생'% 'requests.exceptions.ChunkedEncodingError')
                            print('%s 오류 다음 페이지에서 재접속' % e)
                                        

                        except etree.ParserError as e:
                            errorpass = True
                            print('%s 오류로 다음 페이지에서 재접속' % e)
                            # 내용이 비어 있다면 채우고 각 게시글의 내용, 링크, 댓글 등을 딕셔너리에 저장
                            # 해당 페이지의 정보를 모두 blank 채우고 다음페이지 호출
                            
                        finally:
                            # 만일 에러가났다면,
                            if errorpass == True:
                                temp = 'Requests Error' # 언론사 목록을 Requests 에러로 돌린다 (url 문제일 확률이 제일 높음)

                            else:
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
                                    
                                except IndexError: #네이버의 경우 스포츠 뉴스가 선택자가 달라서 잡히지 않는 경우 발생 그에 대비  
                                    temp = 'Selector Not Found'
                                    
                
                    if temp == '뉴스': # news 라는 키워드때문에 '뉴스' 로 걸러지긴 했는데 언론사 리스트에 없다면 기타 언론사 
                        temp = '기타 언론사'

                    print('링크 발견! %s %s' % (link[j], temp)) # 발견한 링크와 언론사 출력
                    news_company.append(temp)
     
        return news_company
