# 접속 및 파싱
import requests
import lxml.html
# import lxml.etree
import cssselect
import collections
from news_company import News_company

# error = lxml.etree.ParseError()
# msg = error.msg
# if msg == "Document is empty"
#     pass

# 데이터 저장
# from ruri_data import Ruri_Data

# 딜레이
from time import sleep

# 시간측정
import time

class WebCrawler:
    # 링크정보를 꼬리만 가지고 있을 때, 모든 정보를 합침.
    def adjusthtml_pb_tail(self, part_html, head=""):
        
        if 'http' not in part_html.get('href'):
            full_html = head + part_html.get('href')
        else:
            full_html = part_html.get('href')
        return full_html
    
    # 페이지를 설정할 수 있게 옵션 선택
    def crawlingposts(self, lastpage, cvalues):
        ### 크롤링 시간측정 시작 ####
        start_time = time.time()
        keykeys = list(cvalues.keys())
        keyvalues = list(cvalues.values())
        
        # 언론사 수집을 위한 클래스 생성
        news = News_company()

        # 접속할 주소 및 기타 접속 정보
        url = keyvalues[0]
        
        headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36'}
        print('%s 에 접속합니다. : ' % url)


        ##############################################
        # 0. 준비 - 매 페이지의 정보를 저장할 리스트 준비
        
        ## upper page
        ruri_upper_page_list = []
        ### 값이 빈 upper page 리스트의 확인을 위해 모든 리스트를 하나의 list로 묶음
        for i in range(0,6):
            prelist = list()
            ruri_upper_page_list.append(prelist)

        ## lower page
        ruri_contents_part_list = [] #게시글, 게시글 내 링크, 댓글


        #################################
        # 1. upper page - 상단 페이지 실행
        # 모든 페이지에 접속하여 글번호, 제목, 제목링크, 추천 정보를 저장.
        for i in range(1, int(lastpage)+1):
            params = {'page': i}
            res = requests.get(url, headers=headers, params=params)
            html = res.text
            root = lxml.html.fromstring(html)
            
            sleep(0.5)
            
            # 0. 번호 - 특이사항 : cssselect를 이용할 때 :not(.클래스이름)을 사용하여 notice class 제거.
            # 1. 링크 - 특이사항 : 꼬리만 추출되는 경우 감안
            # 2. 제목 - 특이사항 : x
            # 3. 추천수 - 특이사항 : cssselect를 이용할 때 :not(.클래스이름)을 사용하여 notice class 제거.
            # 4. 비추수
            # 5. 날짜
            for j in range(0,6):                
                for part_html in root.cssselect(keyvalues[j+2]):
                    if j == 1:
                        ruri_upper_page_list[j].append(self.adjusthtml_pb_tail(part_html, keyvalues[1]))
                    else:
                        ruri_upper_page_list[j].append(part_html.text_content())
            #print('기본정보 수집중 : 현재페이지 %s & 소요시간 %s' % i, (round(time.time() - start_time,2)))
        print('It takes %s seconds completing the upper page crawling and the uploading' % (round(time.time() - start_time,2)))

        ###############################
        # 1-2. 빈 upper page 리스트 체크
        # 모든 리스트를 검사해서 빈 리스트가 있으면 이를 더미 값으로 채움.
        chk = sorted(ruri_upper_page_list) #빈 리스트가 모두 리스트의 앞 쪽으로 올 수 있게 정렬함 => 맨 뒤는 무조건 숫자가 있다는 뜻
        # 빈 리스트의 존재 확인 후 
        if [] in ruri_upper_page_list:
            # 있다면
            for i in range(0, len(ruri_upper_page_list)):
                # 리스트를 하나씩 검사해서
                if ruri_upper_page_list[i] == []:
                    # 빈 리스트에 채울 것
                    ruri_upper_page_list[i] = ['None']*len(chk[-1])

        print('총 수집한 링크 수 : ', len(ruri_upper_page_list[1]))


        #################################
        # 2. lower page - 하단 페이지 실행
        
        # 변수
        i = 1 #현재 진행사항을 파악하기 위한 변수 설정
        
        # 수집한 링크로 이동하여 게시글, 게시글 내 링크, 댓글 정보를 저장.
        for innerlink in ruri_upper_page_list[1]:
            print('크롤링 진행사항 :', i, ' / ', len(ruri_upper_page_list[1]))
            inner_res = requests.get(innerlink, headers=headers)
            inner_html = inner_res.text
            inner_root = lxml.html.fromstring(inner_html)

            ruri_content_dict = {}

            #게시물 & 내부링크
            # 6. 게시글
            # 7. 내부링크
            # 8. 댓글
            # 9. 추천수
            # 10. 비추수
            # 11. 날짜
            # 12.
            
            for j in range(6, 12):
                tmplist = list()
                tmpvalue = ''
                for part_html in inner_root.cssselect(keyvalues[j+2]):
                    if j+2 == 9:
                        # 특이사항 : a태그로 link를 불러왔으나, 그림파일 등 a 태크를 사용하는 경우 blank 저장
                        if part_html.get('href') is None:
                            continue
                        tmplist.append(part_html.get('href')) #내부링크

                    # 12.22 성목 추가
                    # 댓글은 여러개 있을 가능성이 많기 때문에 반드시 리스트로 저장(그래야 전처리 및 분석 쉬움)
                    elif j+2 == 10: 
                        tmplist.append(part_html.text_content())
                    else:
                        #게시글이나 날짜 등은 하나 밖에 없기 때문에 리스트가 아닌 일반 변수로 저장
                        if isinstance(part_html, list) == False:
                            tmpvalue = part_html.text_content()
                        else:
                            tmplist.append(part_html.text_content())
                    
                #각 게시글의 내용, 링크, 댓글을 딕셔너리에 저장
                if tmpvalue != '':
                    ruri_content_dict[keykeys[j+2]] = tmpvalue
                else:
                    ruri_content_dict[keykeys[j+2]] = tmplist

            content = list(ruri_content_dict.values())

            # 언론사 정보 가져오기 
            # add_news_company의 param은 글 내의 링크(content[1])와 그 글의 원본 주소(innerlink)
            # return 값으로 리스트를 받음
            news_company = news.add_news_company(content[1], innerlink)

            # 그 리스트를 lower page 사전 제일 마지막에 추가 
            ruri_content_dict['news_company'] = news_company
            
            #list에 모든 dictionary type 저장.
            ruri_contents_part_list.append(ruri_content_dict)
            i += 1

        ### 크롤링 시간측정 종료 ###
        print(" It takes %s seconds crawling these webpages" % (round(time.time() - start_time,2)))
        return (ruri_upper_page_list[0], ruri_upper_page_list[1], ruri_upper_page_list[2], 
        ruri_upper_page_list[3], ruri_upper_page_list[4],ruri_upper_page_list[5],ruri_contents_part_list)
