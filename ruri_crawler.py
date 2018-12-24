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

    # 빈 페이지 검사용 함수
    def cr_pagesinspector(self, dump):
        # 모든 변수 및 리스트를 검사해서 비어 있으면 이를 더미 값으로 채움.
        
        #변수
        chkDict = {}
        chktype = None
        elements = None # 얼마나 채웠는지를 나타내는 수

        if isinstance(dump, list) != True:
            # list를 제외한 모든 변수에 fillblanks를 넣어줌 (None으로 넘어오는 값 포함)
            if dump == None or dump == '':
                dump = 'fillblanks'
                chktype = type(dump)
                elements = 1

        else:    
            # 빈 리스트의 존재 확인 후 
            chk = sorted(dump) #빈 리스트가 모두 리스트의 앞 쪽으로 올 수 있게 정렬함 => 맨 뒤는 무조건 숫자가 있다는 뜻
            
            if [] in dump:
                # 있다면
                for i in range(0, len(dump)):
                    # 리스트를 하나씩 검사해서
                    if dump[i] == []:
                        # 빈 것이 아닌 리스트에 채워진 요소 수만큼 빈 리스트에 채울 것
                        dump[i] = ['fillblanks']*len(chk[-1])
            
                chktype = type(dump)
            elements = len(chk[-1])
        
        # if chktype != None:
            # print('%s으로 빈 자료를 채움' % chktype)
        chkDict = {'number': elements, 'dump' : dump}
        return chkDict
    
    # 상단 페이지의 정보 크롤링
    def cr_upperpages(self, url, headers, lastpage, keyvalues, start_time, startini = 0, endini = 6):
        # 0. 준비 - 매 페이지의 정보를 저장할 리스트 준비
        upper_page_list = []
        ### 값이 빈 upper page 리스트의 확인을 위해 모든 리스트를 하나의 list로 묶음
        for i in range(startini, endini):
            prelist = list()
            upper_page_list.append(prelist)

        #################################
        # 1. upper page - 상단 페이지 실행
        # 링크에 접속하여 아래의 정보를 저장.
        
        # 0. 번호 - 특이사항 : cssselect를 이용할 때 :not(.클래스이름)을 사용하여 notice class 제거.
        # 1. 링크 - 특이사항 : 꼬리만 추출되는 경우 감안
        # 2. 제목 - 특이사항 : x
        # 3. 추천수 - 특이사항 : cssselect를 이용할 때 :not(.클래스이름)을 사용하여 notice class 제거.
        # 4. 비추수
        # 5. 날짜

        ##### 크롤링
        for i in range(1, int(lastpage)+1):
            #변수
            params = {'page': i} #페이지 이동을 위한 파라미터
            
            #접속
            res = requests.get(url, headers=headers, params=params)
            html = res.text
            root = lxml.html.fromstring(html)
            
            sleep(0.3)
            
            for j in range(startini, endini):                
                for part_html in root.cssselect(keyvalues[j+2]):
                    if j == 1:
                        upper_page_list[j].append(self.adjusthtml_pb_tail(part_html, keyvalues[1]))
                    else:
                        upper_page_list[j].append(part_html.text_content())

            print('기본정보 수집중 : 현재페이지 %s , 소요시간 %s 초' % (i, (round(time.time() - start_time,2))))
        
        ##### 크롤링 검사 => 빈 칸은 fillblinks를 채움
        list_completed_chk = self.cr_pagesinspector(upper_page_list).values()
        
        print('총 수집한 링크 수 : ', list(list_completed_chk)[0]) #정보

        return list(list_completed_chk)[1]
    
    # 하단 페이지의 정보 크롤링
    def cr_lowerpages(self, headers, upper_page_list, keykeys, keyvalues, startini=6, endini=12):
        # lower page - 하단 페이지 실행
        # 수집한 링크에 접속하여 아래의 정보를 저장.

        # 6. 게시글
        # 7. 내부링크
        # 8. 댓글
        # 9. 추천수
        # 10. 비추수
        # 11. 날짜
        # 12.

        # 변수
        i = 1                           #현재 진행사항을 파악하기 위한 변수 설정
        contents_part_list = []         #컨텐츠용 변수
        #news = News_company()           # 언론사 수집을 위한 클래스 생성

        for innerlink in upper_page_list[1]:
            print('크롤링 진행사항 :', i, ' / ', len(upper_page_list[1]))
            
            # 변수
            content_dict = {}
            
            # 접속과 크롤링
            inner_res = requests.get(innerlink, headers=headers)
            inner_html = inner_res.text
            inner_root = lxml.html.fromstring(inner_html)

            sleep(0.3)

            # ini 파일에 등록한 내용중 lower page에 해당하는 내용 크롤링
            for j in range(startini, endini):
                tmpvalue = None # 리턴할 변수를 하나로 줄이기 위해 None으로 선언
                tmpstr = ''
                tmplist = []
                for part_html in inner_root.cssselect(keyvalues[j+2]):
                    #print(j+2, part_html.text_content(), isinstance(part_html, list))
                    if j+2 == 9:
                        # 특이사항 : a태그로 link를 불러왔으나, 그림파일 등 a 태크를 사용하는 경우 blank 저장
                        if part_html.get('href') is None:
                            continue
                        tmplist.append(part_html.get('href')) #내부링크
                    else:
                        #게시글이나 날짜 등은 게시물 내에서 하나 밖에 없기 때문에 리스트가 아닌 일반 변수로 저장
                        if j+2 == 10: 
                            tmplist.append(part_html.text_content())

                        elif isinstance(part_html, list) == False:
                            tmpstr = part_html.text_content()
                        # 12.22 성목 추가
                        # 댓글은 여러개 있을 가능성이 많기 때문에 반드시 리스트로 저장(그래야 전처리 및 분석 쉬움)
                        
                            
                        else:
                            tmplist.append(part_html.text_content())
                
                # tmpvalue가 None일 때 str이 0이되면 리스트가 된다
                    if len(tmpstr) > 0:
                        tmpvalue = tmpstr
                    elif len(tmplist) > 0:
                        tmpvalue = tmplist

                #내용이 비어 있다면 채우고 각 게시글의 내용, 링크, 댓글 등을 딕셔너리에 저장
                Dict_completed_chk = self.cr_pagesinspector(tmpvalue).values()
                content_dict[keykeys[j+2]] = list(Dict_completed_chk)[1]
                
                # print('빈 셀을 채운 개수 : ', list(Dict_completed_chk)[0])

            # 이전 코드(크롤링 하나 할 때 같이 함)
            # content = list(content_dict.values())

            # news_company = news.add_news_company(content[1], innerlink)
            # content_dict['news_company'] = news_company
            
            #list에 모든 dictionary type 저장.
            contents_part_list.append(content_dict)
            i += 1
            
        return contents_part_list

    # 페이지를 설정할 수 있게 옵션 선택
    def crawlingposts(self, lastpage, cvalues):
        ### 크롤링 시간측정 시작 ####
        start_time = time.time()
        
        ### 변수설정
        keykeys = list(cvalues.keys())
        keyvalues = list(cvalues.values())
        url = keyvalues[0] # 접속할 주소 및 기타 접속 정보
        news = News_company() # 언론사 수집을 위한 인스턴스 생성        
        headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36'}
        print('%s 에 접속합니다. : ' % url)

        #################################
        # 1. upper page - 상단 페이지 실행
        upper_page_list = self.cr_upperpages(url, headers, lastpage, keyvalues, start_time)
        print('It takes %s seconds completing the upper page crawling and the uploading' % (round(time.time() - start_time,2)))
        #print(upper_page_list)
        #################################
        # 2. lower page - 하단 페이지 실행
        contents_part_list = self.cr_lowerpages(headers, upper_page_list, keykeys, keyvalues)
        
        #################################
        # 3. 언론사 정보 가져오기 => contents_part_list를 호출하여 다시 contents_part_list를 return
        #print(contents_part_list)
        
        # 모든 크롤링이 끝나고 contents_part_list에 news_company 추가

        print('News Company Analyzing...')
        for i in range(len(contents_part_list)):
            board_link = upper_page_list[1][i] # 게시물 자체 링크(혹시나 그 링크로 다시 돌아가야 될 때 대비(뉴스 컴퍼니 함수에선 현재 비활성화))
            #print(board_link)
            links_in_content = contents_part_list[i]['clinks'] # 게시물 내에 
            #print(links_in_content)
            news_company = news.add_news_company(links_in_content, board_link)
            contents_part_list[i]['news_company'] = news_company
            #print(contents_part_list[i])


        # 이전 코드
        # add_news_company의 param은 글 내의 링크(content[1])와 그 글의 원본 주소(innerlink)
        # return 값으로 리스트를 받음
        # news_company = news.add_news_company(content[1], innerlink)

        # # 그 리스트를 lower page 사전 제일 마지막에 추가 
        # ruri_content_dict['news_company'] = news_company
        
        # #list에 모든 dictionary type 저장.
        # ruri_contents_part_list.append(ruri_content_dict)

        ### 크롤링 시간측정 종료 ###
        print(" It takes %s seconds crawling these webpages" % (round(time.time() - start_time,2)))
        return (upper_page_list, contents_part_list)
    