# ------------셀레니움을 쓰는 크롤러 ---------------------
# 가급적 ruri_crawler.py의 형식을 해치지 않고 할 생각
#

# 접속 및 파싱
import requests
import lxml.html
from lxml import etree
import cssselect
import collections
# 딜레이
from time import sleep
# 시간측정
import time
# 셀레니움
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
# os 확인용
import platform

import pickle
import os

# 데이터 가져오기
from ruri_dao import CrwalingDAO
from get_half_element import getElement

#언론사 구분
from news_company import News_company

class selenium_WebCrawler:
    # 링크정보를 꼬리만 가지고 있을 때, 모든 정보를 합침.
    def adjusthtml_pb_tail(self, part_html, head=""):

        if 'http' not in part_html.get('href'):
            full_html = head + part_html.get('href')
        else:
            full_html = part_html.get('href')
        return full_html

    # 빈 페이지 검사용 함수
    def cr_pagesinspector(self, udump, ehapped=False):
        # 모든 변수 및 리스트를 검사해서 비어 있으면 이를 더미 값으로 채움.

        # 변수
        chkDict = {}
        # chktype = None
        dump = udump
        elements = None  # 얼마나 채웠는지를 나타내는 수

        # 에러가 발생하면,
        if ehapped == True:
            elements = 0
            dump = 'errorpassed'
        # 에러가 발생하지 않으면,
        else:
            # 넘어온 값이 리스트가 아닐 때
            if isinstance(dump, list) != True:
                # fillblanks를 넣어줌 (None으로 넘어오는 값 포함)
                if dump == None or dump == '':
                    dump = 'fillblanks'
                    chktype = type(dump)
                    elements = 1
            # 리스트일 때,
            else:
                # 빈 리스트의 존재 확인 후
                chk = sorted(dump)  # 빈 리스트가 모두 리스트의 앞 쪽으로 올 수 있게 정렬함 => 맨 뒤는 무조건 숫자가 있다는 뜻

                if [] in dump:
                    # 있다면
                    for i in range(0, len(dump)):
                        # 리스트를 하나씩 검사해서
                        if dump[i] == []:
                            # 빈 것이 아닌 리스트에 채워진 요소 수만큼 빈 리스트에 채울 것
                            dump[i] = ['fillblanks'] * len(chk[-1])

                    chktype = type(dump)
                elements = len(chk[-1])

            # if chktype != None:
            # print('%s으로 빈 자료를 채움' % chktype)
        chkDict = {'number': elements, 'dump': dump}
        return chkDict

    # 상단 페이지의 정보 크롤링
    def cr_upperpages(self, url, headers, lastpage, keyvalues, start_time, pagetype, startini=0,
                      endini=6):  # 페이지 parar 이름이 다른 사이트가 있어서 일단 추가
        # 0. 준비 - 매 페이지의 정보를 저장할 리스트 준비
        upper_page_list = []
        currentPath = os.path.relpath(os.path.dirname(__file__))
        ### 값이 빈 upper page 리스트의 확인을 위해 모든 리스트를 하나의 list로 묶음
        # for i in range(startini, endini):
        #     prelist = list()
        #     upper_page_list.append(prelist)

        # 0. 번호 - 특이사항 : cssselect를 이용할 때 :not(.클래스이름)을 사용하여 notice class 제거.
        # 1. 링크 - 특이사항 : 꼬리만 추출되는 경우 감안
        # 2. 제목 - 특이사항 : x
        # 3. 추천수 - 특이사항 : cssselect를 이용할 때 :not(.클래스이름)을 사용하여 notice class 제거.
        # 4. 비추수
        # 5. 날짜

        ##### 크롤링
        # for i in range(1, int(lastpage) + 1):
        upper_page_list_2 = []
        for ini in range(startini, endini):
            prelist = list()
            upper_page_list_2.append(prelist)

        # 변수
        params = {pagetype: lastpage}  # 페이지 이동을 위한 파라미터

        # 접속
        res = requests.get(url, headers=headers, params=params)
        html = res.text
        root = lxml.html.fromstring(html)

        sleep(0.1)

        for j in range(startini, endini):
            for part_html in root.cssselect(keyvalues[j + 2]):
                if j == 1:
                    upper_page_list_2[j].append(self.adjusthtml_pb_tail(part_html, keyvalues[1]))
                else:
                    upper_page_list_2[j].append(part_html.text_content())

        print('기본정보 수집중 : 현재페이지 %s , 소요시간 %s 초' % (lastpage, (round(time.time() - start_time, 2))))
        
        list_completed_chk = self.cr_pagesinspector(upper_page_list_2).values()
        #print(list(list_completed_chk)[1])
        #print('-------------------------------------------------')
        with open(currentPath + 'dump.pickle', 'wb') as f:
            pickle.dump(list(list_completed_chk)[1], f)

        print('저장완료')    
        # ##### 크롤링 검사 => 빈 칸은 fillblinks를 채움
        # list_completed_chk = self.cr_pagesinspector(upper_page_list).values()

        # print('총 수집한 링크 수 : ', list(list_completed_chk)[0])  # 정보

        #return list(list_completed_chk)[1]

    # 하단 페이지의 정보 크롤링

    def cr_lowerpages(self, board_links, keykeys, keyvalues, startini=6, endini=12):
        # lower page - 하단 페이지 실행
        # 수집한 링크에 접속하여 아래의 정보를 저장.

        # *SELENIUM PART
        # OS가 윈도우라면 크롬 웹드라이버 실행
        # OS가 리눅스라면 파이어폭스 게코 드라이버 실행(리눅스에서 크롬은 문제가 좀..)
        host = ""  # linux구분
        if platform.system() != "Linux":
            options = webdriver.ChromeOptions()
            # options.add_argument('headless')
            options.set_headless(True)
            wd = webdriver.Chrome(executable_path=r'C:\Program Files (x86)\Google\Chrome\Application\chromedriver.exe',
                                  options=options)

        else:
            firefox_profile = webdriver.FirefoxProfile()
            firefox_profile.set_preference('permissions.default.image', 2)
            firefox_profile.set_preference('dom.ipc.plugins.enabled.libflashplayer.so', 'false')
            options = Options()
            options.set_headless(True)  # newer webdriver versions
            wd = webdriver.Firefox(options=options, executable_path=r'/home/centos/selenium_test/geckodriver', firefox_profile = firefox_profile)
            print("Headless Firefox Initialized")


        # 6. 게시글
        # 7. 내부링크
        # 8. 댓글
        # 9. 추천수
        # 10. 비추수
        # 11. 날짜
        # 12.

        # 변수
        count_cr = 1  # 현재 진행사항을 파악하기 위한 변수 설정
        contents_part_list = []  # 컨텐츠용 변수
        news = News_company()  # 언론사 수집을 위한 클래스 생성
        scnt = 0
        
        # 수집한 내부링크(게시판)의 수만큼 loop를 돌며 접속
        for innerlink in board_links:
            oksign = True
            print('크롤링 진행사항 :', count_cr, ' / ', len(board_links))
            
            while oksign == True:  
                # 변수
                errorpass = False  # 재접속 확인
                content_dict = {}
                try:
                    
                    wd.set_page_load_timeout(30)
                    wd.get(innerlink)
                    
                    sleep(0.05)

                    # ini 파일에 입력한 CSS tag중 lower page에 해당하는 행 번호를 가져와
                    for j in range(startini, endini):
                        tmpvalue = None  # 리턴할 변수를 하나로 줄이기 위해 None으로 선언
                        tmpstr = ''
                        tmplist = []
                        # 해당 행(예를 들어 댓글)에 따른 번호를 넣어준다.
                        for part_html in wd.find_elements_by_css_selector(keyvalues[j + 2]):
                            if j + 2 == 9:
                                # 특이사항 : a태그로 link를 불러왔으나, 그림파일 등 a 태크를 사용하는 경우 blank 저장
                                if part_html.get_attribute('href') is None:
                                    continue
                                tmplist.append(part_html.get_attribute('href'))  # 내부링크

                            else:
                                # 12.24 성목 수정
                                # 모든 part_html은 값이 여러개라도 개별적으로 넘어오기 때문에
                                # isinstance list가 False일 수 밖에 없음
                                # 그래서 댓글이 리스트로 저장 안되는 상황 발생해서 수정
                                # 현재는 순서를 바꿔놓았는데 판단해서 isinstance는 지워도 될 듯 함
                                if j + 2 == 10:
                                    tmplist.append(part_html.text.strip())

                                # 게시글이나 날짜 등은 게시물 내에서 하나 밖에 없기 때문에 리스트가 아닌 일반 변수로 저장
                                elif isinstance(part_html, list) == False:
                                    tmpstr = part_html.text.strip()
                                else:
                                    tmplist.append(part_html.text.strip())

                            # tmpvalue가 None일 때 str이 0이되면 리스트가 된다
                            if len(tmpstr) > 0:
                                tmpvalue = tmpstr
                            elif len(tmplist) > 0:
                                tmpvalue = tmplist

                        # 한 행(댓글 등)이 종료되면, 개별 항목마다 검사하여 fillblanks를 채워준다.
                        Dict_completed_chk = self.cr_pagesinspector(tmpvalue).values()
                        content_dict[keykeys[j + 2]] = list(Dict_completed_chk)[1]
                    
                    
                    scnt += 1
                    if scnt % 30 == 0:
                        wd.delete_all_cookies()
                        print('쿠키 지움!')

                    # list에 모든 dictionary type 저장.
                    contents_part_list.append(content_dict)
                    count_cr += 1
                    oksign = False

                except:
                    print('timeout!',innerlink,'로 재접속')
                    print('브라우져 닫음')
                    wd.quit()

                    host = ""  # linux구분
                    if platform.system() != "Linux":
                        options = webdriver.ChromeOptions()
                        # options.add_argument('headless')

                        options.set_headless(True)
                        wd = webdriver.Chrome(
                            executable_path=r'C:\Program Files (x86)\Google\Chrome\Application\chromedriver.exe',
                            options=options)

                    else:
                        firefox_profile = webdriver.FirefoxProfile()
                        firefox_profile.set_preference('permissions.default.image', 2)
                        firefox_profile.set_preference('dom.ipc.plugins.enabled.libflashplayer.so', 'false')
                        options = Options()
                        options.set_headless(True)  # newer webdriver versions
                        wd = webdriver.Firefox(options=options, executable_path=r'/home/centos/selenium_test/geckodriver', firefox_profile = firefox_profile)
                        print("Headless Firefox Initialized Once Again!")
                
                    oksign = True
    
        wd.quit()
        print('긁기 완료, 브라우져 닫음')
        return contents_part_list

    # 페이지를 설정할 수 있게 옵션 선택
    def selenium_upperpage_only(self, lastpage, cvalues, pagetype):  # 역시 여기서도 pagetype 설정 추가
        ### 크롤링 시간측정 시작 ####
        start_time = time.time()
        u_time = None
        l_time = None

        ### 변수설정
        keykeys = list(cvalues.keys())
        keyvalues = list(cvalues.values())
        url = keyvalues[0]  # 접속할 주소 및 기타 접속 정보
        news = News_company()  # 언론사 수집을 위한 인스턴스 생성
        cd = CrwalingDAO()
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36'}
        print('%s 에 접속합니다. : ' % url)

        #################################
        # 1. upper page - 상단 페이지 실행
        
        for dummy_page in range(1, int(lastpage)+1):
            print(dummy_page, '번째 페이지 시작!')
            self.cr_upperpages(url, headers, dummy_page, keyvalues, start_time, pagetype)
            u_time = time.time()
            print('It takes %s seconds completing the upper page crawling and the uploading' % (
                round(u_time - start_time, 2)))

            currentPath = os.path.relpath(os.path.dirname(__file__))
            with open(currentPath + 'dump.pickle', 'rb') as f:
                upper_page_list = pickle.load(f)

            empty_dict = {}
            #for i in range(len(upper_page_list[0])):
            #    empty_list.append([])

            cr = []
            cr.append(upper_page_list)
            cr.append(empty_dict)
            cd = CrwalingDAO()
            cd.insert(cr)
            print('insert 완!!!')

            ### 크롤링 시간측정 종료 ###
            print(" It takes %s seconds crawling these webpages" % (round(time.time() - start_time, 2)))
    
    
    def selenium_lowerpage_only(self, collection, cvalues):
        start_time = time.time()
        u_time = None
        l_time = None

        ### 변수설정
        keykeys = list(cvalues.keys())
        keyvalues = list(cvalues.values())
        news = News_company()
        cd = CrwalingDAO()
        
        empty = False
        while empty == False:

            result = cd.find_empty(collection)
            chkempty = len(result)

            if chkempty == 0:
                empty = True
            else:   
                # 원하는 콜렉션에서 데이터 가져오기 
                board_links = []

                for i in range(len(result)):
                    board_links.append(result[i]['clink'])

                ids = []

                for i in range(len(result)):
                    ids.append(result[i]['_id'])

                print(board_links)
                # ################################
                # #2. lower page - 하단 페이지 실행
                contents_part_list = self.cr_lowerpages(board_links, keykeys, keyvalues)
                l_time = time.time()

                # #################################
                # # 3. 언론사 정보 가져오기 => contents_part_list를 호출하여 다시 contents_part_list를 return
                # # print(contents_part_list)

                # # 모든 크롤링이 끝나고 contents_part_list에 news_company 추가
                #print(contents_part_list)

                print('News Company Analyzing...')
                for i in range(len(contents_part_list)):
                
                    links_in_content = contents_part_list[i]['clinks']  # 게시물 내에
                    
                    news_company = news.add_news_company(links_in_content)
                    contents_part_list[i]['news_company'] = news_company

                #print(contents_part_list)    
            
                cd = CrwalingDAO()
                cd.update_one(contents_part_list, collection, ids)
                print('insert 완!!!')
        print('It takes %s seconds completing the news info crawling and the uploading' % (round(time.time() - l_time, 2)))