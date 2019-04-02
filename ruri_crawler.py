# 접속 및 파싱
import requests
import lxml.html
from lxml import etree
import cssselect
import collections
from news_company import News_company
from ruri_dao import CrwalingDAO
from ruri_etc import CrStatus

import os  # 파일 및 디렉토리 생성
import sys
import pickle  # 피클형태로 데이터 저장
from time import sleep  # 딜레이
import time  # 시간측정
import datetime


class WebCrawler:
    # 링크정보를 꼬리만 가지고 있을 때, 모든 정보를 합침.
    def adjusthtml_pb_tail(self, part_html, head=""):

        if 'http' not in part_html.get('href'):
            full_html = head + part_html.get('href')
        else:
            full_html = part_html.get('href')
        return full_html

    # 백업 확인 (백업 피클 파일 호출)
    def cr_backupchk(self):
        loadcontainer = None
        bkupdir = 'backup'
        bkupfile = 'backup.pickle'
        currentPath = os.path.relpath(os.path.dirname(__file__))
        bkupdirPath = os.path.join(currentPath, bkupdir)
        bkupfilePath = os.path.join(bkupdirPath, bkupfile)
        if os.path.isdir(bkupdirPath) != True:
            pass
        else:
            if os.path.isfile(bkupfilePath) != True:
                with open(bkupfilePath, 'rb') as f:
                    loadcontainer = pickle.load(f)
                    print(loadcontainer)
        return loadcontainer

    # 백업 (백업 파일 피클로 저장)
    def cr_backup(self, odata, init=False):
        data = odata
        container = list()  # upper, lower, newslist를 저장할 리스트
        loadcontainer = None
        bkupdir = 'backup'
        bkupfile = 'backup.pickle'
        currentPath = os.path.relpath(os.path.dirname(__file__))
        bkupdirPath = os.path.join(currentPath, bkupdir)
        bkupfilePath = os.path.join(bkupdirPath, bkupfile)
        # print(os.path.realpath(bkupdirPath))

        # (당연히 최초) 백업디렉토리가 없으면, 디렉토리 생성 파일 생성
        if os.path.isdir(bkupdirPath) != True:
            os.mkdir(bkupdirPath)
            container.append(data)
            with open(bkupfilePath, 'wb') as f:
                pickle.dump(container, f)
        # 백업디렉토리가 있으면,
        else:
            # upper page의 경우
            if init == True:
                container.append(data)
                # 바로 디렉토리에 데이터 생성 (덮어쓰기)
                with open(bkupfilePath, 'wb') as f:
                    pickle.dump(container, f)
            # 그 외의 경우 (lower, newslinks)
            else:
                try:
                    # 데이터를 load한 뒤
                    with open(bkupfilePath, 'rb') as f:
                        loadcontainer = pickle.load(f)
                        # 새로운 데이터를 저장한 뒤
                        loadcontainer.append(data)

                except FileNotFoundError:
                    # 에러가 생긴 경우 강제로 True로 변경하여
                    # 우선 두번째 파일이라도 백업할 수 있도록 함.
                    init = True
                    print("load 할 파일을 못 찾았습니다. 우선 백업 없이 진행합니다.")

                finally:
                    if init == True:
                        # 데이터 저장
                        with open(bkupfilePath, 'wb') as f:
                            pickle.dump(data, f)
                    else:
                        # 다시 저장
                        with open(bkupfilePath, 'wb') as f:
                            pickle.dump(loadcontainer, f)
        print('백업성공')

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
                # 빈 리스트가 모두 리스트의 앞 쪽으로 올 수 있게 정렬함 => 맨 뒤는 무조건 숫자가 있다는 뜻
                chk = sorted(dump)

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
        chkDict = {'number': elements, 'dump': dump}
        return chkDict

    # 페이지 분할 후 분할 구간을 return하는 함수
    def splitpages(self, nsplit, lastpage, firstpage):
        # 상단페이지에서 나온 링크를 지정한 수로 쪼개 페이지의 각 구간 값을 구하고
        indexdiv = list()
        startpage = firstpage - 1  # 첫 번째 페이지는 1페이지가 기본으로 설정되었기 때문에 구간을 정할 때는 1을 빼줌
        endpage = lastpage
        nsplit = nsplit
        pagerange = endpage-startpage  # 크롤링 할 목표 구간 범위

        # nsplit이 pagerange보다 크면 강제로 조절한다.
        if nsplit > pagerange:
            nsplit = int(pagerange/2)

        nlistquotient = int(pagerange/nsplit)  # 구간 값
        nlistremainder = int(pagerange % nsplit)  # 구간 값에 못 미친 나머지
        # print('페이지의 몫과 나머지 : ', nlistquotient, nlistremainder)
        r = nsplit
        if nlistremainder != 0:
            r = nsplit+1

        # 구간을 설정.
        print('구간번호  |   분할구간   /   전체 (기준 : 페이지)')
        print('-----------------------------------------------------')
        # 예) 구간을 3으로 쪼개면, loop로 돌려야 할 구간은 총 4개
        for i in range(r):
            # 시작페이지를 기본 값으로 갖으면 시작페이지를 조절할 수 있다.
            nfrom = startpage + i*nlistquotient
            nto = startpage
            # 구간 구별
            # 나머지가 없다면,
            if i is not nsplit:
                nto = nto + (i+1)*nlistquotient  # 일반 구간 (직접 시작페이지를 적용)
            # 만약 나머지가 있다면,
            else:
                nto = nfrom + nlistremainder  # 마지막 구간 (nfrom에서 시작페이지를 적용)
            # 인덱스에 넣을 구간 값은 0부터 넣는다.
            tmp = [nfrom, nto]
            indexdiv.append(tmp)
            # 페이지의 범위에서 볼 때는 1씩 추가한다.
            print('   %2d     | %5d  ~ %5d / %5d' %
                  (i+1, nfrom+1, nto, endpage))
        print('-----------------------------------------------------')
        return indexdiv

    # 매개변수로 날짜를 쓰는 사이트를 위해 만든 함수
    # 페이지를 날짜로 변경해주는 함수
    def convertpagetodate(self, firstpage, lastpage):
        # 페이지로 받아오는 값은 index값이기 때문에 마지막 페이지에서 1을 뺀다. [0-3] => 3apge (but 4개로 보임)
        lastpage = lastpage-1  # 구간값
        cnt = lastpage - firstpage  # 구간값
        today = datetime.date.today()

        # 구간분할용으로 페이지가 첫 페이지가 아니면 기준날짜를 바꿔준다.
        if firstpage > 0:  # 1, 2....
            today = today - datetime.timedelta(days=(firstpage))  # 0

        # 목표 날짜를 정한다.
        targetday = today - datetime.timedelta(days=cnt)  # 구간값

        # 한 페이지를 변경하기 위해서는 하루 전 날짜가 필요하기 때문에 이를 모두 계산하여 제너레이터로 만들어 둔다.
        def daysgenerator(today, cnt):  # 4
            for i in range(1, cnt+1):  # 4 + 1
                nextday = today - datetime.timedelta(days=i)
                nextday = datetime.datetime.strftime(nextday, '%Y%m%d')
                yield nextday

        nextdays = daysgenerator(today, cnt)
        # 날짜를 str로 변환
        today = datetime.datetime.strftime(today, '%Y%m%d')
        targetday = datetime.datetime.strftime(targetday, '%Y%m%d')
        return today, targetday, nextdays, cnt

    # 상단 페이지의 정보 크롤링 함수

    def cr_upperpages(self, target, url, headers, splitstartpage, splitlastpage, lastpage, keyvalues, start_time, startini=0, endini=6):
        # 준비
        first_page = 0  # 첫 번째 페이지의 수를 저장할 변수
        backupinfo = []  # backup을 위한 리스트
        upper_page_list = []  # 매 페이지의 정보를 저장할 리스트 준비
        status = CrStatus()

        # 0-2. 값이 없는 upper page의 요소를 확인하기 위해
        # 모든 요소를 우선 리스트화 하여 접근할 수 있도록 준비
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

        # 크롤링 시작
        # 1페이지에서부터 시작해야 하기 때문에 시작과 종료페이지에 각각 1을 더한다.
        for i in range(int(splitstartpage)+1, int(splitlastpage)+1):
            # 파라미터
            params = {}  # 페이지 이동을 위한 파라미터
            if target == 'clien':
                params = {'page': i}
                # pass # 아래의 주석을 수정하여 원하는대로 파라미터를 수정할 수 있음.
                # params = {'po' : i-1} #0페이지부터 시작할 수 있도록 i-1

            elif target == 'MPark':
                params = {'p':(i-1)*30+1, 'm':'search','query':'정치', 'select':'spf'}

            else:
                params = {'page': i}

            # 변수
            errorpass = False  # 에러발생 확인용 변수. 기본적으로 False

            try:
                # 접속
                res = requests.get(url, headers=headers, params=params)
                html = res.text
                root = lxml.html.fromstring(html)

                sleep(0.1)

            except ConnectionResetError as e:
                errorpass = True
                print('%s에서 에러 발생' % 'ConnectionResetError')
                print('%s 오류 다음 페이지에서 재접속' % e)

            except requests.ConnectionError as e:
                errorpass = True
                print('%s에서 에러 발생' % 'requests.ConnectionError')
                print('%s 오류 다음 페이지에서 재접속' % e)

            except requests.exceptions.ConnectionError as e:
                errorpass = True
                print('%s에서 에러 발생' % 'requests.exceptions.ConnectionError')
                print('%s 오류 다음 페이지에서 재접속' % e)

                # 톰캣서버를 활성화 시킨다음에 서버에 접속시키면서 강제로 에러를 발생시켜
                # connectionerror를 누르면 해결이 되는지 확인.

                # 여기에 재접속 코드 삽입. => 적용취소

            except requests.exceptions.ChunkedEncodingError as e:
                errorpass = True
                print('%s에서 에러 발생' % 'requests.exceptions.ChunkedEncodingError')
                print('%s 오류 다음 페이지에서 재접속' % e)

            except etree.ParserError as e:
                errorpass = True
                print('%s 오류로 다음 페이지에서 재접속' % e)
                # 내용이 비어 있다면 채우고 각 게시글의 내용, 링크, 댓글 등을 딕셔너리에 저장
                # 해당 페이지의 정보를 모두 blank 채우고 다음페이지 호출

            finally:
                # 만일 에러가났다면,
                if errorpass == True:
                    print('오류가 일어난 페이지의 "fillblanks" 처리')
                    # CSS에 등록된 upper page의 개수만큼 loop를 돌며 빈칸으로 넣어줌.
                    for j in range(startini, endini):
                        upper_page_list[j].append('')
                # 에러가 나지 않았다면,
                else:
                    # CSS에 등록된 upper page의 수만큼 loop를 돌며 내용을 넣음.
                    for j in range(startini, endini):
                        for part_html in root.cssselect(keyvalues[j+2]):
                            if j == 1:
                                upper_page_list[j].append(
                                    self.adjusthtml_pb_tail(part_html, keyvalues[1]))
                            else:
                                upper_page_list[j].append(
                                    part_html.text_content())

                status.progressBar(i, lastpage, 'crawling pages')
                # print('페이지 수집중 : %s / %s , 소요시간 %s 초' % (i, lastpage, (round(time.time() - start_time,2))))

        # 크롤링 검사 => 빈 칸은 fillblinks를 채움
        list_completed_chk = self.cr_pagesinspector(upper_page_list).values()
        print('\n총 수집한 링크 수 : ', list(list_completed_chk)[0])  # 정보

        # 크롤링 백업
        # 상단에서 백업에 보낼 데이터 리스트에는 상단 페이지를 크롤링하여 얻은
        # 첫 번째 페이지의 총 수, 최종 인덱스 숫자, 완료여부를 리턴하고 피클에 저장
        # backupinfo = [first_page, list(list_completed_chk)[0], True]

        # # 백업이 있으면,
        # if backuploaded != False:
        #     # 모두 완료되었으면 이미 완성된 크롤링이 남긴 백업으로 파악하고 (backuploaded = [0,0,1,0,1,0,1]), 백업 파일을 새롭게 덮어쓰며
        #     if (backuploaded[2] == True) and (backuploaded[4] == True) and (backuploaded[6] == True):
        #         self.cr_backup(backupinfo, True) #상단페이지 백업
        #     # 그렇지 않으면 백업하지 않음.
        #     else:
        #         pass
        # # 백업이 없으면, 실행
        # else:
        #     self.cr_backup(backupinfo, True) #상단페이지 백업
        return list(list_completed_chk)[1]

    # 하단 페이지의 정보 크롤링 함수
    def cr_lowerpages(self, target, headers, upper_page_list, keykeys, keyvalues, last_time, startini=6, endini=12):
        # lower page - 하단 페이지 실행
        # 수집한 링크에 접속하여 아래의 정보를 저장.

        # 8. 게시글
        # 9. 내부링크
        # 10. 댓글
        # 11. 추천
        # 12. 비추천
        # 13. 날짜

        # 변수
        count_cr = 1                    # 현재 진행사항을 파악하기 위한 변수 설정
        cut_duplicate = None            # 중복이 생겼을 때 자르기 위한 변수
        is_duplicate = False            # 중복이 생겼을 때 나머지 글들을 모두 넘기기 위한 변수 
        contents_part_list = []         # 컨텐츠용 변수

        # last time은 날짜, 제목 튜플로 넘어오기 때문에 각각 지정 
        last_title = last_time[1]  
        last_time = last_time[0]

        status = CrStatus()  # progress bar
        # 수집한 내부링크(게시판)의 수만큼 loop를 돌며 접속
        for innerlink in upper_page_list[1]:
            status.progressBar(count_cr, len(
                upper_page_list[1]), 'crawling contents')
           
            # 변수
            errorpass = False  # 재접속 확인
            content_dict = {}

            maybe_duplicate = False # 먼저 upper_page에서 끌어온 제목과 마지막 크롤링한 제목을 맞춰본다
            
            if upper_page_list[2][count_cr - 1] == last_title:    
                maybe_duplicate = True  # 맞다면 후에 검사를 위해 True로 변경

            try:
                # 접속과 크롤링
                inner_res = requests.get(innerlink, headers=headers)
                inner_html = inner_res.text
                inner_root = lxml.html.fromstring(inner_html)

                sleep(0.05)

                # ini 파일에 입력한 CSS tag중 lower page에 해당하는 행 번호를 가져와
                for j in range(startini, endini):
                    tmpvalue = None  # 리턴할 변수를 하나로 줄이기 위해 None으로 선언
                    tmpstr = ''
                    tmplist = []
                    selected_ir = inner_root.cssselect(keyvalues[j+2])

                    # 해당 행(예를 들어 댓글)에 따른 번호를 넣어준다.
                    for part_html in selected_ir:
                        if j+2 == 9:
                            # 특이사항 : a태그로 link를 불러왔으나, 그림파일 등 a 태크를 사용하는 경우 blank 저장
                            if part_html.get('href') is None:
                                continue

                            tmplist.append(part_html.get('href'))  # 내부링크
                        
                        elif (j+2 == 13 and part_html.text_content() == last_time and cut_duplicate == None and maybe_duplicate == True): # 중복 내용 최초 발견
                            # 중복 여부 테스트 중
                            cut_duplicate = count_cr - 1  # 중복 제거용 인덱스 생성
                            print(
                                ' *** 중복 글이 발견되었습니다. 해당 글을 제외한 글만 크롤링 완료 후 종료합니다 *** ')
                            print('중복 내용: %s %s %s %s\n ' %
                                (last_time, last_title, part_html.text_content(),upper_page_list[2][count_cr - 1] ))
                            tmpstr = part_html.text_content()
                            is_duplicate = True
                            
                        else:
                            # 게시글이나 날짜 등은 게시물 내에서 하나 밖에 없기 때문에 리스트가 아닌 일반 변수로 저장
                            if len(selected_ir) < 2:
                                tmpstr = part_html.text_content()
        
                            else:
                                tmplist.append(part_html.text_content())

                        # tmpvalue가 None일 때 str이 0이되면 리스트가 된다
                        if len(tmpstr) > 0:
                            tmpvalue = tmpstr
                        elif len(tmplist) > 0:
                            tmpvalue = tmplist

                    # 한 행(댓글 등)이 종료되면, 개별 항목마다 검사하여 fillblanks를 채워준다.
                    Dict_completed_chk = self.cr_pagesinspector(
                        tmpvalue).values()
                    content_dict[keykeys[j+2]] = list(Dict_completed_chk)[1]

            except ConnectionResetError as e:
                errorpass = True
                print('%s에서 에러 발생' % 'ConnectionResetError')
                print('%s 오류 다음 페이지에서 재접속' % e)

            except requests.ConnectionError as e:
                errorpass = True
                print('%s에서 에러 발생' % 'requests.ConnectionError')
                print('%s 오류 다음 페이지에서 재접속' % e)

            except requests.exceptions.ConnectionError as e:
                errorpass = True
                print('%s에서 에러 발생' % 'requests.exceptions.ConnectionError')
                print('%s 오류 다음 페이지에서 재접속' % e)

                # 톰캣서버를 활성화 시킨다음에 서버에 접속시키면서 강제로 에러를 발생시켜
                # connectionerror를 누르면 해결이 되는지 확인.

                # 여기에 재접속 코드 삽입. => 적용취소

            except requests.exceptions.ChunkedEncodingError as e:
                errorpass = True
                print('%s에서 에러 발생' % 'requests.exceptions.ChunkedEncodingError')
                print('%s 오류 다음 페이지에서 재접속' % e)

            except etree.ParserError as e:
                errorpass = True
                print('%s 오류로 다음 페이지에서 재접속' % e)
                # 내용이 비어 있다면 채우고 각 게시글의 내용, 링크, 댓글 등을 딕셔너리에 저장
                # 해당 페이지의 정보를 모두 blank 채우고 다음페이지 호출

            finally:
                # 만일 에러가났다면,
                if errorpass == True:
                    print('오류가 일어난 페이지 처리')
                    # CSS에 등록된 lower page의 개수만큼 loop를 돌며 빈칸을 채움
                    for j in range(startini, endini):
                        tmpvalue = None
                        Dict_completed_chk = self.cr_pagesinspector(
                            tmpvalue, errorpass).values()
                        content_dict[keykeys[j+2]
                                     ] = list(Dict_completed_chk)[1]

                # print('빈 셀을 채운 개수 : ', list(Dict_completed_chk)[0])

            # list에 모든 dictionary type 저장.
            contents_part_list.append(content_dict)
            count_cr += 1

            # 중복 확인 변수 생성없이 넘어간다면, upper page 리스트의 길이와 똑같이 설정(자르는 부분 없음)
        if cut_duplicate == None:
            cut_duplicate = len(upper_page_list[0])

        # self.cr_backup(backupinfo) #하단페이지 백업
        return contents_part_list, cut_duplicate

    # 하단 페이지의 게시판에서 추출한 뉴스링크 크롤링 함수
    def cr_newspages(self, target, headers, upper_page_list, contents_part_list, cut_duplicate):
        # contents_part_list를 호출하여 다시 contents_part_list를 return
        # 모든 크롤링이 끝나고 contents_part_list에 news_company 추가

        # 변수
        news = News_company()  # 언론사 수집을 위한 인스턴스 생성
        backupinfo = []  # backup을 위한 리스트
        n_contents_part_list = contents_part_list  # 매개변수를 다시 저장하여 넘겨줄 변수 선언

        # 중복 확인
        if cut_duplicate == len(upper_page_list[0]):
            is_duplicate = False
        else:
            is_duplicate = True

        print('News Company Analyzing...')
        for i in range(len(contents_part_list)):
            # 게시물 자체 링크(혹시나 그 링크로 다시 돌아가야 될 때 대비(뉴스 컴퍼니 함수에선 현재 비활성화))
            board_link = upper_page_list[1][i]
         
            links_in_content = n_contents_part_list[i]['clinks']  # 게시물 내에
           
            news_company = news.add_news_company(links_in_content, board_link)
            n_contents_part_list[i]['news_company'] = news_company
           
        return n_contents_part_list, is_duplicate

    # 구간 세분화 실행

    def crawlingpostslittle(self, collection, target, nsplit, firstpage, lastpage, cvalues):
        ### 크롤링 시간측정 시작 ####
        start_time = time.time()
        u_time = None
        l_time = None

        # 변수설정

        # cook hotfix : 쿡은 사이트 디자인이 좀 잘못되어있어서, 페이지 0번을 선택하면 글 내용이 중복되는 문제가 발생.
        # 한 세션(page split) 에 0페이지와 1페이지가 함께 있다면, 아직 중복 체크가 안된 동일한 글 2개가 같이 들어가는 문제 발생.
        # 첫페이지이기 때문에 꽤나 골치아픈 문제일 것 같아 강제로 0->1로 설정 (글 내용엔 문제 없음)
        if (target == 'cook' and firstpage == 0):
            firstpage = 1

        pageranges = self.splitpages(
            nsplit, lastpage, firstpage)  # 시작 및 종료 페이지 구간설정
        upper_page_list = None
        keykeys = list(cvalues.keys())
        keyvalues = list(cvalues.values())
        url = keyvalues[0]  # 접속할 주소 및 기타 접속 정보
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36'}
        
        # 중복여부 테스트용
        dao = CrwalingDAO()
        last_time = dao.select_last_time(collection)
        is_duplicate = False
        print('마지막 날짜와 글:', last_time, '\n')

        print('########################')
        print('%s으로 접속중...' % target)
        print('########################')

        # 조정한 페이지 구간 수 만큼 루프
        for i in range(len(pageranges)):
            
            if is_duplicate:
                print('중복 페이지가 발견되었습니다. %s 의 크롤링을 종료합니다' % target)
                break
            else:
                #################################
                # 1. upper page - 상단 페이지 실행
                upper_page_list = self.cr_upperpages(
                    target, url, headers, pageranges[i][0], pageranges[i][1], lastpage, keyvalues, start_time)
                u_time = time.time()
                print('It takes %s seconds completing the upper page crawling and the uploading' % (
                    round(u_time - start_time, 2)))
                print('upper page list ', len(upper_page_list[1]))
                print(len(upper_page_list[1]), len(upper_page_list[2]))
                input('dd')
                #################################
                # 2. lower page - 하단 페이지 실행
                lower_page_list = self.cr_lowerpages(
                    target, headers, upper_page_list, keykeys, keyvalues, last_time)  # + 마지막 크롤링 시간 추가
                cut_duplicate = lower_page_list[1]  # 중복 자르는 번호
                lower_page_list = lower_page_list[0]
                l_time = time.time()
                print('It takes %s seconds completing the lower page crawling and the uploading' % (
                    round(l_time - u_time, 2)))
                print('lower page list ', len(lower_page_list))

                sleep(2)

                #################################
                # 3. 언론사 정보 가져오기
                # print(contents_part_list)
                contents_part_list = self.cr_newspages(
                    target, headers, upper_page_list, lower_page_list, cut_duplicate)
                is_duplicate = contents_part_list[1]  # 2번째 리턴값이 중복 여부 boolean
                contents_part_list = contents_part_list[0]  # 1번째 리턴값이 실제 데이터

                print('It takes %s seconds completing the news info crawling and the uploading' % (
                    round(time.time() - l_time, 2)))

                ### 크롤링 시간측정 종료 ###
                print("It takes %s seconds crawling these webpages" %
                      (round(time.time() - start_time, 2)))

                #################################
                # 4. insert

                # 중복이 없다면 cut_duplicate를 해도 아무것도 잘리지 않는다
                # upperpage_list는 리스트 안의 리스트이기 때문에 각각 잘라준다
                upper_page_list = [arr[:cut_duplicate]
                                   for arr in upper_page_list]

                cr = [upper_page_list, contents_part_list[:cut_duplicate]]

                insertmgdb = CrwalingDAO()
                insertmgdb.insert(cr, collection)

    def crawling_nvnews(self, target, nsplit, firstpage, lastpage, cvalues):
        ### 크롤링 시간측정 시작 ####
        start_time = time.time()
        status = CrStatus()
        u_time = None
        l_time = None
        keykeys = list(cvalues.keys())
        keyvalues = list(cvalues.values())
        url = keyvalues[0]
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36'}

        pageranges = self.splitpages(nsplit, lastpage, firstpage)  # 페이지 분할

        # 분할페이지 적용
        for p in range(len(pageranges)):
            print(' == phase1 == ')
            # phase 1------------------------------------------------------------------
            topic_html_list = []  # 메인 주제 파싱용 html 리스트
            # css
            topic_section_css = keyvalues[2]

            # 먼저 각 주제를 구분하기 위해 주제마다 파싱용 html 만 가져온다
            for i in range(int(pageranges[p][0])+1, int(pageranges[p][1])+1):
                params = {'page': i}
                res = requests.get(url, headers=headers, params=params)
                html = res.text
                root = lxml.html.fromstring(html)

                for topic in root.cssselect(topic_section_css):
                    topic_html_list.append(lxml.html.tostring(topic))

                sleep(0.2)
                status.progressBar(i, len(pageranges), '주제구분 html')

            print('\n == phase2 == ')
            # phase2 ----------------------------------------------------------------

            topic_list = []  # 메인 주제 제목 리스트
            news_title_list = []  # 사설 제목 리스트
            news_company_list = []  # 해당 사설 언론사 리스트
            news_link_list = []  # 사설 링크 리스트

            # css
            topic_title_css = keyvalues[3]
            news_title_css = keyvalues[4]
            news_company_css = keyvalues[5]

            # 가져온 파싱용 html 하위에 있는 사설들의 제목, 각 사설의 언론사, 각 사설의 링크를 가져온다
            # 리스트의 개수에 맞춰서 주제의 제목도 반복해서 넣어준다
            for i in range(len(topic_html_list)):
                root = lxml.html.fromstring(topic_html_list[i])

                cnt = 0  # 사설의 개수에 맞춰서 주제도 중복해서 넣기 위한 카운트

                # 사설 제목
                for title in root.cssselect(news_title_css):
                    news_title_list.append(title.text_content())
                    cnt += 1

                # 사설 언론사
                for company in root.cssselect(news_company_css):
                    news_company_list.append(company.text_content())

                # 사설 링크(내용 추출용)
                # 링크의 경우 제목과 css는 같고, href만 get해서 가져오면 된다
                for link in root.cssselect(news_title_css):
                    news_link_list.append(link.get('href'))

                topic_title = root.cssselect(topic_title_css)[0]

                for j in range(cnt):
                    topic_list.append(topic_title.text_content())

                status.progressBar(i+1, len(topic_html_list), '주제 제목 및 기타정보')

            print('\n == phase3 == ')
            # phase 3 -------------------------------------------------------------------

            content_list = []
            content_date_list = []

            # css
            content_css = keyvalues[6]
            content_date_css = keyvalues[7]

            # phase2 에서 가져온 링크에 접속해서 해당 사설의 내용 가져오기

            for i in range(len(news_link_list)):
                inurl = news_link_list[i]
                res = requests.get(inurl, headers=headers)
                html = res.text
                root = lxml.html.fromstring(html)

                for content in root.cssselect(content_css):
                    content_list.append(content.text_content())

                # 날짜는 변환해서 넣음
                for date in root.cssselect(content_date_css):
                    strdate = date.text_content()
                    realdate = datetime.datetime.strptime(
                        strdate, '%Y-%m-%d %H:%M')
                    content_date_list.append(realdate)

                status.progressBar(i+1, len(news_link_list), '주제 내용 및 기타정보')
                sleep(0.2)

            print('\n == phase4 == ')
            # phase 4---------------------------------------------------------

            # 함수의 리턴을 위한 사전 생성
            # 사전을 담기 위한 리스트
            news_dict_list = []

            for i in range(len(news_title_list)):
                temp = {
                    "cno": topic_list[i],
                    "clink": news_link_list[i],
                    "ctitle": news_title_list[i],
                    "cthumbup": "fillblanks",
                    "cthumbdown": "fillblanks",
                    "content":
                        {
                            "ccontent": content_list[i],
                            "clinks": 'fillblanks',
                            "creplies": 'fillblanks',
                            "cthumbupl": 'fillblanks',
                            "cthumbdownl": 'fillblanks',
                            "idate": content_date_list[i],
                            "news_company": news_company_list[i]
                    }
                }

                news_dict_list.append(temp)

            insertDB = CrwalingDAO()
            insertDB.insertnews(news_dict_list)

    def crawling_natepann(self, target, nsplit, firstpage, lastpage, cvalues):
        status = CrStatus()
        keykeys = list(cvalues.keys())
        keyvalues = list(cvalues.values())

        # 메인 페이지 들어갈 기본 url 및 헤더
        head = keyvalues[1]
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36'}

        # 페이지 분할
        pageranges = self.splitpages(nsplit, lastpage, firstpage)

        for p in range(len(pageranges)):

            # 페이지를 날짜로 변환
            strDates = self.convertpagetodate(
                pageranges[p][0], pageranges[p][1])

            start_str = strDates[0]
            end_str = strDates[1]
            next_str = strDates[2]
            cnt = strDates[3]

            print('시작일 : %s - 종료일 : %s' % (start_str, end_str))
            time.sleep(2)

            # 분할된 페이지의 수만큼 반복
            for splitedpage in range(int(pageranges[p][0])+1, int(pageranges[p][1])+1):
                # while cnt != 0: # 돌릴 날짜의 일수에 따라 while문으로 계속 반복
                print('=-=-=-=-=-=', start_str, '시작! =-=-=-=-=-=')
                # =-=-=-=-=-=-=-=-=-=-=-=-= PHASE 1 =-=-=-=-=-=-=-=-=-=-=-=-=
                # 메인 페이지에 접속 및 글의 링크 가져오기

                # 기본 url + 입력한 시작 날짜 = 메인 페이지 URL
                main_url = head + start_str

                # 페이지 접속
                res = requests.get(main_url, headers=headers)
                html = res.text
                root = lxml.html.fromstring(html)

                # 오늘의 톡, 오늘의 연예가 섞여있음
                # 오늘의 톡만 뽑기 위해 첫번째 섹션만 일단 html 텍스트로 가져옴
                upper_section_css = keyvalues[2]
                todaytalk = root.cssselect(upper_section_css)[0]
                todaytalk = lxml.html.tostring(todaytalk)

                # 가져온 오늘의 톡 섹션만 파싱
                subroot = lxml.html.fromstring(todaytalk)

                # 글 내부로 안내할 링크를 모을 리스트
                link_list = []

                # 필요 css
                clink_css = keyvalues[3]

                # 'title' 속성을 가지고 있는 링크만 가져와서 리스트에 추가
                for part_html in subroot.cssselect(clink_css):
                    link_list.append(part_html.get('href'))
                print('링크 수 : ', len(link_list))

                # =-=-=-=-=-=-=-=-=-=-=-=-= PHASE 2 =-=-=-=-=-=-=-=-=-=-=-=-=
                # 가져온 링크를 통해 글 안으로 접속

                # 가져온 링크는 /로 시작하므로 또 다른 기본 URL 준비
                base_url = keyvalues[0]

                # 필요 리스트
                title_list = []
                content_list = []
                date_list = []
                reply_list = []
                thumbup_list = []
                thumbdown_list = []

                # 필요 css
                title_css = keyvalues[4]
                content_css = keyvalues[5]
                reply_css = keyvalues[6]
                thumbup_css = keyvalues[7]
                thumbdown_css = keyvalues[8]
                date_css = keyvalues[9]

                # 가져온 글 링크 개수에 맞게 반복문
                for i in range(len(link_list)):
                    inurl = base_url + link_list[i]

                    # 글 접속 및 css로 요소 가져오기
                    # 예외처리 이유 : 여러가지 이유로 삭제된 글들이 많아서 Indexerror 일으킴
                    # 예외처리는 밑에 해놓았고, finally로 마무리 해놓았음
                    try:
                        res2 = requests.get(inurl, headers=headers)
                        html2 = res2.text
                        root2 = lxml.html.fromstring(html2)

                        # 글 제목
                        title = root2.cssselect(title_css)[0]
                        title = title.text_content()

                        # 글 내용
                        content = root2.cssselect(content_css)[0]
                        content = content.text_content()

                        # 날짜는 datetime 으로 변형하였음
                        date = root2.cssselect(date_css)[0]
                        date = date.text_content()
                        date = datetime.datetime.strptime(
                            date, '%Y.%m.%d %H:%M')

                        # # 댓글은 베스트3포함 1페이지만.
                        reply = []
                        for part_html in root2.cssselect(reply_css):
                            reply.append(part_html.text_content())

                        # 추천
                        thumbup = root2.cssselect(thumbup_css)[0]
                        thumbup = thumbup.text_content()

                        # 비추천
                        thumbdown = root2.cssselect(thumbdown_css)[0]
                        thumbdown = thumbdown.text_content()

                    # 삭제된 글에 대해서 예외처리, 모든 항목 fillblanks
                    except IndexError:
                        title = 'fillblanks'
                        content = 'fillblanks'
                        date = 'fillblanks'
                        reply = 'fillblanks'
                        thumbup = 'fillblanks'
                        thumbdown = 'fillblanks'

                        print(str(i+1), '번째 글은 삭제되었습니다')

                    # try/except와 관계없이 가져온 요소들을 각기 리스트에 저장
                    finally:
                        title_list.append(title)
                        content_list.append(content)
                        date_list.append(date)
                        reply_list.append(reply)
                        thumbup_list.append(thumbup)
                        thumbdown_list.append(thumbdown)

                    status.progressBar(i+1, len(link_list), '내용 크롤링중 ')

                # =-=-=-=-=-=-=-=-=-=-=-=-= PHASE 3 =-=-=-=-=-=-=-=-=-=-=-=-=
                # 사전을 만들고 그 사전을 리스트에 저장
                # 그 리스트를 insert
                dict_list = []

                for i in range(len(title_list)):
                    temp = {
                        "cno": 'fillblanks',
                        "clink": link_list[i],
                        "ctitle": title_list[i],
                        "cthumbup": "fillblanks",
                        "cthumbdown": "fillblanks",
                        "content":
                            {
                                "ccontent": content_list[i],
                                "clinks": 'fillblanks',
                                "creplies": reply_list[i],
                                "cthumbupl": thumbup_list[i],
                                "cthumbdownl": thumbdown_list[i],
                                "idate": date_list[i],
                                "news_company": 'fillblanks'
                        }
                    }

                    dict_list.append(temp)
                    status.progressBar(i+1, len(title_list), '내용 저장중 ')

                # 인서트
                insertDB = CrwalingDAO()
                insertDB.insertnews(dict_list)

                # 인서트 끝나고 안내문

                print('=-=-=-=-=-=', start_str, '완료! =-=-=-=-=-=')

                print('\n')
                time.sleep(1)
                print('1초 슬립!')
                print('\n')

                # =-=-=-=-=-=-=-=-=-=-=-=-= PHASE 4 =-=-=-=-=-=-=-=-=-=-=-=-=
                # 날짜 진행
                if splitedpage != int(pageranges[p][1]):
                    start_str = next(next_str)
                else:
                    start_str = None
                print('다음날 : ', start_str)

                # # n일에서 -1을 해줌
                # cnt -= 1
