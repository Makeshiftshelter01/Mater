# 접속 및 파싱
import requests
import lxml.html
from lxml import etree
import cssselect
import collections
from news_company import News_company
from ruri_dao import CrwalingDAO
from ruri_etc import CrStatus

import os #파일 및 디렉토리 생성
import pickle #피클형태로 데이터 저장
from time import sleep # 딜레이
import time # 시간측정



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
        container = list() #upper, lower, newslist를 저장할 리스트
        loadcontainer = None
        bkupdir = 'backup'
        bkupfile = 'backup.pickle'
        currentPath = os.path.relpath(os.path.dirname(__file__))
        bkupdirPath = os.path.join(currentPath, bkupdir)
        bkupfilePath = os.path.join(bkupdirPath, bkupfile)
        # print(os.path.realpath(bkupdirPath))

        #(당연히 최초) 백업디렉토리가 없으면, 디렉토리 생성 파일 생성
        if os.path.isdir(bkupdirPath) != True:
            os.mkdir(bkupdirPath)
            container.append(data)
            with open(bkupfilePath, 'wb') as f:
                pickle.dump(container, f)
        #백업디렉토리가 있으면,
        else:
            #upper page의 경우
            if init == True:
                container.append(data)
                #바로 디렉토리에 데이터 생성 (덮어쓰기)
                with open(bkupfilePath, 'wb') as f:
                    pickle.dump(container, f)
            #그 외의 경우 (lower, newslinks)
            else:
                try:
                    #데이터를 load한 뒤
                    with open(bkupfilePath, 'rb') as f:
                        loadcontainer = pickle.load(f)
                        #새로운 데이터를 저장한 뒤
                        loadcontainer.append(data)
                    
                    
                except FileNotFoundError:
                    #에러가 생긴 경우 강제로 True로 변경하여
                    #우선 두번째 파일이라도 백업할 수 있도록 함.
                    init = True 
                    print("load 할 파일을 못 찾았습니다. 우선 백업 없이 진행합니다.")
                
                finally:
                    if init == True:
                        #데이터 저장
                        with open(bkupfilePath, 'wb') as f:
                            pickle.dump(data, f)
                    else:
                        #다시 저장
                        with open(bkupfilePath, 'wb') as f:
                            pickle.dump(loadcontainer, f)
        print('백업성공')

    # 빈 페이지 검사용 함수
    def cr_pagesinspector(self, udump, ehapped = False):
        # 모든 변수 및 리스트를 검사해서 비어 있으면 이를 더미 값으로 채움.
        
        #변수
        chkDict = {}
        # chktype = None
        dump = udump
        elements = None # 얼마나 채웠는지를 나타내는 수
        
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
    
    # 페이지 분할 후 분할 구간을 return하는 함수
    def splitpages(self, nsplit, lastpage, firstpage):
        # 상단페이지에서 나온 링크를 지정한 수로 쪼개 페이지의 각 구간 값을 구하고
        indexdiv = list()
        startpage = firstpage - 1 #첫 번째 페이지는 1페이지가 기본으로 설정되었기 때문에 구간을 정할 때는 1을 빼줌
        endpage = lastpage
        nsplit = nsplit
        pagerange = endpage-startpage #크롤링 할 목표 구간 범위
        
        # nsplit이 pagerange보다 크면 강제로 조절한다.
        if nsplit > pagerange:
            nsplit = int(pagerange/2)

        nlistquotient = int(pagerange/nsplit) #구간 값
        nlistremainder = int(pagerange%nsplit) #구간 값에 못 미친 나머지
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
                nto = nto + (i+1)*nlistquotient # 일반 구간 (직접 시작페이지를 적용)
            # 만약 나머지가 있다면,
            else:
                nto = nfrom + nlistremainder # 마지막 구간 (nfrom에서 시작페이지를 적용)
            # 인덱스에 넣을 구간 값은 0부터 넣는다.
            tmp = [nfrom, nto]
            indexdiv.append(tmp)
            # 페이지의 범위에서 볼 때는 1씩 추가한다.
            print('   %2d     | %5d  ~ %5d / %5d' % (i+1, nfrom+1, nto, endpage))
        print('-----------------------------------------------------')
        return indexdiv



    # 상단 페이지의 정보 크롤링 함수
    def cr_upperpages(self, target, url, headers, splitstartpage, splitlastpage, lastpage, keyvalues, start_time, startini = 0, endini = 6):
        ##### 준비
        first_page = 0 # 첫 번째 페이지의 수를 저장할 변수
        backupinfo = [] #backup을 위한 리스트
        upper_page_list = [] # 매 페이지의 정보를 저장할 리스트 준비
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

        ##### 크롤링 시작
        #1페이지에서부터 시작해야 하기 때문에 시작과 종료페이지에 각각 1을 더한다.
        for i in range(int(splitstartpage)+1, int(splitlastpage)+1):
            # 파라미터
            params = {} #페이지 이동을 위한 파라미터
            if target == 'clien':
                params = {'page': i}
                # pass # 아래의 주석을 수정하여 원하는대로 파라미터를 수정할 수 있음.
                # params = {'po' : i-1} #0페이지부터 시작할 수 있도록 i-1
            else:
                params = {'page': i}

            # 변수
            errorpass = False #에러발생 확인용 변수. 기본적으로 False

            try:
                #접속
                res = requests.get(url, headers=headers, params=params)
                html = res.text
                root = lxml.html.fromstring(html)
                
                sleep(0.1)
                       
            except ConnectionResetError as e:
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

                #톰캣서버를 활성화 시킨다음에 서버에 접속시키면서 강제로 에러를 발생시켜
                #connectionerror를 누르면 해결이 되는지 확인.
                
                #여기에 재접속 코드 삽입. => 적용취소

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
                    print('오류가 일어난 페이지의 "fillblanks" 처리')
                    #CSS에 등록된 upper page의 개수만큼 loop를 돌며 빈칸으로 넣어줌.
                    for j in range(startini, endini):
                        upper_page_list[j].append('')
                # 에러가 나지 않았다면,
                else:
                    # CSS에 등록된 upper page의 수만큼 loop를 돌며 내용을 넣음.
                    for j in range(startini, endini):                
                        for part_html in root.cssselect(keyvalues[j+2]):
                            if j == 1:
                                upper_page_list[j].append(self.adjusthtml_pb_tail(part_html, keyvalues[1]))
                            else:
                                upper_page_list[j].append(part_html.text_content())

                status.progressBar(i, lastpage, 'crawling pages')
                # print('페이지 수집중 : %s / %s , 소요시간 %s 초' % (i, lastpage, (round(time.time() - start_time,2))))

        
        ##### 크롤링 검사 => 빈 칸은 fillblinks를 채움
        list_completed_chk = self.cr_pagesinspector(upper_page_list).values()
        print('\n총 수집한 링크 수 : ', list(list_completed_chk)[0]) #정보
        
        ##### 크롤링 백업
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
    def cr_lowerpages(self, target, headers, upper_page_list, keykeys, keyvalues, startini=6, endini=12):
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
        count_cr = 1                    # 현재 진행사항을 파악하기 위한 변수 설정
        contents_part_list = []         # 컨텐츠용 변수
        backupinfo = [] #backup을 위한 리스트
        status = CrStatus() #progress bar
        
        # 수집한 내부링크(게시판)의 수만큼 loop를 돌며 접속 
        for innerlink in upper_page_list[1]:
            status.progressBar(count_cr, len(upper_page_list[1]), 'crawling contents' )
            # print('크롤링 진행사항 :', count_cr, ' / ', len(upper_page_list[1]))
            
            # 변수
            errorpass = False #재접속 확인
            content_dict = {}
            
            try:
                # 접속과 크롤링
                inner_res = requests.get(innerlink, headers=headers)
                inner_html = inner_res.text
                inner_root = lxml.html.fromstring(inner_html)

                # print(inner_html)
                # print(inner_res.status_code, inner_res.encoding, inner_res.headers['content-type'])
                

                # with open('check.txt', 'w') as f:
                #     f.write(anj)

                sleep(0.05)

                # ini 파일에 입력한 CSS tag중 lower page에 해당하는 행 번호를 가져와
                for j in range(startini, endini):
                    tmpvalue = None # 리턴할 변수를 하나로 줄이기 위해 None으로 선언
                    tmpstr = ''
                    tmplist = []
                    selected_ir = inner_root.cssselect(keyvalues[j+2])

                    # 해당 행(예를 들어 댓글)에 따른 번호를 넣어준다.
                    for part_html in selected_ir:
                        if j+2 == 9:
                            # 특이사항 : a태그로 link를 불러왔으나, 그림파일 등 a 태크를 사용하는 경우 blank 저장
                            if part_html.get('href') is None:
                                continue
                            tmplist.append(part_html.get('href')) #내부링크
                        else:
                            #게시글이나 날짜 등은 게시물 내에서 하나 밖에 없기 때문에 리스트가 아닌 일반 변수로 저장
                            if len(selected_ir) < 2:
                                tmpstr = part_html.text_content()
                            # 12.22 성목 추가
                            # 댓글은 여러개 있을 가능성이 많기 때문에 반드시 리스트로 저장(그래야 전처리 및 분석 쉬움)
                            # elif j+2 == 10: 
                            #     tmplist.append(part_html.text_content())
                            else:
                                tmplist.append(part_html.text_content())
                    
                        # tmpvalue가 None일 때 str이 0이되면 리스트가 된다
                        if len(tmpstr) > 0:
                            tmpvalue = tmpstr
                        elif len(tmplist) > 0:
                            tmpvalue = tmplist

                    # 한 행(댓글 등)이 종료되면, 개별 항목마다 검사하여 fillblanks를 채워준다.
                    Dict_completed_chk = self.cr_pagesinspector(tmpvalue).values()
                    content_dict[keykeys[j+2]] = list(Dict_completed_chk)[1]
            
            # except requests as e:
            #     errorpass = True
            #     print('%s 오류 다음 페이지에서 재접속' % e)

            except ConnectionResetError as e:
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

                #톰캣서버를 활성화 시킨다음에 서버에 접속시키면서 강제로 에러를 발생시켜
                #connectionerror를 누르면 해결이 되는지 확인.
                
                #여기에 재접속 코드 삽입. => 적용취소

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
                    print('오류가 일어난 페이지 처리')
                    #CSS에 등록된 lower page의 개수만큼 loop를 돌며 빈칸을 채움
                    for j in range(startini, endini):
                        tmpvalue = None
                        Dict_completed_chk = self.cr_pagesinspector(tmpvalue, errorpass).values()
                        content_dict[keykeys[j+2]] = list(Dict_completed_chk)[1]
                
                # print('빈 셀을 채운 개수 : ', list(Dict_completed_chk)[0])

            #list에 모든 dictionary type 저장.
            contents_part_list.append(content_dict)
            count_cr += 1
        
        # 2. 하단 페이지 및 뉴스페이지를 크롤링하여 얻은 최종인덱스 숫자 및 완료여부 리턴하고 피클에 저장
        # backupinfo = [len(contents_part_list), errorpass]
        
        # # # 넘어온 백업파일의 리스트가 
        # # if len(backuploaded) >= 5 and backuploaded[4] == Ture: 
        # # if :
        # #     # 모두 완료되었으면 이미 완성된 크롤링이 남긴 백업으로 파악하고 (backuploaded = [0,0,1,0,1,0,1]), 백업 파일을 새롭게 덮어쓰며
        # #     if (backuploaded[4] == True) and (backuploaded[6] == True):
        # #         self.cr_backup(backupinfo, True) #상단페이지 백업
        # #     # 그렇지 않으면 백업하지 않음.
        # #     else:
        # #         pass
        # # # 백업이 없으면, 실행
        # # else:
        # #     self.cr_backup(backupinfo, True) #상단페이지 백업

        
        # self.cr_backup(backupinfo) #하단페이지 백업
        return contents_part_list

    # 하단 페이지의 게시판에서 추출한 뉴스링크 크롤링 함수
    def cr_newspages(self, target, headers, upper_page_list, contents_part_list):
        # contents_part_list를 호출하여 다시 contents_part_list를 return
        # 모든 크롤링이 끝나고 contents_part_list에 news_company 추가

        # 변수
        news = News_company() # 언론사 수집을 위한 인스턴스 생성
        backupinfo = [] #backup을 위한 리스트
        n_contents_part_list = contents_part_list #매개변수를 다시 저장하여 넘겨줄 변수 선언

        print('News Company Analyzing...')
        for i in range(len(contents_part_list)):
            board_link = upper_page_list[1][i] # 게시물 자체 링크(혹시나 그 링크로 다시 돌아가야 될 때 대비(뉴스 컴퍼니 함수에선 현재 비활성화))
            #print(board_link)
            links_in_content = n_contents_part_list[i]['clinks'] # 게시물 내에 
            #print(links_in_content)
            news_company = news.add_news_company(links_in_content, board_link)
            n_contents_part_list[i]['news_company'] = news_company
            #print(contents_part_list[i])
        # backupinfo = [len(n_contents_part_list), errorpass]
        # self.cr_backup(backupinfo) #뉴스백업
        return n_contents_part_list

    # 사용중지
    def crawlingposts(self, target, nsplit, lastpage, cvalues):
        ### 크롤링 시간측정 시작 ####
        start_time = time.time()
        u_time = None
        l_time = None
        
        ### 변수설정
        startpage = 0 #시작 페이지를 생각하고 싶지 않을 때
        upper_page_list = None
        keykeys = list(cvalues.keys())
        keyvalues = list(cvalues.values())
        url = keyvalues[0] # 접속할 주소 및 기타 접속 정보      
        headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36'}
        print('########################')
        print('%s으로 접속중...' % target)
        print('########################')

        #################################
        # 1. upper page - 상단 페이지 실행
        upper_page_list = self.cr_upperpages(target, url, headers, startpage, lastpage, lastpage, keyvalues, start_time)
        u_time = time.time()
        print('It takes %s seconds completing the upper page crawling and the uploading' % (round(u_time - start_time,2)))
        #print(upper_page_list)

        #################################
        # 2. lower page - 하단 페이지 실행
        contents_part_list = self.cr_lowerpages(target, headers, upper_page_list, keykeys, keyvalues)
        l_time = time.time()
        print('It takes %s seconds completing the lower page crawling and the uploading' % (round(l_time - u_time,2)))
        sleep(2)
        #################################
        # 3. 언론사 정보 가져오기
        #print(contents_part_list)
        contents_part_list = self.cr_newspages(target, headers, upper_page_list, contents_part_list)
        print('It takes %s seconds completing the news info crawling and the uploading' % (round(time.time() - l_time,2)))

        ### 크롤링 시간측정 종료 ###
        print("It takes %s seconds crawling these webpages" % (round(time.time() - start_time,2)))
        return (upper_page_list, contents_part_list)

    # 구간 세분화 실행
    def crawlingpostslittle(self, target, nsplit, firstpage, lastpage, cvalues):
        ### 크롤링 시간측정 시작 ####
        start_time = time.time()
        u_time = None
        l_time = None
        
        ### 변수설정
        pageranges = self.splitpages(nsplit, lastpage, firstpage) #시작 및 종료 페이지 구간설정
        upper_page_list = None
        keykeys = list(cvalues.keys())
        keyvalues = list(cvalues.values())
        url = keyvalues[0] # 접속할 주소 및 기타 접속 정보      
        headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36'}
        print('########################')
        print('%s으로 접속중...' % target)
        print('########################')

        # 조정한 페이지 구간 수 만큼 루프
        for i in range(len(pageranges)):
            #################################
            # 1. upper page - 상단 페이지 실행
            upper_page_list = self.cr_upperpages(target, url, headers, pageranges[i][0], pageranges[i][1], lastpage, keyvalues, start_time)
            u_time = time.time()
            print('It takes %s seconds completing the upper page crawling and the uploading' % (round(u_time - start_time,2)))
            #print(upper_page_list)

            #################################
            # 2. lower page - 하단 페이지 실행
            contents_part_list = self.cr_lowerpages(target, headers, upper_page_list, keykeys, keyvalues)
            l_time = time.time()
            print('It takes %s seconds completing the lower page crawling and the uploading' % (round(l_time - u_time,2)))
            sleep(2)
        
            #################################
            # 3. 언론사 정보 가져오기
            #print(contents_part_list)
            contents_part_list = self.cr_newspages(target, headers, upper_page_list, contents_part_list)
            print('It takes %s seconds completing the news info crawling and the uploading' % (round(time.time() - l_time,2)))

            ### 크롤링 시간측정 종료 ###
            print("It takes %s seconds crawling these webpages" % (round(time.time() - start_time,2)))

            #################################
            # 4. insert
            cr = [upper_page_list, contents_part_list]
            insertmgdb = CrwalingDAO()
            insertmgdb.insert(cr)
            