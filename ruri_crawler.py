# 접속 및 파싱 #  pull request 테스트!!!!!!!!
import requests
import lxml.html
import cssselect

# 데이터 저장
# from ruri_data import Ruri_Data

# 딜레이
from time import sleep

# 시간측정
import time

class WebCrawler:
    # 링크정보를 꼬리만 가지고 있을 때, 모든 정보를 합침.
    def adjusthtml_pb_tail(self, part_html, head=""):
        
        #full_html = "http://www.82cook.com/entiz/"
        if 'http' not in part_html.get('href'):
            full_html = head + part_html.get('href')
        else:
            full_html = part_html.get('href')
        return full_html
    
    # 페이지를 설정할 수 있게 옵션 선택
    def crawlingposts(self, lastpage, cno, clink, ctitle, cthumb, ccontent, clinks, creplies):
        ### 크롤링 시간측정 시작 ####
        start_time = time.time()

        # 접속할 주소 및 기타 접속 정보
        
        url = 'http://www.82cook.com/entiz/enti.php?bn=15' #루리웹
        # url = 'http://www.ilbe.com/index.php?mid=politics' #일베
        headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36'}
        print('%s 에 접속합니다. : ' % url)

        # 0. 매 페이지의 정보를 저장할 리스트 준비
        ## upper page
        ruri_no_list = [] #글번호
        ruri_title_list = [] #제목
        ruri_html_list = [] #제목링크
        ruri_thumbup_list = [] #추천

        ## lower page
        ruri_contents_part_list = [] #게시글, 게시글 내 링크, 댓글

        # 1. upper page 
        # 모든 페이지에 접속하여 글번호, 제목, 제목링크, 추천 정보를 저장.
        for i in range(1, int(lastpage)):
            params = {'page': i}
            res = requests.get(url, headers=headers, params=params)
            html = res.text
            root = lxml.html.fromstring(html)

            sleep(0.5)

            ## 번호
            # 특이사항 : cssselect를 이용할 때 :not(.클래스이름)을 사용하여 notice class 제거.
            for part_html in root.cssselect(cno):
                ruri_no_list.append(part_html.text_content())

            ## 링크
            for part_html in root.cssselect(clink):
                #특이사항 : 꼬리만 추출되는 경우 감안
                ruri_html_list.append(self.adjusthtml_pb_tail(part_html, 'http://www.82cook.com/entiz/'))
                # ruri_html_list.append(part_html.get('href'))

            ## 제목
            for part_html in root.cssselect(ctitle):
                ruri_title_list.append(part_html.text_content())

            ## 추천수
            # 특이사항 : cssselect를 이용할 때 :not(.클래스이름)을 사용하여 notice class 제거.
            for part_html in root.cssselect(cthumb):
        
                ruri_thumbup_list.append('None')

        print('총 수집한 링크 수 : ', len(ruri_html_list))

        # 2. lower page
        i = 1 #현재 진행사항을 파악하기 위한 변수 설정
        # 수집한 링크로 이동하여 게시글, 게시글 내 링크, 댓글 정보를 저장.
        for innerlink in ruri_html_list:
            print('크롤링 진행사항', i, ' / ', len(ruri_html_list))
            inner_res = requests.get(innerlink, headers=headers)
            inner_html = inner_res.text
            inner_root = lxml.html.fromstring(inner_html)

            #게시물 & 내부링크
            main_content = ""
            ruri_innerlink_list = []
            ruri_replies_list = []

            # 게시글
            for part_html in inner_root.cssselect(ccontent):
                #게시글은 하나 밖에 없기 때문에 리스트가 아닌 일반 변수로 저장
                main_content = part_html.text_content()
            
            # 링크
            ## 해결필요 : 유튜브 등의 주소는?
            for part_html in inner_root.cssselect(clinks):
                # 특이사항 : a태그로 link를 불러왔으나, 그림파일 등 a 태크를 사용하는 경우 blank 저장
                if part_html.get('href') is None:
                    continue
                ruri_innerlink_list.append(part_html.get('href')) #내부링크

            # 댓글
            for part_html in inner_root.cssselect(creplies):
                ruri_replies_list.append(part_html.text_content())

            # 각 게시글의 내용, 링크, 댓글을 저장할 dictionary type 생성
            ruri_content_dict = {
                'content' : main_content,
                'link' : ruri_innerlink_list,
                'reply' : ruri_replies_list
            }

            #list에 모든 dictionary type 저장.
            ruri_contents_part_list.append(ruri_content_dict)
            i += 1

        ### 크롤링 시간측정 종료 ###
        print(" It takes %s seconds crawling these webpages" % (round(time.time() - start_time,2)))
        return (ruri_no_list, ruri_html_list, ruri_title_list, ruri_thumbup_list, ruri_contents_part_list)
    

