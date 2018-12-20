
# 인벤 글 긁기
from selenium import webdriver
from pymongo import MongoClient
import time as t
from time import sleep
import time
import re
from selenium.webdriver.firefox.options import Options


start_time = time.time()


def crawlingposts(self, lastpage, cvalues)::

    options = Options()
    options.set_headless(True)  # newer webdriver versions
    chrome = webdriver.Firefox(options=options, executable_path=r'/home/tester/geckodriver')
    print("Headless Firefox Initialized")


    numbers = []
    tit_links = []
    titles = []
    dates = []
    likes = []
    hates = []
    content = []

    # 몽고에 넣을 사전
    all = {}


    for i in range(sp,ep):

        # 페이지 넘버
        url = 'http://www.inven.co.kr/board/wow/762?sort=PID&iskin=webzine&p=' + str(i)

        chrome.get(url)
        sleep(1)

        print('페이지'+str(i)+'접속완료')

        print('글번호/링크 모두 긁는 중')

        # 한 페이지의 글 번호 모두 저장(공지 제외)
        for no in chrome.find_elements_by_css_selector('tr.ls.tr td.bbsNo'):
            numbers.append(no.text.strip())

        # 그 페이지의 제목에서 링크 가져오기(공지 제외)
        for tit_link in chrome.find_elements_by_css_selector('tr.ls.tr td.bbsSubject a'):
            tit_links.append(tit_link.get_attribute('href'))


    # 가져온 글 링크로 접속 후 내용 가져오기
    for i in range(0, len(numbers)):
        try:
            print(numbers[i], '+' , tit_links[i])

            newurl = tit_links[i]

            chrome.get(newurl)
            print('글 안으로 접속 완료')
            # 슬립
            sleep(0.5)

            # 글 안의 내용 크롤링
            # 예외처리를 위해 append는 제일 마지막에

            #타이틀 긁기
            title= chrome.find_element_by_css_selector('div.articleTitle h1')
            # 날짜
            date = chrome.find_element_by_css_selector('div.articleDate')
            # 추천수
            like = chrome.find_element_by_css_selector('span.reqblue')
            # 비추천수
            hate = chrome.find_element_by_css_selector('span.reqred')

            # --------------다음 3개의 항목은 안에 사전을 만들어서 한번에 넣을 것
            #글 내용
            c = chrome.find_element_by_id('powerbbsContent')
            con = c.text.strip()
            # 링크 가져오기
            lk = []
            for link in chrome.find_elements_by_css_selector('#powerbbsContent a'):
                lk.append(link.get_attribute('href'))
            # 댓글
            comm = []
            for j in chrome.find_elements_by_css_selector('div.commentList1 ul li.row div div span.cmtContentOne'):
                comm.append(j.text.strip())
            print('글 안 내용 모두 긁기 완료' + str(i+1) + '/' + str(len(numbers)))

            # 글 내용, 링크, 댓글
            incontent = {
                'content' : con,
                'link' : lk,
                'reply' : comm
            }
            content.append(incontent)
            titles.append(title.text.strip())
            dates.append(date.text.strip())
            likes.append(like.text.strip())
            hates.append(hate.text.strip())
            print()
            if (i+1) % 30 == 0:
                print("Refresh...............")
                sleep(30)
        except:
            #!!! 문서 수가 많아질수록 간혹 로딩이 안되면서 에러가 뜨는데 예외처리를 하고 넘어가려고 했으나 문제는 남은 문서들도 모두 예외처리가 되어버림
            # 그렇기에 예외처리에서 크롬을 끄고 다시 로딩하게 하려고 했으나 이번에는 전혀 다른 예외가 뜨면서(except로 안걸러짐) 프로그램 강제 종료
            print('에러 발생')
            print('관련 내용은 예외 처리')

            titles.append('Error')
            dates.append('Error')
            likes.append(0)
            hates.append(0)

            con = 'Error'
            lk = []
            comm = []
            incontent = {
                'content': con,
                'link': lk,
                'reply': comm
            }
            content.append(incontent)
            print('예외처리 완료')


    client = MongoClient('mongodb://13.209.74.74:8854')

    # 데이터베이스 객체 가져오기
    db = client.project

    # 컬렉션 객체 가져오기

    inven = db.inven_test


    print('모든 글 긁기 완료')
    print('몽고에 저장 중...')

    # 몽고에 저장
    for q in range(0, len(numbers)):
        all = {'no': numbers[q],
               'html' : tit_links[q],
               'title' : titles[q],
               'date' : dates[q],
               'Content' : content[q],
               'thumb_up' : likes[q],
               'thumb_down' : hates[q]}
        inven.insert_one(all)
    print('몽고 저장 완료')

    chrome.close()
    client.close()


invencrawl(1, 10+1)
end_time = time.time()

# 걸린 총 시간 표시
print(end_time - start_time)