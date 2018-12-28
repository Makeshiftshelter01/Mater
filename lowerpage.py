
from ruri_service import Crawling
from ruri_dao import CrwalingDAO
import time
import os
from selenium_crawl_test import selenium_WebCrawler


# 프로그램 시작 측정
start_time = time.time()

# 크롤링
cr = Crawling() #크롤링
cd = CrwalingDAO() #현재는 mongoDB


#sw =selenium_WebCrawler()

#cr.selenium_lowerpage(target = 'inven', coll = 'inven_test')

#혹여나 에러가 나서 모든 항목이 fillblanks일 때를 고치는 함수
cr.selenium_fixerrors(target = 'inven', coll = 'inven_test')


# 프로그램 종료 측정 및 결과 출력
print('It takes %s seconds completing the crawling and the uploading' % (round(time.time() - start_time,2)))