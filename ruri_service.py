import json
import platform
import sys

# config
from ruri_config import Config

# 크롤링
from ruri_crawler import WebCrawler

#셀레니움 크롤링
from selenium_crawl import selenium_WebCrawler

# 크롤링
class Crawling:
    # 기본값 호출
    def setcsstags(self, target):
        #크롤링 가능한 website 존재유무 => 일단 주소를 넘긴다.
        config = Config()
        if config.read_init_config(target) != True:
            sys.exit()
        ctargetdata = config.read_info_in_config(target)
        return ctargetdata

    # 크롤링
    def crawling(self, target, lastpage):
        ##### 세팅 정보
        ctargetdata = self.setcsstags(target) #크롤링 하기 위한 타겟 사이트의 필수 데이터 호출
        
        ##### 실행 및 결과 호출
        wc = WebCrawler() #웹 크롤러 기능 활성화
        result = wc.crawlingposts(lastpage, ctargetdata) #크롤링 실행 및 결과를 변수에 담음
        return result
    
    #셀레니움 크롤링
    def selenium_crawling(self, target, lastpage, pagetype ='page'):
        ##### 세팅 정보
        ctargetdata = self.setcsstags(target) #크롤링 하기 위한 타겟 사이트의 필수 데이터 호출
        
        ##### 실행 및 결과 호출
        sw = selenium_WebCrawler() #웹 크롤러 기능 활성화
        result = sw.selenium_crawlingposts(lastpage, ctargetdata, pagetype) #크롤링 실행 및 결과를 변수에 담음
        return result