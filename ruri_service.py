import json
import platform
import sys

# config
from ruri_config import Config

# 크롤링
from ruri_crawler import WebCrawler

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
        result = wc.crawlingposts(target, lastpage, ctargetdata) #크롤링 실행 및 결과를 변수에 담음
        return result
