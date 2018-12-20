import json
import platform
import sys

# config
from ruri_config import Config

# 크롤링
from ruri_crawler import WebCrawler

# 크롤링
class Crawling:
    def setcsstags(self, target):
        #크롤링 가능한 website 존재유무 => 일단 주소를 넘긴다.
        config = Config()
        if config.read_init_config(target) != True:
            sys.exit()
        ctargetdata = config.read_info_in_config(target)
        return ctargetdata

    def crawling(self, target, lastpage):
        ctargetdata = self.setcsstags(target)
        # print(len(cdata))
        # for i in cdata:
        #     print(i)
        # print('이것은 : ', cdata['url'])
        
        ## 순서대로 번호, 게시글 링크, 제목, 추천, 게시글 내 링크, 댓글
        wc = WebCrawler()
        result = wc.crawlingposts(lastpage, ctargetdata)
        return result

#test
c = Crawling()
# print(c.setcsstags('ruriweb'))
print(c.crawling('ruriweb', 2))

