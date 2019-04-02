# import

from ruri_service import Crawling
from ruri_dao import CrwalingDAO
import time
import os

# import logging
# import logging.handlers

# # logger 인스턴스 생성 및 로그레벨 설정
# # logger leverl - DEBUG < INFO < WARNING < ERROR < CRITICAL
# logger = logging.getLogger('my')
# logger.setLevel(logging.INFO)

# # fileHandler와 streamHandler를 생성 (로깅한 정보가 파일과 콘솔로 출력되게 설정)
# # 파일 생성시 디렉토리도 함께 만듦.
# if not(os.path.isdir('./log/')):
#     os.makedirs('./log/')
# # file에 출력
# fileHandler = logging.FileHandler('./log/my.log')
# # console에 출력
# streamHandler = logging.StreamHandler()

# # Handler를 logging에 추가
# logger.addHandler(fileHandler)
# logger.addHandler(streamHandler)

# # logging (level을 info이상으로 했기 때문에 debug는 출력이 안됨.)
# logger.debug('debug')
# logger.info('info')
# logger.warning('warning')
# logger.error('error')
# logger.critical('critical')

# 프로그램 시작 측정
start_time = time.time()

#변수
collection = 'crawl_test'
target = 'inven' #필수
firstpage =1 #옵션
lastpage =5 #필수
# dividePages는 n개로 구간을 나눠 크롤링.
# 만약 구간이 n개로 나눠지지 않을 경우 나머지를 처리하기 위해 1회 더 실행
dividePages = lastpage - firstpage #옵션

# 크롤링
cr = Crawling() #크롤링
# cd = CrwalingDAO() #현재는 mongoDB
# cd.insert(cr.crawling('ruriweb', 10)) #사용금지

cr.crawling(collection, target, lastpage, dividePages, firstpage) # (콜렉션명,목표, 마지막페이지, (옵션 : 원하는 페이지 분할 개수), (옵션 : 첫 번째 페이지))

# 프로그램 종료 측정 및 결과 출력
print('It takes %s seconds completing the crawling and the uploading' % (round(time.time() - start_time,2)))
print('------------------------------------------------------------------------------')