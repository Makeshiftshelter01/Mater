# import

from ruri_service import Crawling
from ruri_dao import CrwalingDAO
import time
import os
import email_module

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



#변수
# collection = 'crawl_test'
# target = 'theqoo' #필수

collection_list = ['fm_test1', 'fm_test1', 'fm_test1']
target_list = ['clien', 'theqoo', 'inven']

firstpage =1 #옵션
lastpage =3 #필수
# dividePages는 n개로 구간을 나눠 크롤링.
# 만약 구간이 n개로 나눠지지 않을 경우 나머지를 처리하기 위해 1회 더 실행
dividePages = lastpage - firstpage #옵션

# 크롤링
cr = Crawling() #크롤링
# cd = CrwalingDAO() #현재는 mongoDB
# cd.insert(cr.crawling('ruriweb', 10)) #사용금지

body = '' # 이메일 본문


for i in range(len(collection_list)):
    try:
        # 프로그램 시작 측정
        start_time = time.time()
        target = target_list[i]
        collection = collection_list[i]
        cr.crawling(collection, target, lastpage, dividePages, firstpage) # (콜렉션명,목표, 마지막페이지, (옵션 : 원하는 페이지 분할 개수), (옵션 : 첫 번째 페이지))

        # 프로그램 종료 측정 및 결과 출력
        print('It takes %s seconds completing the crawling and the uploading' % (round(time.time() - start_time,2)))
        print('------------------------------------------------------------------------------')

        body +=  target + ' / STATUS : SUCCESSFUL / ' +'Total Time Consumed: ' + str(round(time.time() - start_time,2)) + '\n' #하나의 커뮤니티가 성공하면 본문에 상태 추가
            
    except:
        print('FAILED!')
        body +=  target + ' / STATUS : FAILED!' + '\n' #실패시에도 본문에 실패했다고 추가

  
email_module.send_email('Crawling Result ', body) #크롤링 하나 끝날 때마다 메일 보내기 
