import configparser
import os
from pymongo import MongoClient
import mongo_proxy
import re
from ckonlpy.tag import Twitter
import pickle
import time
import pandas as pd
from threading import Thread
import jpype
import datetime


def comm_date(comm_name, dates_array, comm_name2):

    jpype.attachThreadToJVM()  # 쓰레드 하나당 JVM을 할당하기 위한 메소드

    for dates in dates_array:

        # 날짜 initial check ----------------------------------
        mongodb2_info = config['mongoDB2']
        check_process_client = mongo_proxy.MongoProxy(
            MongoClient(('mongodb://%s:%s' % (mongodb2_info['host'], mongodb2_info['port']))))

        check_process_db = check_process_client[mongodb2_info['database']]

        check_process_coll = check_process_db[comm_name2]

        dash_date = re.sub('\.', '-', dates)
        check_process_cursor = check_process_coll.find(
            {'date': {'$regex': dash_date}}).limit(1)

        check_process_client.close()
        print('checking %s' % (dash_date))

        check_list = []
        for rec in check_process_cursor:
            check_list.append(rec)

        if (len(check_list) > 0):
            print('%s already tokened, proceed to next date' % dates)
            continue

        # 이미 전처리 및 토큰화가 끝난 날이면 여기서 프로세스 종료
        # ---------------------------------------------------------------------
        print('%s not yet tokened, start tokening' % dates)

        mongodb1_info = config['mongoDB']
        client = mongo_proxy.MongoProxy(
            MongoClient(('mongodb://%s:%s@%s:%s' % (mongodb1_info['username'], mongodb1_info['password'], mongodb1_info['host'], mongodb1_info['port']))))

        db = client[mongodb1_info['database']]

        # 컬렉션 객체 가져오기
        comm = db[comm_name]

        cursor = comm.find(
            {'content.idate': {'$regex': dates}}).sort([('_id', -1)])

        client.close()

        idate_with_all = []

        for record in cursor:
            reply_temp = record['content']['creplies']
            content_temp = record['content']['ccontent']
            title_temp = record['ctitle']

            if isinstance(title_temp, list):
                idate_with_all.extend(title_temp)
            else:
                idate_with_all.append(title_temp)

            if isinstance(content_temp, list):
                idate_with_all.extend(content_temp)
            else:
                idate_with_all.append(content_temp)

            if isinstance(reply_temp, list):
                idate_with_all.extend(reply_temp)
            else:
                idate_with_all.append(reply_temp)

        print(dates, '가져오기 완료 ', len(idate_with_all), 'docs fetched')

        # ---------------------------------------------------------------------------------
        # # 전처리 (필요없는 특수기호 영문 숫자 지우기)

        nick_list = ['문재인', '문대통령', '문정부', '문프', '문통', '문재앙', '문제인', '문죄인', '문재해', '문재지변', '화재인', '문근혜', '문구라',
                     '문벌구', '문구라', '문찐따', '문재인조', '쇼통령', '곡재인', '쩝쩝이', '문쩝쩝', '문보궐', '문변태', '문치매', '문치맥',
                     '문등쉰', '문각기동대', '문혼밥', '문틀딱', '문정은', '문북한', '문쪼다', '문저리', '문어벙', '문오다리',
                     '문고구마', '먼저인', '문틀러', '문주주의', '문벌레',  '문노스',  '미세문지', '재앙이', '문제아', '문세먼지', '재앙'
                     '젠틀재인', '문바마', '문깨끗', '파파미', '왕수석', 'negotiator', '달님', '문프', '명왕', '재인리', '금괴왕', '달레반',  '머깨문',  '대깨문',  '달창',  '문팬']

        for nick in nick_list:
            twitter.add_dictionary(nick, 'Noun')

        print(dates, ' 전처리 및 토큰화 시작 ')

        client = mongo_proxy.MongoProxy(
            MongoClient(('mongodb://%s:%s' % (mongodb2_info['host'], mongodb2_info['port']))))

        db = client[mongodb2_info['database']]

        collection = db[comm_name2]

        for i in range(len(idate_with_all)):

            text = idate_with_all[i]
            # 전처리
            text = re.sub('[0-9A-Za-zㅋㅎㄷㅡ]+', ' ', text)
            text = re.sub(
                '[\[\]\.\!\?\/\.\:\-\>\~\@\·\"\"\%\,\(\)\& ]+', ' ', text)
            text = re.sub('[\n\xa0\r]+', ' ', text)
            text = re.sub('[^\x00-\x7F\uAC00-\uD7AF]+', ' ', text)
            text = re.sub('&nbsp;| |\t|\r|\n', ' ', text)

            # 토큰화
            token = twitter.nouns(text)  # 명사만

            dates = re.sub('\.', '-', dates)  # comm2에 넣을 땐 무조건 dash형태로.

            print(dates, i, '/', len(idate_with_all))

            mongoDict = {}
            mongoDict['date'] = dates
            mongoDict['token'] = token
            #mongoDict['link'] = link

            doc_id = collection.insert_one(mongoDict).inserted_id

            print('no.', i+1, 'inserted id in mongodb : ', doc_id)

        client.close()


def do_multi_thread_job(comm_name, date_array, comm_name2):
    # date_array를 쪼개기 위해 나눔
    array_split_section1 = (int(len(date_array) * (1/8)))
    array_split_section2 = (int(len(date_array) * (2/8)))
    array_split_section3 = (int(len(date_array) * (3/8)))
    array_split_section4 = (int(len(date_array) * (4/8)))
    array_split_section5 = (int(len(date_array) * (5/8)))
    array_split_section6 = (int(len(date_array) * (6/8)))
    array_split_section7 = (int(len(date_array) * (7/8)))

    # 쓰레드 선언
    t1 = Thread(target=comm_date,
                args=(comm_name,
                      date_array[0:array_split_section1],
                      comm_name2))

    t2 = Thread(target=comm_date,
                args=(comm_name,
                      date_array[array_split_section1:array_split_section2],
                      comm_name2))

    t3 = Thread(target=comm_date,
                args=(comm_name,
                      date_array[array_split_section2:array_split_section3],
                      comm_name2))
    t4 = Thread(target=comm_date,
                args=(comm_name,
                      date_array[array_split_section3:array_split_section4],
                      comm_name2))

    t5 = Thread(target=comm_date,
                args=(comm_name,
                      date_array[array_split_section4:array_split_section5],
                      comm_name2))
    t6 = Thread(target=comm_date,
                args=(comm_name,
                      date_array[array_split_section5:array_split_section6],
                      comm_name2))
    t7 = Thread(target=comm_date,
                args=(comm_name,
                      date_array[array_split_section6:array_split_section7],
                      comm_name2))

    t8 = Thread(target=comm_date,
                args=(comm_name,
                      date_array[array_split_section7:len(date_array)],
                      comm_name2))

    # 쓰레드 시작
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    t6.start()
    t7.start()
    t8.start()

    # 쓰레드가 끝나면 닫기
    t1.join()
    t2.join()
    t3.join()
    t4.join()
    t5.join()
    t6.join()
    t7.join()
    t8.join()


if __name__ == '__main__':  # 멀티 쓰레드, 멀티 프로세싱에서는 __name__=='__main__' 부분이 꼭 필요하다고 함

    # config 파일 읽기
    config = configparser.ConfigParser()
    config_file = os.path.join(os.path.dirname(__file__), 'config.ini')
    config.read(config_file)

    # 날짜 레인지 설정
    yesterday = datetime.datetime.today() - datetime.timedelta(1)
    weekago = datetime.datetime.today() - datetime.timedelta(14)

    # 날짜 스트링으로 변환
    yesterday_string = datetime.datetime.strftime(yesterday, '%Y-%m-%d')
    weekago_string = datetime.datetime.strftime(weekago, '%Y-%m-%d')

    # 년도 판별을 위해 연도만 따로 추출
    yesterday_year_only = datetime.datetime.strftime(yesterday, '%Y')

    # 사전 선언
    twitter = Twitter()

    st = time.time()

    coll_list = [
        {'name': 'ilbe', 'dot_date': True, 'target': 'realilbe'},
        {'name': 'realclien', 'dot_date': False, 'target': 'realclien'},
        {'name': 'realcook', 'dot_date': False, 'target': 'realcook'},
        {'name': 'realmlbpark', 'dot_date': False, 'target': 'realmlbpark'},
        {'name': 'ruri', 'dot_date': True, 'target': 'realruli'},
        {'name': 'ygosu2', 'dot_date': False, 'target': 'realygosu2'}
    ]

    for coll in coll_list:
        # 날짜 배열 생성
        date = pd.date_range(weekago_string, yesterday_string)
        date_array = list(date.astype('str'))

        # 커뮤니티의 날짜가 -가 아닌 .으로 들어가는 커뮤니티는 치환해서 이용

        if coll['dot_date'] == True:
            date_array2 = []
            for i in range(len(date_array)):
                dot_form = re.sub('\-', '.', date_array[i])
                date_array2.append(dot_form)
            date_array = date_array2
        comm_name = coll['name']
        comm_name2 = ('%s_%s' % (coll['target'], yesterday_year_only))

        # ---------
        print('-------------------------')
        print('Community Name : ', comm_name)
        print(date_array)
        print('target_name', comm_name2)
        print('-------------------------')
        
        do_multi_thread_job(comm_name, date_array, comm_name2)

    et = time.time() - st

    print('Total Time : %s' % et)
