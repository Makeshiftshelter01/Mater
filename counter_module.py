# #%%
import time
import MySQLdb
import pandas as pd
import numpy as np
from pymongo import MongoClient
import mongo_proxy
from scipy import stats
from collections import Counter
import pymysql
from sqlalchemy import create_engine
import re
import configparser
import os
import datetime
import email_module

pymysql.install_as_MySQLdb()


def comm_date(comm_name, dates_array, tablename):
    mariadb2_info = config['MariaDB2']
    engine = create_engine(("mysql+mysqldb://%s:%s@%s/%s" % (
        mariadb2_info['username'], mariadb2_info['password'], mariadb2_info['host'], mariadb2_info['database'])), encoding='utf-8')
    # 날짜가 들어있는 리스트에서 하나씩 가져와서 반복문을 돌림
    for dates in dates_array:

        # 기존에 해놓은 것이 있는지 체크

        sql_query = "SELECT * FROM " + tablename + \
            " WHERE dates like %(mydates)s"
        param = {'mydates': dates}

        conn = engine.connect()

        check_df = pd.read_sql_query(sql_query, conn, params=param)
        conn.close()

        print('checking date %s' % (dates))

        # 리스트의 길이가 1이상이라는 것은, 이미 카운트된 데이터가 있다는 것이므로 패스
        if (len(check_df) > 0):
            print('Already Counted %s, proceed to next date' % (dates))
            continue

        # -----------------------------------------------------------
        print('date %s not yet counted, start counting...' % (dates))

        start_time = time.time()

        print(dates, 'is in process')

        # 몽고디비 연결 글라이언트 생성
        mongodb2_info = config['mongoDB2']

        client = mongo_proxy.MongoProxy(
            MongoClient(('mongodb://%s:%s' % (mongodb2_info['host'], mongodb2_info['port']))))

        db = client[mongodb2_info['database']]

        # 원하는 컬랙션을 받아온 후 comm 변수에 담기
        comm = db[comm_name]

        # ilbe와 ruri는 datetime 형식이 .로 연결 / 오류를 필하기 위해서 . -> - 치환
        dates = re.sub('\.', '-', dates)

        cursor = comm.find({'date': {'$regex': dates}}).sort([('_id', -1)])

        client.close()

        time2 = time.time() - start_time

        print("took %0.2f mins to bring cursor using SQL" % ((time2)/60))

        start_time = time.time()

    # 커서로 하나하나 받아온 토큰화된 단어들을 담을 리스트 생성
        text_list = []

        for record in cursor:
            #    print(record['token'])
            text_list.extend(record['token'])

        time2 = time.time() - start_time

        print("took %0.2f mins to put all words in text list" % ((time2)/60))

        # 필터가 필요한 단어를 담을 stopwords 리스트 생성
        stopwords = ['합니다', '겁니다', '그리고', '갑니다', '있습니다', '리플수정', '거예요', '한다는', '보다는', '못한다고', '하는', '이런', '그런', '이건', '놨는데', '어요', '사람', '생각', '지금', '면서', '이나', '그냥', '정도'
                     '이제', '우리', '단일', '이번', '때문', '부터', '후보', '진짜', '정말', '어서', '무슨',
                     '이상', '내용', '다가', '무슨', '어제', '다른', '사실', '시작', '출신', '차이', '면서', '까지',
                     '용지', '까지', '대강', '하소', '새끼', '씨발', '병신', '하하', '답글', '달기', '신고']

        # 필터된 단어를 담을 리스트 생성
        filtered = []
        # 필터 생성
        filtered = [w for w in text_list if w not in stopwords]

        text_list = []

        # 단어 카운팅 시작

        start_time = time.time()

        wc = Counter(filtered)

        filtered = []

        word_list = []       # 단어를 담을 리스트 생성
        freq_list = []      # 빈도를 담을 리스트 생성

        for word, freq in wc.most_common(200000):
            if len(word) >= 2:

                word_list.append(word)
                freq_list.append(freq)

        time2 = time.time() - start_time

        print("took %0.2f mins to count words frequency" % ((time2)/60))
        #print('count words frequency:' ,time.time())

        start_time = time.time()

        # 단어와 키워드를 칼럼으로 가지는 result 데이터프레임 생성
        result = pd.DataFrame(word_list, columns=['word'])

        word_list = []

        result['dates'] = dates       # 날짜를 담을 칼럼에 values 넣기

        result['freq'] = freq_list    # 빈도를 담을 칼럼 values 넣기

        freq_list = []

        # zscore 칼럼 추가하고 계산된 values 넣기

        result['zscore'] = stats.zscore(result.freq)

        time2 = time.time() - start_time

        print("took %0.2f mins to make df" % ((time2)/60))

        # 결과를 Mysql 저장

        conn = engine.connect()

        # 데이터프레임을 sql서버의 테이블로 한번에 넣어주는 코드
        result.to_sql(name=tablename, con=engine,
                      if_exists='append', index=False)

        result = []

        print('date', dates, 'uploaded to mysql')

        print("WorkingTime: {} sec".format(time.time()-start_time))
        conn.close()

    engine.dispose()

# 함수 실행부 -----------------------------------------------------------------------------


# config 파일 읽기
config = configparser.ConfigParser()
config_file = os.path.join(os.path.dirname(__file__), 'config.ini')
config.read(config_file)

# 날짜 레인지 설정
yesterday = datetime.datetime.today() - datetime.timedelta(1)
weekago = datetime.datetime.today() - datetime.timedelta(30)

# 날짜 스트링으로 변환
yesterday_string = datetime.datetime.strftime(yesterday, '%Y-%m-%d')
weekago_string = datetime.datetime.strftime(weekago, '%Y-%m-%d')

# 년도 판별을 위해 연도만 따로 추출
yesterday_year_only = datetime.datetime.strftime(yesterday, '%Y')


target_list = [
    {'comm_name': 'realclien', 'table_name': 'CLIEN'},
    {'comm_name': 'realilbe', 'table_name': 'ILBE'},

    {'comm_name': 'realcook', 'table_name': 'COOK'},
    {'comm_name': 'realmlbpark', 'table_name': 'MLBPARK'},
    {'comm_name': 'realruli', 'table_name': 'RULI'},
    {'comm_name': 'realygosu2', 'table_name': 'YGOSU2'}
]


date = pd.date_range(weekago_string, yesterday_string)


# 파이썬 리스트 형식으로 변환
date_array = list(date.astype('str'))

body = ''  # 이메일 본문
for target in target_list:
    try:
        start_time = time.time()
        # 몽고디비 collection 이름
        comm_name = ('%s_%s' % (target['comm_name'], yesterday_year_only))
        # mysql에 저장할 테이블 이름
        tablename = ('%s_%s' % (target['table_name'], yesterday_year_only))
        comm_date(comm_name, date_array, tablename)

        body += target['comm_name'] + ' / STATUS : SUCCESSFUL / ' + 'Total Time Consumed: ' + \
            str(round(time.time() - start_time, 2)) + \
            '\n'  # 하나의 커뮤니티가 성공하면 본문에 상태 추가
    except:
        print('FAILED!')
        body += target['comm_name'] + ' / STATUS : FAILED!' + \
            '\n'  # 실패시에도 본문에 실패했다고 추가

email_module.send_email('Counting Result ', body)  # 크롤링 하나 끝날 때마다 메일 보내기
