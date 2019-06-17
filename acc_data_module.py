import MySQLdb
from sqlalchemy import create_engine
import pymysql
import pandas as pd
import numpy as np
import re
import configparser
import os
import datetime
import email_module
import time


def check_duplicate_date(comm_name, date_array):

    valid_date = ''
    mariadb2_info = config['MariaDB2']
    engine = create_engine(("mysql+mysqldb://%s:%s@%s/%s" % (
        mariadb2_info['username'], mariadb2_info['password'], mariadb2_info['host'], mariadb2_info['database'])), encoding='utf-8')

    for dates in date_array:
        sql_query = "SELECT dates FROM period_data" + \
            " WHERE dates like %(mydates)s and name like %(name)s"
        param = {'mydates': dates, 'name': comm_name}

        conn = engine.connect()
        check_df = pd.read_sql_query(sql_query, conn, params=param)

        conn.close()

        if (len(check_df) == 0):
            print('community %s, date %s is valid' % (comm_name, dates))
            valid_date = dates
            break
    engine.dispose()

    return valid_date


def acc_data(comm_name, valid_date):
    print('processing... %s' % (comm_name))

    mariadb2_info = config['MariaDB2']
    engine = create_engine(("mysql+mysqldb://%s:%s@%s/%s" % (
        mariadb2_info['username'], mariadb2_info['password'], mariadb2_info['host'], mariadb2_info['database'])), encoding='utf-8')

    conn = engine.connect()
    SQL = 'select * from info where name like %(name)s and `date` >= %(dates)s'
    param = {'name': comm_name, 'dates': valid_date}

    df = pd.read_sql_query(SQL, conn, params=param)

    conn.close()

    df = df.drop(['word1', 'word1_1', 'word2',
                  'word2_1', 'word3', 'word3_1', 'rank'], axis=1)

    df.rename(columns={'date': 'dates'}, inplace=True)
    df['dates'] = pd.to_datetime(df['dates'], format='%Y-%m-%d')

    df['years'] = df['dates'].dt.year
    df['months'] = df['dates'].dt.month
    df['months'] = df['months'].map(lambda x: '{:02}'.format(x))
    df['weeks'] = df['dates'].dt.week

    df['dates'] = df['dates'].astype(str)
    df['years'] = df['years'].astype(str)
    df['months'] = df['months'].astype(str)

    df['weeks'] = df['weeks'].astype(str)

    conn = engine.connect()
    df.to_sql(name='period_data', con=engine,
              if_exists='append', index=False)
    conn.close()

    engine.dispose()
    print('done')


# 날짜 레인지 설정
yesterday = datetime.datetime.today() - datetime.timedelta(1)
weekago = datetime.datetime.today() - datetime.timedelta(7)

# 날짜 스트링으로 변환
yesterday_string = datetime.datetime.strftime(yesterday, '%Y-%m-%d')
weekago_string = datetime.datetime.strftime(weekago, '%Y-%m-%d')

# 년도 판별을 위해 연도만 따로 추출
yesterday_year_only = datetime.datetime.strftime(yesterday, '%Y')

config = configparser.ConfigParser()
config_file = os.path.join(os.path.dirname(__file__), 'config.ini')
config.read(config_file)


target_list = [
    'ilbe', 'clien', 'mlbpark', 'ygosu', 'ruli', 'cook'
]

date = pd.date_range(weekago_string, yesterday_string)


# 파이썬 리스트 형식으로 변환
date_array = list(date.astype('str'))


body = ''  # 이메일 본문
for target in target_list:

    try:
        print('trying %s' % (target))
        valid_date = check_duplicate_date(target, date_array)
        start_time = time.time()
        # 몽고디비 collection 이름
        if valid_date == '':

            print('no valid date, skipping')
            body += target + ' / STATUS : SUCCESSFUL(skipped) / ' + 'Total Time Consumed: ' + \
                str(round(time.time() - start_time, 2)) + \
                '\n'  # 하나의 커뮤니티가 성공하면 본문에 상태 추가
            continue

        acc_data(target, valid_date)

        body += target + ' / STATUS : SUCCESSFUL / ' + 'Total Time Consumed: ' + \
            str(round(time.time() - start_time, 2)) + \
            '\n'  # 하나의 커뮤니티가 성공하면 본문에 상태 추가
    except:
        print('FAILED!')
        body += target + ' / STATUS : FAILED!' + \
            '\n'  # 실패시에도 본문에 실패했다고 추가

email_module.send_email('ACC_DATA_RESULT ', body)  # 크롤링 하나 끝날 때마다 메일 보내기
