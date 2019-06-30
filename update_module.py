# -*- coding: utf-8 -*-
import time
import MySQLdb
import pymysql
import pymysql.cursors
import pandas as pd
from pymongo import MongoClient
import mongo_proxy
import numpy as np
import pymysql
from sqlalchemy import create_engine
import re
import configparser
import os
import datetime
import email_module

pymysql.install_as_MySQLdb()


def get_raw_data(tablename,  start_date, end_date, conn):

    SQL = "SELECT * FROM " + tablename + \
        " WHERE dates between %(date1)s and %(date2)s"

    param = {"date1": start_date, "date2": end_date}

    # pd.read_sql_query를 통해 df데이터프레임에 JS_IDX 테이블 넣어줌
    df = pd.read_sql_query(SQL, conn, params=param)

    # str -> 시계열 데이터로 변환(시계열 그래프 그리기 위한 준비)
    df['dates'] = pd.to_datetime(df['dates'], format='%Y-%m-%d')

    df2 = df.groupby(['dates'], as_index=False).sum()

    return df2


# ---------------------------------------------------------------


def get_word_filtered_data(where, tablename, conn, start_date, end_date):

    SQL = "SELECT * FROM " + tablename + where
    param = {"date1": start_date, "date2": end_date}
    # pd.read_sql_query를 통해 df데이터프레임에 JS_IDX 테이블 넣어줌
    df = pd.read_sql_query(SQL, conn, params=param)
    # str -> 시계열 데이터로 변환(시계열 그래프 그리기 위한 준비)
    df['dates'] = pd.to_datetime(df['dates'], format='%Y-%m-%d')

    # 날짜 별로 문재인 인덱스 키워드 빈도수 합을 계산한 결과를 담는 df2
    df2 = df.groupby(['dates'], as_index=False).sum()
    df2 = df2.drop(['zscore'], axis=1)

    # print(df2)

    return df2


# ----------------------------------------------------------------------


def get_most_common_words(tablename,  valid_date, conn):

    SQL = "SELECT * FROM " + tablename + \
        " where dates = %(date)s ORDER by freq DESC LIMIT 100 "

    param = {"date": valid_date}
    # pd.read_sql_query를 통해 df데이터프레임에 JS_IDX 테이블 넣어줌
    df = pd.read_sql_query(SQL, conn, params=param)

    return df


def get_object_id(comm_name, date_array):

    object_ids = []
    w_counts = []

    client = mongo_proxy.MongoProxy(
        MongoClient(('mongodb://%s:%s@%s:%s' % (mongodb1_info['username'], mongodb1_info['password'], mongodb1_info['host'], mongodb1_info['port']))))

    db = client[mongodb1_info['database']]

    # 컬렉션 객체 가져오기
    coll = db['mss_info']

    for dates in date_array:

        cursor = coll.find({'dates': dates, 'name': comm_name}).limit(1)
        check_df = []
        for doc in cursor:
            check_df.append(doc)

        object_ids.append(check_df[0]['_id'])
        w_counts.append(check_df[0]['w_count'])

    client.close()
    return [object_ids, w_counts]


def upload_dict(data, valid_date_array, object_ids, comm_name):
    client = mongo_proxy.MongoProxy(
        MongoClient(('mongodb://%s:%s@%s:%s' % (mongodb1_info['username'], mongodb1_info['password'], mongodb1_info['host'], mongodb1_info['port']))))

    db = client[mongodb1_info['database']]

    # 컬렉션 객체 가져오기
    coll = db['mss_info']

    for i in range(len(valid_date_array)):
        dates = valid_date_array[i]

        coll.update({'_id': object_ids[i], 'dates': dates, 'name': comm_name}, {
                    '$set': data[i]})
    client.close()
    print('community upload done')


def do_main_job(valid_date_array, start_date, end_date, tablename, comm_name, object_ids, w_counts, comm_femi_avg):
    st = time.time()
    engine = create_engine(("mysql+mysqldb://%s:%s@%s/%s" % (
        mariadb2_info['username'], mariadb2_info['password'], mariadb2_info['host'], mariadb2_info['database'])), encoding='utf-8')

    rawdate = pd.date_range(start_date, end_date)
    rawdate = list(rawdate.astype('str'))

    # m_count ===============================================
    sql = " where word in ('문재인', '문대통령', '문정부', '문프', '문통','문재앙', '문죄인', '문재해', '문재지변', '문근혜', '문구라', '문벌구', '문구라', '문찐따', '문재인조', '쇼통령', '곡재인', '쩝쩝이', '문쩝쩝', '문보궐', '문변태', '문치매', '문치맥', '문등쉰', '문각기동대', '문혼밥', '문틀딱', '문정은', '문북한', '문쪼다', '문저리','문어벙','문오다리','문고구마',  '문틀러', '문주주의', '문벌레',  '문노스',  '미세문지', '재앙이', '문제아', '문세먼지', '재앙 ','젠틀재인','문바마', '문깨끗','파파미','왕수석','negotiator','달님','문프','명왕','재인리','금괴왕') and dates between %(date1)s and %(date2)s"

    conn = engine.connect()
    df = get_word_filtered_data(sql, tablename, conn, start_date, end_date)
    conn.close()

    dates = list(df.dates.astype('str'))
    freq = list(df.freq)

    m_count_freq = []

    for i in range(len(rawdate)):
        if rawdate[i] not in dates:
            m_count_freq.append(0)
        else:
            idx = dates.index(rawdate[i])
            m_count_freq.append(freq[idx])

    # anti_count =========================================
    sql = " where word in ('문재앙', '문죄인', '문재해', '문재지변',  '문근혜', '문구라', '문벌구', '문구라', '문찐따', '문재인조', '쇼통령', '곡재인', '쩝쩝이', '문쩝쩝', '문보궐', '문변태', '문치매', '문치맥', '문등쉰', '문각기동대', '문혼밥', '문틀딱', '문정은', '문북한', '문쪼다', '문저리','문어벙','문오다리','문고구마', '문틀러', '문주주의', '문벌레',  '문노스',  '미세문지', '재앙이', '문제아', '문세먼지',  '재앙')  and dates between %(date1)s and %(date2)s"

    conn = engine.connect()
    df = get_word_filtered_data(sql, tablename, conn, start_date, end_date)
    conn.close()

    dates = list(df.dates.astype('str'))
    freq = list(df.freq)
    anti_count_freq = []

    for i in range(len(rawdate)):
        if rawdate[i] not in dates:
            anti_count_freq.append(0)
        else:
            idx = dates.index(rawdate[i])
            anti_count_freq.append(freq[idx])

    # 페미 단어 빈도 가져오기 ==========================================
    sql = " where word in ('한남', '페미', '여초', '차별', '여성부', '여가부', '성별', '메갈', '맘충',  '김치', '보지')  and dates between %(date1)s and %(date2)s"

    conn = engine.connect()
    filltered_df = get_word_filtered_data(
        sql, tablename, conn, start_date, end_date)
    conn.close()

    dates = list(filltered_df.dates.astype('str'))
    freq = list(filltered_df.freq)
    femi_count_freq = []

    for i in range(len(rawdate)):
        if rawdate[i] not in dates:
            femi_count_freq.append(0)
        else:
            idx = dates.index(rawdate[i])
            femi_count_freq.append(freq[idx])

    # 애매한 페미 빈도 가져오기
    sql = " where word in ('여성, 여자')  and dates between %(date1)s and %(date2)s"

    conn = engine.connect()
    df = get_word_filtered_data(sql, tablename, conn, start_date, end_date)
    conn.close()

    dates = list(df.dates.astype('str'))
    freq = list(df.freq)
    general_femi_list = []

    for i in range(len(rawdate)):
        if rawdate[i] not in dates:
            general_femi_list.append(0)
        else:
            idx = dates.index(rawdate[i])

            if (freq[idx] > comm_femi_avg):  # 애매한 페미 단어 빈도가 커뮤니티 평균보다 높다면
                general_femi_list.append(
                    (freq[idx] - comm_femi_avg))  # 평균을 빼준 값을 넣어준다
            else:
                general_femi_list.append(0)  # 작다면, 0을 넣어준다

    array_for_community = []
    for i in range(len(valid_date_array)):
        dict_for_date = {}

        dict_for_date['femi_count'] = femi_count_freq[i] + \
            general_femi_list[i]  # 애매한 페미 단어 빈도도 더해준다
        dict_for_date['femi_ratio'] = dict_for_date['femi_count'] / w_counts[i]

        # 문 카운트 업데이트
        dict_for_date['m_count'] = m_count_freq[i]

        # 새로운 문카운트를 가져온 전체 단어로 나눔(지분율 수정)
        dict_for_date['popularity'] = dict_for_date['m_count'] / w_counts[i]

        # 안티 카운트 수정
        dict_for_date['anti_count'] = anti_count_freq[i]

        # 수정된 안티카운트와 문카운트를 나눠줘서 적극거부율 수정

        try:
            dict_for_date['anti_ratio'] = dict_for_date['anti_count'] / \
                dict_for_date['m_count']
        except ZeroDivisionError:
            # 작은 커뮤니티의 경우 m_count가 0일 때가 종종 있음..
            # 어쩔 수 없이 이럴땐 거부율도 0
            dict_for_date['anti_ratio'] = 0

        array_for_community.append(dict_for_date)

    upload_dict(array_for_community, valid_date_array, object_ids, comm_name)

    print(comm_name, 'upload completed', time.time() - st)
    engine.dispose()


# ------함수 실행 -------------------
target_list = [
    {'comm_name': 'ruli', 'table_name': 'RULI', 'comm_femi_avg': 223},
    {'comm_name': 'cook', 'table_name': 'COOK', 'comm_femi_avg': 214},

    {'comm_name': 'ilbe', 'table_name': 'ILBE', 'comm_femi_avg': 27},
    {'comm_name': 'clien', 'table_name': 'CLIEN', 'comm_femi_avg': 310},
    {'comm_name': 'mlbpark', 'table_name': 'MLBPARK', 'comm_femi_avg': 50},
    {'comm_name': 'ygosu', 'table_name': 'YGOSU2', 'comm_femi_avg': 305}
]


# config 파일 읽기
config = configparser.ConfigParser()
config_file = os.path.join(os.path.dirname(__file__), 'config.ini')
config.read(config_file)

mongodb1_info = config['mongoDB']
mariadb2_info = config['MariaDB2']

# 날짜 레인지 설정
yesterday = datetime.datetime.today() - datetime.timedelta(1)
weekago = datetime.datetime.today() - datetime.timedelta(7)

# 날짜 스트링으로 변환
yesterday_string = datetime.datetime.strftime(yesterday, '%Y-%m-%d')
weekago_string = datetime.datetime.strftime(weekago, '%Y-%m-%d')

# 년도 판별을 위해 연도만 따로 추출
yesterday_year_only = datetime.datetime.strftime(yesterday, '%Y')

# date = pd.date_range(weekago_string, yesterday_string)
date = pd.date_range('2019-01-01', '2019-06-29')
yesterday_year_only = '2019'

date_array = list(date.astype('str'))

valid_date_array = date_array
print('main process started')

body = ''  # 이메일 본문
for target in target_list:
    start_time = time.time()
    # try:

    # 날짜 레인지가 올바른지 체크 및 리턴 받은 날짜 레인지 사용
    past_dict = get_object_id(
        target['comm_name'], date_array)
    object_ids = past_dict[0]
    w_counts = past_dict[1]

    print('valid date checked')
    print(target['comm_name'], 'in progress')
    tablename = '%s_%s' % (target['table_name'], yesterday_year_only)
    do_main_job(
        valid_date_array,
        valid_date_array[0], valid_date_array[-1], tablename, target['comm_name'], object_ids, w_counts, target['comm_femi_avg'])

    body += target['comm_name'] + ' / STATUS : SUCCESSFUL / ' + 'Total Time Consumed: ' + \
        str(round(time.time() - start_time, 2)) + \
        '\n'  # 하나의 커뮤니티가 성공하면 본문에 상태 추가

    # except:
    #     print('FAILED!')
    #     body += target['comm_name'] + ' / STATUS : FAILED!' + \
    #         '\n'  # 실패시에도 본문에 실패했다고 추가

email_module.send_email('info clone upload Result ',
                        body)  # 크롤링 하나 끝날 때마다 메일 보내기
