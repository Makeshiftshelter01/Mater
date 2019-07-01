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


def check_duplicate_date(comm_name, date_array):

    valid_date_array = []

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

        if (len(check_df) == 0):
            print('date %s is valid' % (dates))
            valid_date_array.append(dates)

    client.close()
    return valid_date_array


def upload_dict(data):
    client = mongo_proxy.MongoProxy(
        MongoClient(('mongodb://%s:%s@%s:%s' % (mongodb1_info['username'], mongodb1_info['password'], mongodb1_info['host'], mongodb1_info['port']))))

    db = client[mongodb1_info['database']]

    # 컬렉션 객체 가져오기
    coll = db['mss_info']
    coll.insert_many(data)
    client.close()
    print('community upload done')


def do_main_job(valid_date_array, start_date, end_date, tablename, comm_name, comm_femi_avg):
    st = time.time()
    engine = create_engine(("mysql+mysqldb://%s:%s@%s/%s" % (
        mariadb2_info['username'], mariadb2_info['password'], mariadb2_info['host'], mariadb2_info['database'])), encoding='utf-8')

    conn = engine.connect()
    df = get_raw_data(tablename, start_date, end_date, conn)
    conn.close()

    dates = list(df.dates.astype('str'))
    freq = list(df.freq)

    rawdate = pd.date_range(start_date, end_date)
    rawdate = list(rawdate.astype('str'))

    new_freq = []

    for i in range(len(rawdate)):

        if rawdate[i] not in dates:
            new_freq.append(0)
        else:
            idx = dates.index(rawdate[i])
            new_freq.append(freq[idx])

    filltered_df = pd.DataFrame(rawdate, columns=['dates'])
    filltered_df['datetime'] = pd.to_datetime(
        filltered_df['dates'], format='%Y-%m-%d')

    filltered_df['name'] = comm_name
    filltered_df['w_count'] = new_freq

    # 관심도 가져오기  -------------------------------------
    sql = " where word in ('문재인', '문대통령', '문정부', '문프', '문통','문재앙', '문죄인', '문재해', '문재지변', '문근혜', '문구라', '문벌구', '문구라', '문찐따', '문재인조', '쇼통령', '곡재인', '쩝쩝이', '문쩝쩝', '문보궐', '문변태', '문치매', '문치맥', '문등쉰', '문각기동대', '문혼밥', '문틀딱', '문정은', '문북한', '문쪼다', '문저리','문어벙','문오다리','문고구마',  '문틀러', '문주주의', '문벌레',  '문노스',  '미세문지', '재앙이', '문제아', '문세먼지', '재앙 ','젠틀재인','문바마', '문깨끗','파파미','왕수석','negotiator','달님','문프','명왕','재인리','금괴왕', '훠훠', '훳훳') and dates between %(date1)s and %(date2)s"

    conn = engine.connect()
    df = get_word_filtered_data(sql, tablename, conn, start_date, end_date)
    conn.close()

    dates = list(df.dates.astype('str'))
    freq = list(df.freq)

    new_freq = []

    for i in range(len(rawdate)):
        if rawdate[i] not in dates:
            new_freq.append(0)
        else:
            idx = dates.index(rawdate[i])
            new_freq.append(freq[idx])

    filltered_df['m_count'] = new_freq

    filltered_df['popularity'] = filltered_df.m_count/filltered_df.w_count

    # 안티율 가져오기 -------------------------------------
    sql = " where word in ('문재앙', '문죄인', '문재해', '문재지변',  '문근혜', '문구라', '문벌구', '문구라', '문찐따', '문재인조', '쇼통령', '곡재인', '쩝쩝이', '문쩝쩝', '문보궐', '문변태', '문치매', '문치맥', '문등쉰', '문각기동대', '문혼밥', '문틀딱', '문정은', '문북한', '문쪼다', '문저리','문어벙','문오다리','문고구마', '문틀러', '문주주의', '문벌레',  '문노스',  '미세문지', '재앙이', '문제아', '문세먼지',  '재앙',  '훠훠', '훳훳')  and dates between %(date1)s and %(date2)s"

    conn = engine.connect()
    df = get_word_filtered_data(sql, tablename, conn, start_date, end_date)
    conn.close()

    dates = list(df.dates.astype('str'))
    freq = list(df.freq)
    new_freq = []

    for i in range(len(rawdate)):
        if rawdate[i] not in dates:
            new_freq.append(0)
        else:
            idx = dates.index(rawdate[i])
            new_freq.append(freq[idx])

    filltered_df['anti_count'] = new_freq

    filltered_df['anti_ratio'] = filltered_df.anti_count/filltered_df.m_count

    filltered_df[filltered_df.anti_ratio > 1]

    # 페미 빈도 가져오기 ----------------------------------------
    sql = " where word in ('한남', '페미', '여초', '차별', '여성부', '여가부', '성별', '메갈', '맘충',  '김치', '보지')  and dates between %(date1)s and %(date2)s"

    conn = engine.connect()
    df = get_word_filtered_data(sql, tablename, conn, start_date, end_date)
    conn.close()

    dates = list(df.dates.astype('str'))
    freq = list(df.freq)
    new_freq = []

    for i in range(len(rawdate)):
        if rawdate[i] not in dates:
            new_freq.append(0)
        else:
            idx = dates.index(rawdate[i])
            new_freq.append(freq[idx])

    filltered_df['femi_count'] = new_freq

    # 반발지수(문제~~) 가져오기 ================================================
    sql = " where word like '문제%%' and dates between %(date1)s and %(date2)s"

    conn = engine.connect()
    df = get_word_filtered_data(sql, tablename, conn, start_date, end_date)
    conn.close()

    dates = list(df.dates.astype('str'))
    freq = list(df.freq)
    new_freq = []

    for i in range(len(rawdate)):
        if rawdate[i] not in dates:
            new_freq.append(0)
        else:
            idx = dates.index(rawdate[i])
            new_freq.append(freq[idx])

    filltered_df['problem_count'] = new_freq
    filltered_df['problem_ratio'] = filltered_df.problem_count / \
        filltered_df.w_count

    # 애매한 페미 단어 빈도 가져오기 ----------------------------------------
    sql = " where word in ('여성, 여자')  and dates between %(date1)s and %(date2)s"

    conn = engine.connect()
    df = get_word_filtered_data(sql, tablename, conn, start_date, end_date)
    conn.close()

    dates = list(df.dates.astype('str'))
    freq = list(df.freq)
    new_freq = []

    for i in range(len(rawdate)):
        if rawdate[i] not in dates:
            new_freq.append(0)
        else:
            idx = dates.index(rawdate[i])

            if (freq[idx] > comm_femi_avg):  # 애매한 페미 단어 빈도가 커뮤니티 평균보다 높다면
                new_freq.append(
                    (freq[idx] - comm_femi_avg))  # 평균을 빼준 값을 넣어준다
            else:
                new_freq.append(0)  # 작다면, 0을 넣어준다

    filltered_df['general_femi_count'] = new_freq
    filltered_df['femi_count'] = filltered_df.femi_count + \
        filltered_df.general_femi_count  # 애매한 페미 단어 빈도를 합산한 값(없다면 0을 더해줌)을 더해준다
    filltered_df['femi_ratio'] = filltered_df.femi_count / \
        filltered_df.w_count  # 최종적으로 femi_ratio를 구해준다

    array_for_community = []

    for i in range(len(filltered_df)):
        dict_for_date = {}
        dict_for_date['dates'] = str(filltered_df['dates'][i])
        dict_for_date['years'] = dict_for_date['dates'][:4]
        dict_for_date['months'] = dict_for_date['dates'][5:7]
        dict_for_date['weeks'] = int(datetime.datetime.strftime(
            filltered_df['datetime'][i], '%U'))
        dict_for_date['name'] = str(filltered_df['name'][i])
        dict_for_date['w_count'] = int(filltered_df['w_count'][i])
        dict_for_date['m_count'] = int(filltered_df['m_count'][i])
        dict_for_date['anti_count'] = int(filltered_df['anti_count'][i])
        dict_for_date['anti_ratio'] = float(filltered_df['anti_ratio'][i])
        dict_for_date['femi_count'] = int(filltered_df['femi_count'][i])
        dict_for_date['femi_ratio'] = float(filltered_df['femi_ratio'][i])
        dict_for_date['popularity'] = float(filltered_df['popularity'][i])
        dict_for_date['problem_count'] = int(filltered_df['problem_count'][i])
        dict_for_date['problem_ratio'] = float(
            filltered_df['problem_ratio'][i])

        array_for_community.append(dict_for_date)

    # 최다 빈도 가져오기 -------------------------------------
    for i in range(len(array_for_community)):
        main_dict = array_for_community[i]
        starttime = time.time()
        valid_date = main_dict['dates']

        conn = engine.connect()
        common_word_df = get_most_common_words(tablename, valid_date, conn)
        conn.close()

        word_array = []
        for i in range(len(common_word_df)):
            temp_dict = {}
            temp_dict['word'] = common_word_df['word'].astype(str)[i]
            temp_dict['freq'] = int(common_word_df['freq'][i])
            word_array.append(temp_dict)

        main_dict['words'] = word_array
        endtime = time.time()

        print(comm_name, valid_date, endtime-starttime)

    upload_dict(array_for_community)

    print(comm_name, 'upload completed', time.time() - st)
    engine.dispose()


# ------함수 실행 -------------------
target_list = [
    {'comm_name': 'cook', 'table_name': 'COOK', 'comm_femi_avg': 214},
    {'comm_name': 'ruli', 'table_name': 'RULI', 'comm_femi_avg': 223},
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

date = pd.date_range(weekago_string, yesterday_string)
# date = pd.date_range('2017-05-01', '2017-05-31')
# yesterday_year_only = '2017'

date_array = list(date.astype('str'))


print('main process started')

body = ''  # 이메일 본문
for target in target_list:
    start_time = time.time()
    try:
        # 날짜 레인지가 올바른지 체크 및 리턴 받은 날짜 레인지 사용
        valid_date_array = check_duplicate_date(
            target['comm_name'], date_array)

        if (len(valid_date_array) == 0):
            print('All cloned, exiting process')
            continue

        print('valid date checked')
        print(target['comm_name'], 'in progress')
        tablename = '%s_%s' % (target['table_name'], yesterday_year_only)
        do_main_job(
            valid_date_array,
            valid_date_array[0], valid_date_array[-1], tablename, target['comm_name'], target['comm_femi_avg'])

        body += target['comm_name'] + ' / STATUS : SUCCESSFUL / ' + 'Total Time Consumed: ' + \
            str(round(time.time() - start_time, 2)) + \
            '\n'  # 하나의 커뮤니티가 성공하면 본문에 상태 추가

    except:
        print('FAILED!')
        body += target['comm_name'] + ' / STATUS : FAILED!' + \
            '\n'  # 실패시에도 본문에 실패했다고 추가

email_module.send_email('info clone upload Result ',
                        body)  # 크롤링 하나 끝날 때마다 메일 보내기
