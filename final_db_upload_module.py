# -*- coding: utf-8 -*-
import time
import MySQLdb
import pymysql
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import matplotlib as mpl
import matplotlib.font_manager as fm
import pymysql
from sqlalchemy import create_engine
import re
import configparser
import os
import datetime
import email_module

pymysql.install_as_MySQLdb()


def get_raw_data(tablename, date1, date2, conn):

    SQL = "SELECT * FROM " + tablename + \
        " WHERE dates between %(date1)s and %(date2)s"

    param = {"date1": date1, "date2": date2}

    # pd.read_sql_query를 통해 df데이터프레임에 JS_IDX 테이블 넣어줌
    df = pd.read_sql_query(SQL, conn, params=param)

    # str -> 시계열 데이터로 변환(시계열 그래프 그리기 위한 준비)
    df['dates'] = pd.to_datetime(df['dates'], format='%Y-%m-%d')

    df2 = df.groupby(['dates'], as_index=False).sum()

    return df2


# ---------------------------------------------------------------


def get_word_filtered_data(sql, tablename, conn, date1, date2):

    SQL = "SELECT * FROM " + tablename + sql
    param = {"date1": date1, "date2": date2}
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


def get_most_common_words(sql, tablename, conn):

    SQL = "SELECT * FROM " + tablename + sql

    # pd.read_sql_query를 통해 df데이터프레임에 JS_IDX 테이블 넣어줌
    df = pd.read_sql_query(SQL, conn)

    return df


def check_duplicate_date(date_array):

    valid_date_array = []

    mariadb2_info = config['MariaDB2']
    engine = create_engine(("mysql+mysqldb://%s:%s@%s/%s" % (
        mariadb2_info['username'], mariadb2_info['password'], mariadb2_info['host'], mariadb2_info['database'])), encoding='utf-8')

    for dates in date_array:
        sql_query = "SELECT * FROM info" + \
            " WHERE date like %(mydates)s"
        param = {'mydates': dates}

        conn = engine.connect()
        check_df = pd.read_sql_query(sql_query, conn, params=param)

        conn.close()
        if (len(check_df) == 0):
            print('date %s is valid' % (dates))
            valid_date_array.append(dates)
    engine.dispose()
    return valid_date_array


def do_main_job(start_date, end_date, tablename, comm_name):

    mariadb2_info = config['MariaDB2']
    engine = create_engine(("mysql+mysqldb://%s:%s@%s/%s" % (
        mariadb2_info['username'], mariadb2_info['password'], mariadb2_info['host'], mariadb2_info['database'])), encoding='utf-8')

    # 첫 데이터 가져오기 -------------------------------------

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

    filltered_df = pd.DataFrame(rawdate, columns=['date'])
    filltered_df['date'] = pd.to_datetime(
        filltered_df['date'], format='%Y-%m-%d')
    filltered_df['name'] = comm_name
    filltered_df['w_count'] = new_freq

    # 관심도 가져오기  -------------------------------------
    sql = " where word in ('문재인', '문대통령', '문정부', '문프', '문통','문재앙', '문제인', '문죄인', '문재해', '문재지변', '화재인', '문근혜', '문구라', '문벌구', '문구라', '문찐따', '문재인조', '쇼통령', '곡재인', '쩝쩝이', '문쩝쩝', '문보궐', '문변태', '문치매', '문치맥', '문등쉰', '문각기동대', '문혼밥', '문틀딱', '문정은', '문북한', '문쪼다', '문저리','문어벙','문오다리','문고구마', '먼저인', '문틀러', '문주주의', '문벌레',  '문노스',  '미세문지', '재앙이', '문제아', '문세먼지', '재앙 ','젠틀재인','문바마', '문깨끗','파파미','왕수석','negotiator','달님','문프','명왕','재인리','금괴왕') and dates between %(date1)s and %(date2)s"

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
    sql = " where word in ('문재앙', '문제인', '문죄인', '문재해', '문재지변', '화재인', '문근혜', '문구라', '문벌구', '문구라', '문찐따', '문재인조', '쇼통령', '곡재인', '쩝쩝이', '문쩝쩝', '문보궐', '문변태', '문치매', '문치맥', '문등쉰', '문각기동대', '문혼밥', '문틀딱', '문정은', '문북한', '문쪼다', '문저리','문어벙','문오다리','문고구마', '먼저인', '문틀러', '문주주의', '문벌레',  '문노스',  '미세문지', '재앙이', '문제아', '문세먼지',  '재앙')  and dates between %(date1)s and %(date2)s"

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

    date = filltered_df['date']

    # 최다 빈도 가져오기 -------------------------------------
    date_array = list(date.astype('str'))

    word = []
    freq = []

    for i in date_array:

        starttime = time.time()
        sql = " where word not in ('질문','경우','게시판','요즘','처럼','가요','수건','감사','고요','어고','색스','가지','오라오라','오늘','려고','오오오오오오오오','쎾쓰','그거','나타','이지','던데','토비','아아','이지','래서','쎆쓰쎆쓰쎆쓰쎆쓰','사람', '생각','지금','면서','이나','그냥','정도','이제','우리','단일','이번','때문','부터','후보','진짜','정말','어서','무슨','포경','거지','이상','내용','다가','무슨','어제','다른','사실','시작', '출신','차이','면서','까지','시발','던데','용지','까지','대강','하소','새끼','씨발','병신','하하','답글', '달기', '신고','존나','섹스','보고','정도','해주','물어','웨더','유용','콘돔','저런','문제','아', '휴', '아이구', '아이쿠', '아이고', '우리', '저희', '따라', '의해', '으로','에게', '뿐이다', '의거하여', '근거하여', '입각하여', '기준으로', '예하면', '예를', '들면', '예를', '들자면', '저', '소인', '소생', '저희', '지말고', '하지마', '하지마라', '다른', '물론', '또한', '그리고', '비길수', '없다', '해서는', '안된다', '뿐만', '아니라', '만이', '만은', '아니다', '막론하고', '관계없이', '그치지', '않다', '그러나', '그런데', '하지만', '든간에', '논하지', '않다', '따지지', '않다', '설사', '비록', '더라도', '아니면', '못하다', '하는', '편이', '낫다', '불문하고', '향하여', '향해서', '향하다', '쪽으로', '틈타', '이용하여', '타다', '오르다', '제외하고', '외에', '밖에', '하여야', '비로소', '한다면', '몰라도', '외에도', '이곳', '여기', '부터', '기점으로', '따라서', '생각이다', '하려고하다', '이리하여', '그리하여', '그렇게', '함으로써', '하지만', '일때', '할때', '앞에서', '중에서', '보는데서', '으로써', '로써', '까지', '해야한다', '일것이다', '반드시', '할줄알다', '할수있다', '할수있어', '임에', '틀림없다', '한다면', '등등', '겨우', '단지', '다만', '할뿐', '딩동', '댕그', '대해서', '대하여', '대하면', '훨씬', '얼마나', '얼마만큼', '얼마큼', '남짓', '얼마간', '약간', '다소', '조금', '다수', '얼마', '지만', '하물며', '또한', '그러나', '그렇지만', '하지만', '이외에도', '대해', '말하자면', '뿐이다', '다음에', '반대로', '말하자면', '이와', '반대로', '바꾸어서', '말하면', '바꾸어서', '한다면', '만약', '그렇지않으면', '까악', '삐걱거리다', '보드득', '비걱거리다', '꽈당', '응당', '해야한다', '가서', '각각', '여러분', '각종', '각자', '제각기', '하도록하다', '그러므로', '그래서', '고로', '까닭에', '하기', '때문에', '거니와', '이지만', '대하여', '관하여', '관한', '과연', '실로', '아니나다를가', '생각한대로', '진짜로', '한적이있다', '하곤하였다', '하하', '허허', '아하', '거바', '어째서', '무엇때문에', '어찌', '하겠는가', '무슨', '어디', '어느곳', '더군다나', '하물며', '더욱이는', '어느때', '언제','이봐', '어이', '여보시오', '흐흐', '헉헉', '헐떡헐떡', '영차', '여차', '어기여차', '끙끙', '아야', '아야', '콸콸', '졸졸', '좍좍', '뚝뚝', '주룩주룩', '우르르', '그래도', '그리고', '바꾸어말하면', '바꾸어말하자면', '혹은', '혹시', '답다', '그에', '따르는', '때가', '되어','지든지', '설령', '가령', '하더라도', '할지라도', '일지라도', '지든지', '몇', '거의', '하마터면', '인젠', '이젠', '된바에야', '된이상', '만큼', '어찌됏든', '그위에', '게다가', '점에서', '비추어', '보아', '고려하면', '하게될것이다', '일것이다', '비교적', '보다더', '비하면', '시키다', '하게하다', '할만하다', '의해서', '연이서', '이어서', '잇따라', '뒤따라', '뒤이어', '결국', '의지하여', '기대여', '통하여', '자마자', '더욱더', '불구하고', '얼마든지', '마음대로', '주저하지', '않고', '즉시', '바로', '당장', '하자마자', '밖에', '안된다', '하면된다', '그래', '그렇지', '요컨대', '다시', '말하자면', '바꿔', '말하면', '구체적으로', '말하자면', '시작하여', '시초에', '이상', '허걱', '바와같이', '해도좋다', '해도된다', '게다가', '더구나', '하물며', '와르르', '펄렁', '동안', '이래', '하고있었다', '이었다', '에서', '로부터', '까지', '예하면', '했어요', '해요', '함께', '같이', '더불어', '마저', '마저도', '양자', '모두', '습니다', '가까스로', '하려고하다', '즈음하여', '다른', '방면으로', '해봐요', '습니까', '했어요', '말할것도', '없고', '무릎쓰고', '개의치않고', '하는것만', '못하다', '하는것이', '낫다', '매', '매번', '들', '모', '어느것', '어느', '로써', '갖고말하자면', '어디', '어느쪽', '어느것', '어느해', '어느', '년도', '해도', '언젠가', '어떤것', '어느것', '저기', '저쪽', '저것', '그때', '그럼', '그러면', '요만한걸', '그래', '그때', '저것만큼', '그저', '이르기까지', '안다', '할', '힘이', '있다', '너', '너희', '당신', '어찌', '설마', '차라리', '할지언정', '할지라도', '할망정', '할지언정', '구토하다', '게우다', '토하다', '메쓰겁다', '옆사람', '퉤', '쳇', '의거하여', '근거하여', '의해', '따라', '힘입어', '그', '다음', '버금', '두번째로', '기타', '첫번째로', '나머지는', '그중에서', '견지에서', '형식으로', '쓰여', '입장에서', '위해서', '단지', '의해되다', '하도록시키다', '뿐만아니라', '반대로', '전후', '전자', '앞의것', '잠시', '잠깐', '하면서', '그렇지만', '다음에', '그러한즉', '그런즉', '남들', '아무거나', '어찌하든지', '같다', '비슷하다', '예컨대', '이럴정도로', '어떻게', '만약', '만일', '위에서', '서술한바와같이', '듯하다', '하지', '않는다면', '만약에', '무엇', '무슨', '어느', '어떤', '아래윗', '조차', '한데', '그럼에도', '불구하고', '여전히', '심지어', '까지도', '조차도', '하지', '않도록', '않기', '위하여', '때', '시각', '무렵', '시간', '동안', '어때', '어떠한', '하여금', '네', '예', '우선', '누구', '누가', '알겠는가', '아무도', '줄은모른다', '줄은', '몰랏다', '하는', '김에', '겸사겸사', '하는바', '그런', '까닭에', '이유는', '그러니', '그러니까', '때문에', '너희', '그들', '너희들', '타인', '것', '것들', '너', '위하여', '공동으로', '동시에', '하기', '위하여', '어찌하여', '무엇때문에', '붕붕', '윙윙', '나', '우리', '엉엉', '휘익', '윙윙', '오호', '아하', '어쨋든', '못하다', '하기보다는', '차라리', '하는', '편이', '낫다', '흐흐', '놀라다', '상대적으로', '말하자면', '마치', '아니라면', '그렇지', '않으면', '그렇지', '않다면', '그러면', '아니었다면', '하든지', '아니면', '이라면', '좋아', '알았어', '하는것도', '그만이다', '어쩔수', '없다', '하나', '일반적으로', '일단', '한켠으로는', '오자마자', '이렇게되면', '이와같다면', '전부', '한마디', '한항목', '근거로', '하기에', '아울러', '하지', '않도록', '않기', '위해서', '이르기까지', '되다', '인하여', '까닭으로', '이유만으로', '이로', '인하여', '그래서', '이', '때문에', '그러므로', '그런', '까닭에', '있다', '결론을', '있다', '으로', '인하여', '어떤것', '관계가', '있다', '관련이', '연관되다', '어떤것들', '에', '대해', '이리하여', '그리하여', '여부', '하기보다는', '하느니', '하면', '할수록', '운운', '이러이러하다', '하구나', '하도다', '다시말하면', '다음으로', '달려', '있다', '우리', '우리들', '오히려', '하기는한데', '어떻게', '어떻해', '어찌됏어', '어때', '어째서', '본대로', '이쪽', '여기', '이것', '이번', '이렇게말하자면', '이런', '이러한', '이와', '같은', '요만큼', '요만한', '얼마', '되는', '이만큼', '정도의', '이렇게', '많은', '것', '이와', '같다', '이때', '이렇구나', '것과', '같이', '끼익', '삐걱', '따위', '같은', '사람들', '부류의', '사람들', '왜냐하면', '중의하나', '오직', '오로지', '에', '한하다', '하기만', '하면', '도착하다', '까지', '미치다', '도달하다', '정도에', '이르다', '지경이다', '결과에', '이르다', '관해서는', '여러분', '하고', '있다', '혼자', '자기', '자기집', '자신', '우에', '종합한것과같이', '대로', '하다', '으로서', '그만이다', '따름이다', '탕탕', '쾅쾅', '둥둥', '봐', '봐라', '아이야', '아니', '와아', '아이', '참나', '이천육', '이천칠', '이천팔', '이천구', '하나', '다섯', '여섯', '일곱', '여덟', '아홉', '식빵', '간나새끼', '개간나', '쌍간나', '종간나', '좆간나', '걸레', '창녀', '갈보', '개년', '개돼지', '개새끼', '개소리', '개나리', '개씨발', '씨발', '개좆', '개지랄', '개족새', '개차반', '거렁뱅이', '고자', '광년', '광녀', '거지', '급식충', '그지깽깽이', '꺼벙이', '꼬걸', '꼬붕', '꼰대', '꼴통', '노슬아치', '논다니', '느개비', '느금마', '노답', '닥쳐', '돼지', '돼지새끼', '되놈', '등신', '꼬붕', '땡중', '땡추', '또라이', '돌아이', '똘추', '똥개', '레기', '루저', '쓰레기', '망나니', '맘충', '매국노', '머저리', '먹사', '멍청도', '무뇌', '미제', '미치광이', '미친개', '보슬아치', '보지', '닥터', '바보', '멍청이', '해삼', '멍게', '말미잘', '병신', '변태', '버러지', '변태새끼', '보슬', '보전깨', '보지년', '보추', '불알', '부랄', '불한당', '븅딱', '빌어먹을', '빠가', '빠돼썅', '빨통', '뻐큐', '삐꾸', '빡대가리', '상폐녀', '새끼', '십장생', '싸가지', '싹', '싸이코', '쌍욕', '썅', '씨팔', '쌞', '씨발놈', '씨발년', '씨발새끼', '씨방새', '씨방놈', '싸벙짭새', '씨부랄', '시부랄', '시브랄', '씨브랄', '씹새끼', '씹새', '씹창', '씹년', '씹치남', '씹치녀', '쑤비', '아다', '아가리', '애자', '애비충', '양놈', '양아치', '언년이', '얼간이', '에라이', '씨발', '엠창', '니미씨발', '엄창', '엠창인생', '열폭', '염병', '옘병', '엿먹다', '오랑캐', '오유충', '왜놈', '우라질', '육변기', '육갑떨다', '육갑하다', '육시럴', '니애미', '애비', '애벌레', '애미', '인간쓰레기', '인간말종', '인조새', '자지', '잡놈', '잡종', '저능아', '정박아', '정신병자', '제기랄', '젠장', '조센징', '졸라', '종간나새끼', '좆간나새끼', '좆', '좆같다', '좆까', '좆나', '존다', '좆도', '아닌', '좆망', '좆물', '좆밥', '좆만이', '존만이', '좆만한게', '좆대가리', '귀두', '좆심', '좆집', '쥐새끼', '즐', '지기미', '지잘', '짭새', '짱깨', '장궤', '쩌리', '쪼다', '쪽발이', '쪽바리', '쫄보', '찌랭이', '찐따', '찐찌버거', '찌질이', '버러지', '거렁뱅이', '호모', '창녀', '창놈', '천치', '춍', '코흘리개', '통구이', '트롤', '파쇼', '폐인', '폐녀자', '피싸개', '한남', '한남충', '허접', '호구', '호로', '후레자식', '홍어', '화냥년', '후장', '후빨', '흑돼지') and dates = '" + i + "' ORDER by freq DESC LIMIT 3"

        conn = engine.connect()
        res = get_most_common_words(sql, tablename, conn)
        conn.close()

        word.append(list(res.word.astype('str')))
        # date.append(list(df.dates.astype('str')))
        freq.append(list(res.freq.astype('int')))

        endtime = time.time()

        print(i, endtime-starttime)

    word1 = []
    word2 = []
    word3 = []

    for i in range(len(word)):
        word1.append(word[i][0])
        word2.append(word[i][1])
        word3.append(word[i][2])

    freq1 = []
    freq2 = []
    freq3 = []

    for i in range(len(word)):
        freq1.append(freq[i][0])
        freq2.append(freq[i][1])
        freq3.append(freq[i][2])

    final_df = filltered_df

    final_df['word1'] = word1
    final_df['word1_1'] = freq1
    final_df['word2'] = word2
    final_df['word2_1'] = freq2
    final_df['word3'] = word3
    final_df['word3_1'] = freq3

    engine.dispose()
    return final_df


def upload_all_comm(final_df_obj):

    mlbpark = final_df_obj['mlbpark']
    ygosu = final_df_obj['ygosu']
    ilbe = final_df_obj['ilbe']
    cook = final_df_obj['cook']
    ruli = final_df_obj['ruli']
    clien = final_df_obj['clien']

    df = pd.concat([mlbpark, ygosu, ilbe, cook, ruli, clien])

    df = df.sort_values(by=['date'])
    # print(df)
    # df_test = df.drop(['Unnamed: 0'], axis=1)
    df_test = df
    date = list(df.date.unique())

    rank = []
    for i in date:

        anti_ratio = df.anti_ratio[df.date == i]
        rank.extend(anti_ratio.rank(ascending=False))

    df_test['rank'] = rank

    # 날짜를 스트링으로 변경
    df_test['date'] = df_test['date'].astype(str)

    mariadb2_info = config['MariaDB2']
    engine = create_engine(("mysql+mysqldb://%s:%s@%s/%s" % (
        mariadb2_info['username'], mariadb2_info['password'], mariadb2_info['host'], mariadb2_info['database'])), encoding='utf-8')
    conn = engine.connect()
    df_test.to_sql(name='info', con=engine, if_exists='append', index=False)
    conn.close()
    engine.dispose()


# ------함수 실행 -------------------
target_list = [
    {'comm_name': 'clien', 'table_name': 'CLIEN'},
    {'comm_name': 'ilbe', 'table_name': 'ILBE'},

    {'comm_name': 'cook', 'table_name': 'COOK'},
    {'comm_name': 'mlbpark', 'table_name': 'MLBPARK'},
    {'comm_name': 'ruli', 'table_name': 'RULI'},
    {'comm_name': 'ygosu', 'table_name': 'YGOSU2'}
]


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

# date = pd.date_range(weekago_string, yesterday_string)
date = pd.date_range('2019-04-06', '2019-05-28')


date_array = list(date.astype('str'))

# 날짜 레인지가 올바른지 체크 및 리턴 받은 날짜 레인지 사용
valid_date_array = check_duplicate_date(date_array)

print('valid date checked')

final_df_obj = {

}

print('main process started')

for target in target_list:
    print(target['comm_name'], 'in progress')
    tablename = '%s_%s' % (target['table_name'], yesterday_year_only)
    final_df = do_main_job(
        valid_date_array[0], valid_date_array[-1], tablename, target['comm_name'])

    final_df_obj[target['comm_name']] = final_df


upload_all_comm(final_df_obj)

email_module.send_email('Final DB upload Result ',
                        'All successful')  # 크롤링 하나 끝날 때마다 메일 보내기
