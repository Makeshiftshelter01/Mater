from pymongo import MongoClient
from ruri_connect import ConnectTo
from ruri_config import Config
from ruri_etc import CrStatus
from time import sleep
import math
import json
import os
import re

import platform

class CrwalingDAO:
    #load db info
    def setdbinfo(self, collection):
        host = "" # linux구분
        if platform.system() != "Linux":
            host = 'host'
        else:
            host = 'lhost'

        # ini파일을 이용해 접속 데이터 읽기
        config = Config()
        data = config.read_info_in_config('mongoDB')
        cnct = ConnectTo(data[host],int(data['port']),data['database'], collection) #인스턴스화
        cnct.MongoDB() #mongoDB 접속, 현재는 mongoDB만 가능하지만 추후 다른 DB도 선택할 수 있도록 변경
        return cnct

    # 한 페이지만큼만 mongodb에 넣어줌.
    def insert_one(self, conn, keys, startini, endini, upper, lower, rgnow, rgend):
        status = CrStatus()
        mongoDict = {}
        for j in range(startini, endini-1):
            # j를 이용해서 키 값을 넣을 때는 url과 head가 0, 1번을 차기하고 있기 때문에 +2를 넣어준다.
            mongoDict[keys[j+2]] = upper[j][rgnow]
        
        # 컨텐츠 추가
        mongoDict['content'] = lower[rgnow]

        doc_id = conn.m_collection.insert_one(mongoDict).inserted_id
        status.progressBar(rgnow+1, rgend, 'inserting data into mongoDB')
        # print('no.', rgnow+1, 'inserted id in mongodb : ', doc_id)

    # 입력수 조절
    def number_adj(self, count, tmpruri, conn, cr, startini, endini):
        #변수
        keys = list(tmpruri.keys())
        values = list(tmpruri.values())
        upper = cr[0]
        lower = cr[1]
        exup = sorted(upper[0]) #자료를 가졌다고 생각하는 값을 선택
        rg = None #range
        
        # 만일 데이터가 count에서 설정한 개수가 넘으면 count/2 개 단위로 잘라 서버의 부담을 줄인다.
        if len(exup) > count:
            rg = math.ceil(len(exup)/int(count/2)) #만일 count가 1000이었다면, 500으로 나눔.
            rgremainder = len(exup) % int(count/2) #나머지 범위를 위한 변수
                
            # 전체를 500개로 나눈 몫의 수만큼 돌리고 500개가 안 되는 나머지는 별도로 돌린다.
            for i in range(rg):
                rgstart = i*int(count/2)
                rgend = (i+1)*int(count/2)
                #몫의 범위만큼 loop를 돌리고 (rg가 최대값이 아닐 경우임)
                if i+1 != rg:
                    for k in range(rgstart, rgend):
                        self.insert_one(conn, keys, startini, endini, upper, lower, k, rgend)
                    print('mongoDB의 부하를 줄이기 위해 0.3초 sleep!!!.')
                    sleep(0.3) #500번 입력 후 0.1초간 쉰다.
                #나머지의 범위만큼 loop를 돌림.
                else:  
                    for k in range(rgstart, rgstart+rgremainder):
                        self.insert_one(conn, keys, startini, endini, upper, lower, k, rgstart+rgremainder)

        # 1000개 이하면,   
        else:
            for i in range(0, len(exup)):
                self.insert_one(conn, keys, startini, endini, upper, lower, i, len(exup))

    def insert(self, cr, collection, startini = 0, endini = 6):
        config = Config()

        # 크롤링 CSS를 가져오려 했는데 설정하는 기능이 없어 우선 ruriweb을 넣어 임시로 만듦.
        tmpruri = config.read_info_in_config('ruriweb')

        conn = self.setdbinfo(collection) #접속값을 받아옴.

        print('titles this crawler has collected till now : ', len(sorted(cr[0][0]))) ### 자료를 얼마나 모았을까? ###

        # 회 당 insert입력 수를 조절해서 몽고DB에 입력
        self.number_adj(500, tmpruri, conn, cr, startini, endini)
        
        conn.m_client.close()

    def insertnews(self, newslist, collection):
        # config = Config()
        # tmpruri = config.read_info_in_config('navernews')
        status = CrStatus()
        conn = self.setdbinfo(collection)
        
        for i in range(len(newslist)):
            conn.m_collection.insert_one(newslist[i])
            status.progressBar(i+1, len(newslist), 'inserting data into mongoDB')
        conn.m_client.close()

        
    def select_last_time(self, collection, target): # 중복 체크용 함수 
        conn = self.setdbinfo(collection) #접속값을 받아옴.
        try:
            cursor = conn.m_collection.find({}).sort([('content.idate', -1), ('clink', -1)]).limit(1)  # 중복 체크를 위해 가장 최신 레코드를 가져온다 
            result = [rs for rs in cursor]
            result = result[0]
            last_time = result['content']['idate']  # 날짜  
            title = result['ctitle'] # 타이틀

            # Mpark 이라면 정규 표현식으로 맨 마지막의 댓글 수 지움
            if target == 'MPark':
                title = re.sub(r'.\[.*\]', '', title)
            elif target == 'ygosu':  # 와이고수는 날짜란에 조회수가 함께 있어서 조회수가 올라가면 매칭이 안됨..
                last_time = re.sub('READ.*','',last_time)

        except IndexError: # 아예 처음 시작해서 아무것도 없을 때 
            last_time = ''
            title = ''
        
        conn.m_client.close()

    
        return last_time, title # 날짜와 제목을 반환
    
    def select(self, collection):
        config = Config()
        data = config.get_coll_dict(collection)
       
        host = "" # linux구분
        if platform.system() != "Linux":
            host = 'host'
        else:
            host = 'lhost'
        cnct = ConnectTo(data[host],int(data['port']),data['database'],data['collection'])
        cnct.MongoDB()
        cursor = cnct.m_collection.find({})
        
        result = []
        for l in cursor:
            result.append(l)
        return result

    