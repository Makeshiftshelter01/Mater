from pymongo import MongoClient
from ruri_connect import ConnectTo
from ruri_config import Config
import json
import os

import platform

class CrwalingDAO:
    #load db info
    def setdbinfo(self):
        host = "" # linux구분
        if platform.system() != "Linux":
            host = 'host'
        else:
            host = 'lhost'

        # ini파일을 이용해 접속 데이터 읽기
        config = Config()
        data = config.read_info_in_config('mongoDB')
        cnct = ConnectTo(data[host],int(data['port']),data['database'],data['collection']) #인스턴스화
        cnct.MongoDB() #mongoDB 접속, 현재는 mongoDB만 가능하지만 추후 다른 DB도 선택할 수 있도록 변경
        return cnct


    def insertone(self, cr, startini = 0, endini = 6):
        config = Config()

        # 크롤링 CSS를 가져오려 했는데 설정하는 기능이 없어 우선 ruriweb을 넣어 임시로 만듦.
        tmpruri = config.read_info_in_config('ruriweb')
        keys = list(tmpruri.keys())
        values = list(tmpruri.values())

        conn = self.setdbinfo() #접속값을 받아옴.
        upper = cr[0]
        lower = cr[1]
        exup = sorted(upper[0])
        
        print('titles this crawler has collected till now : ', len(exup)) ### 자료를 얼마나 모았을까? ###

        # 몽고DB에 입력
        for i in range(0, len(exup)):
            mongoDict = {}
            for j in range(startini, endini-1):
                # j를 이용해서 키 값을 넣을 때는 url과 head가 0, 1번을 차기하고 있기 때문에 +2를 넣어준다.
                mongoDict[keys[j+2]] = upper[j][i]
            
            # 컨텐츠 추가
            mongoDict['content'] = lower[i]

            doc_id = conn.m_collection.insert_one(mongoDict).inserted_id
            print('no.', i+1, 'inserted id in mongodb : ', doc_id)

        conn.m_client.close()

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

    