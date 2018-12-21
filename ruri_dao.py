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


    def insertone(self, cr):
        innerurl = os.path.join(os.path.relpath(os.path.dirname(__file__)))
        conn = self.setdbinfo() #접속값을 받아옴.
        ### 자료를 얼마나 모았을까? ###
        print('titles this crawler has collected till now : ', len(cr[2]))
        for i in range(0, len(cr[2])):
            ruri = {
                'no' : cr[0][i],
                'html' : cr[1][i],
                'title' : cr[2][i],
                'thumbup' : cr[3][i],
                'thumbdown' : cr[4][i],
                'date' : cr[5][i],
                'content' : cr[6][i]
            }
            doc_id = conn.m_collection.insert_one(ruri).inserted_id
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

    