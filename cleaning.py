from ruri_dao import CrwalingDAO
from get_element import getElement
from ruri_connect import ConnectTo
import re
import os
import platform
from configparser import ConfigParser

cd = CrwalingDAO()

result = cd.select('cook3')
# 원하는 콜렉션에서 데이터 가져오기 

gd = getElement(result)
# 가져온 데이터를 항목별로 나누기 
 


# 전처리된 데이터를 담을 리스트 준비(제목, 내용, 댓글말 전처리할 예정)
title = []
ccontent = []
creplies = []


# 전처리를 담당할 cleaner 클래스 선언
class cleaner:
    
    def __init__(self, feature, listname):
        self.feature = feature
        self.listname = listname


    # feature : gd.title, gd.content
    def cleaning(self):

         
        for i in range(len(gd.id)):
            rawdata = self.feature[i]
            cleaned =re.sub('[\!\?\/\★\♥\$\&\@\%\~\[\]\(\)\{\}\.\,\=/+\-\_\:\;\*\^]*','', rawdata)
            cleaned =re.sub('[\t\r\n\xa0]*','', cleaned)
            cleaned =re.sub('[A-Za-z0-9]*','', cleaned)
            self.listname.append(cleaned)
        #print(self.listname)




# 새로운 collection 에 저장하기 위해 기존의 config.ini 대신 config.1.ini 을 생성하여 DB정보를 가져오도록 함
# 새로 만든 이유 : config.ini 에 저장된 collection 이름을 CrwalingDAO()의 select 메서드가 필요로 하기 때문에
# 편의를 위해 새로 만듦

class config_for_update:

    def __init__(self, configFilename = 'config.1.ini', debug = False):
        
        self.debug = debug

        # 상대경로
        self.filename = os.path.join(os.path.relpath(os.path.dirname(__file__)), configFilename)
        
        self.config = ConfigParser()
        self.parser = self.config.read(self.filename)
        print("Load Config : %s" % self.filename)

            # config to dict
    def as_dict(self, config):
        the_dict = {}
        for section in config.sections():
            the_dict[section] = {}
            for key, val in config.items(section):
                the_dict[section][key] = val
        
        return the_dict
        #ini 파일에서 전체 및 특정 자료 찾기
    def read_info_in_config(self, section=None):
        config = self.config
        cdict = self.as_dict(config)
        print(type(cdict[section]))
        if section == None:
            return cdict
        else:
            return cdict[section]

    def get_coll_dict(self, target, db ='mongoDB'):
        config = self.config
        coll_dict = {}
        for key, value in config.items(db):
            coll_dict[key]= value
        coll_dict['collection'] = target
        return coll_dict



# 몽고디비 연결 -> 전처리한 데이터를 새 collection에 담는 작업을 실행할 update_db 클래스 선언
class update_db:
    

    def setdbinfo(self):
        host = "" # linux구분
        if platform.system() != "Linux":
            host = 'host'
        else:
            host = 'lhost'

        # ini파일을 이용해 접속 데이터 읽기
        config = config_for_update()
        data = config.read_info_in_config('mongoDB')
        cnct = ConnectTo(data[host],int(data['port']),data['database'],data['collection']) #인스턴스화
        cnct.MongoDB() #mongoDB 접속, 현재는 mongoDB만 가능하지만 추후 다른 DB도 선택할 수 있도록 변경
        return cnct


    def insertone(self):
        config = config_for_update()

        conn = self.setdbinfo()



        # 몽고DB에 입력
 ### 수정 필요 : 크롤링 시 모든 댓글을 가져오지 않고 하나의 댓글만 가져오는 걸로 보임
 ### 이 부분은 크롤링 문제 해결 후 다시 업데이트 할 예정       
        for i in range(len(gd.id)):
            mongoDict = {}
            # mongoDict['id'] = gd.id[i] #db 입력시 새로운 id 생성되므로 이 부분은 업데이트하지 않음
            mongoDict['no'] = gd.no[i]
            mongoDict['html'] = gd.html[i]
            mongoDict['title'] = title[i]
            mongoDict['thumbup'] = gd.thumbup[i]
            mongoDict['thumbdown'] = gd.thumbdown[i]
            mongoDict['ccontent'] = ccontent[i]
            mongoDict['clinks'] = gd.clinks[i]
            mongoDict['creplie'] = creplies[i]
            mongoDict['cthumbupl'] = gd.cthumbupl[i]
            mongoDict['cthumbdownl'] = gd.cthumbdownl[i]
            mongoDict['idate'] = gd.idate[i]
            mongoDict['news_company'] = gd.news_company[i]
            #print(mongoDict)

            doc_id = conn.m_collection.insert_one(mongoDict).inserted_id
            print('no.', i+1, 'inserted id in mongodb : ', doc_id)

        conn.m_client.close()





# ilbe = update_db()
# ilbe.setdbinfo()
# ilbe.insertone()
