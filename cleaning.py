from ruri_dao import CrwalingDAO
from get_element import getElement
from ruri_connect import ConnectTo
import re
import os
import platform
from configparser import ConfigParser
from datetime import datetime

cd = CrwalingDAO()

result = cd.select('clienforcleaning')
# 원하는 콜렉션에서 데이터 가져오기 #### 이 이분도 설정해야함

gd = getElement(result)
# 가져온 데이터를 항목별로 나누기 
 


# 전처리된 데이터를 담을 리스트 준비(제목, 내용, 댓글말 전처리할 예정)
cno = []
title = []
thumbup = []
cthumbdownl = []
cthumbupl = []
ccontent = []
creplies = []
clinks = [] 
idate = []


# 전처리를 담당할 cleaner 클래스 선언
class cleaner:
    
    def __init__(self, feature, listname, collection):
        self.feature = feature
        self.listname = listname
        self.collection = collection

    # feature : gd.title, gd.content
    def cleaning(self):

        if self.feature == gd.creplies:
            for i in range(len(gd.creplies)):
                cleandList = []
                #print('문자열로 %s' % i," ".join(gd.creplies[i]))
                list2string = '""'.join(gd.creplies[i])
                #print(list2string)
                list2string =re.sub('[\!\?\/\★\♥\$\&\@\%\~\[\]\(\)\{\}\.\,\=/+\-\_\:\;\*\^]*','', list2string)
                list2string =re.sub('[\t\r\n\xa0]*','', list2string)
                cleaned =re.sub('[A-Za-z0-9]*','', list2string)
                cleandList.append(cleaned) 
                creplies.append(cleandList)

        elif self.feature == gd.clinks:
            for i in range(len(gd.clinks)):
                cleandList = []
                list2string = " ".join(gd.clinks[i])
                list2string =re.sub('[\!\?\/\★\♥\$\&\@\%\~\[\]\(\)\{\}\.\,\=/+\-\_\:\;\*\^]*','', list2string)
                list2string =re.sub('[\t\r\n\xa0]*','', list2string)
                cleaned =re.sub('[A-Za-z0-9]*','', list2string)
                cleandList.append(cleaned) 
                clinks.append(cleandList)
        
        #### 82 쿡 시간    #### collection 이름 바꾸어줄 것!!!
        elif self.feature == gd.idate and self.collection == 'cookforcleaning':
            for i in range(len(gd.idate)):
                rawdata = gd.idate[i]
                cleaned =re.sub('[\t\r\n\xa0]*','', rawdata)
                cleaned =re.sub('[작성일:]','', cleaned)
                cleaned =cleaned.lstrip()
                toDatetype = datetime.strptime(cleaned, '%Y-%m-%d %H%M%S')
                idate.append(toDatetype)
                    
        #### ruriweb 시간  #### collection 이름 바꾸어줄 것!!!
        elif self.feature == gd.idate and self.collection == 'ruriforcleaning':
            for i in range(len(gd.idate)):
                rawdata = gd.idate[i]
                cleaned =re.sub('[\(\)]*','', rawdata)
                cleaned =re.sub('[\t\r\n\xa0]*','', cleaned)
                cleaned =cleaned.lstrip()
                toDatetype = datetime.strptime(cleaned, '%Y.%m.%d %H:%M:%S')
                idate.append(toDatetype)
        
        #### ilbe 시간   #### collection 이름 바꾸어줄 것!!!
        elif self.feature == gd.idate and self.collection == 'ilbeforcleaning':
            for i in range(len(gd.idate)):
                rawdata = gd.idate[i]+' 00:00:00'
                toDatetype = datetime.strptime(rawdata , '%Y.%m.%d %H:%M:%S')
                idate.append(toDatetype)
                print(idate)


        elif self.feature == gd.thumbup:
            for i in range(len(gd.thumbup)):
                rawdata = self.feature[i]
                cleaned =re.sub('[\t\r\n\s]*','', rawdata)        
                self.listname.append(cleaned)
        
        elif self.feature == gd.cthumbdownl:
            for i in range(len(gd.cthumbdownl)):
                rawdata = self.feature[i]
                cleaned =re.sub('[\t\r\n\s]*','', rawdata)        
                self.listname.append(cleaned)
        
        elif self.feature == gd.cthumbupl:
            for i in range(len(gd.cthumbupl)):
                rawdata = self.feature[i]
                cleaned =re.sub('[\t\r\n]*','', rawdata)        
                self.listname.append(cleaned)
        
        
        else:  
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
            mongoDict['thumbup'] = thumbup[i]
            mongoDict['thumbdown'] = gd.thumbdown[i]
            mongoDict['ccontent'] = ccontent[i]
            mongoDict['cthumbupl'] = gd.cthumbupl[i]
            mongoDict['cthumbdownl'] = gd.cthumbdownl[i]
            mongoDict['idate'] = idate[i]
            mongoDict['news_company'] = gd.news_company[i]
            mongoDict['clinks'] = clinks[i]
            mongoDict['creplies'] = creplies[i]
            


            doc_id = conn.m_collection.insert_one(mongoDict).inserted_id
            print('no.', i+1, 'inserted id in mongodb : ', doc_id)

        conn.m_client.close()





# ilbe = update_db()
# ilbe.setdbinfo()
# ilbe.insertone()
