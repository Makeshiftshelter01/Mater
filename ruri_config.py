from configparser import ConfigParser
import os

class Config:
    def __init__(self, configFilename = 'config.ini', debug = False):
        
        self.debug = debug

        # 상대경로
        self.filename = os.path.join(os.path.relpath(os.path.dirname(__file__)), configFilename)
        
        #절대경로
        # self.filename = os.path.join(os.path.split(__file__)[0], configFilename)

        self.config = ConfigParser()
        self.parser = self.config.read(self.filename)
        print("Load Config : %s" % self.filename)

        ## 혹시라도 리눅스에 쓸지도 몰라서
        # innerurl = ""
        # if platform.system() != "Linux":
        #     innerurl = './myProject_A_python/mycrawlingCSS.json'
        # else:
        #     innerurl = '/home/pythonuser/project/mycrawlingCSS.json'
        # with open(innerurl) as f:
        #     print('css tag가 설정된 %s파일을 호출합니다 : ' % f.name)
        #     cdata = json.load(f)
        #     return cdata
    

    #입력한 웹사이트를 ini 파일에서 찾기 (webcrawling으로 ini파일의 목표는 고정)
    def read_init_config(self, target, section='webcrawling'):
        config = self.config
        if target in config[section]:
            print('%s를 찾았습니다. 크롤링을 시작합니다. ' % target)
            return True
        else:
            print('%s를 찾지 못했습니다. 크롤링을 중단합니다. ' % target)
            return False

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

# c = Config()
# # b = c.read_init_config('ruriweb')
# d = c.read_info_in_config('ruriweb')
# print(d)