from pymongo import MongoClient
from ruri_connect import ConnectTo
from ruri_config import Config
import json
import os
import platform


class getElement:
    def __init__(self, result):
        self.id = []
        self.no = []
        self.html = []
        self.title = []
        self.thumbup = []
        self.thumbdown = []
        #self.date = []
        self.ccontent = []
        self.clinks = []
        self.creplies = []
        self.cthumbupl = []
        self.cthumbdownl = []
        self.idate = []
        #self.news_company = []

        for i in range(len(result)):
            self.id.append(result[i]['_id'])
            self.no.append(result[i]['cno'])
            self.html.append(result[i]['clink'])
            self.title.append(result[i]['ctitle'])
            self.thumbup.append(result[i]['cthumbup'])
            self.thumbdown.append(result[i]['cthumbdown'])
            #self.date.append(result[i]['date'])
            self.ccontent.append(result[i]['content']['ccontent'])
            self.clinks.append(result[i]['content']['clinks'])
            self.creplies.append(result[i]['content']['creplies'])
            self.cthumbupl.append(result[i]['content']['cthumbupl'])
            self.cthumbdownl.append(result[i]['content']['cthumbdownl'])
            self.idate.append(result[i]['content']['idate'])
            #self.news_company.append(result[i]['content']['news_company'])