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
      

        for i in range(len(result)):
            self.id.append(result[i]['_id'])
            self.no.append(result[i]['cno'])
            self.html.append(result[i]['clink'])
            self.title.append(result[i]['ctitle'])
            self.thumbup.append(result[i]['cthumbup'])
            self.thumbdown.append(result[i]['cthumbdown'])
          