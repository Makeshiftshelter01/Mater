from ruri_dao import CrwalingDAO
from get_element import getElement
import datetime
import pandas as pd
cd = CrwalingDAO()
from collections import Counter
import konlpy
from konlpy.tag import Okt
import time
import os
import psutil

#result = cd.select('inven_test')
# 원하는 콜렉션에서 데이터 가져오기 
memory = []

process = psutil.Process(os.getpid())   # 메모리 확인용
mem_before = process.memory_info().rss / 1024 / 1024

start_time = time.time()
result = cd.select_first('token_ilbe')
gd = getElement(result)

print(gd.idate[0])

yoil = gd.idate[0].weekday()

if yoil == 0:
    start_date = gd.idate[0]
else: 
    start_date = gd.idate[0] - datetime.timedelta(days=yoil)

# gd = getElement(result)

# df = pd.DataFrame(columns = ['dates', 'texts'])

# #print(df)
# inven_format='%Y-%m-%d %H:%M'

twitter = Okt()

# #print(datetime.datetime(2014, 1, 1))
# start_date = datetime.datetime.strptime(gd.idate[0], inven_format)
# end_date = datetime.datetime.strptime(gd.idate[-1], inven_format)
# total_days = end_date - start_date
# total_days = total_days.days

# period_number = (total_days//7) + 1

# print('총 구간:',period_number)


#------------------------------------------------
from pymongo import MongoClient

client = MongoClient('YOURIP')
#   데이터베이스 객체 가져오기
db = client.YOURDB

# 학생 컬렉션 객체 가져오기
coll = db.YOURCOLL

alldict = []





#-------------------------------------------------





allperiod = []
# alltext = []
wclist = []
postext= []
listcnt = 0
while True:
    #allperiod = []
    alltext = []
    end_date = start_date + datetime.timedelta(days=7)
    print('started',start_date)
    print('ended', end_date)

    rs = cd.select_date_range('token_ilbe', start_date, end_date)
    #gd = getElement(rs)
    
    cnt = len(rs)
    
    if cnt == 0 or cnt == None: # 가져온 행의 갯수가 0이거나 없으면 while문 중지
        break
  
    else:
        timerange = str(start_date)[:10] + '~'+ str(end_date)[:10] # 00:00:00 같은 시간 제거
        # allperiod = timerange
        #allperiod.append(timerange)
        
        #alltext.append([])
        postext.append([])

        for idx in range(len(rs)):
            #print(rs[idx])


            nouns = rs[idx]['tokenized']['T_noun']

            for i in range(len(nouns)):
                if len(nouns[i][0]) >= 2:
                    #alltext[listcnt].append(nouns[i][0])
                    alltext.append(nouns[i][0])
                
                # if gd.creplies[i] != 'fillblanks':
                #     for l in range(len(gd.creplies[i])):
                #         alltext[listcnt].append(gd.creplies[i][l])

        #listcnt += 1
        
        start_date = start_date + datetime.timedelta(days=7)
        mem_after = process.memory_info().rss / 1024 / 1024
        memory.append(mem_after)
        print('메모리', memory)

       
        wc = Counter(alltext)
        #allperiod.append()
        wcmost = wc.most_common(10)
        #print(wcmost)
        #wclist.append(wcmost)
        #print()

        for i in range(len(wcmost)):
            temp = {
                'timerange' : timerange,
                'keyword' : wcmost[i][0],
                'freq' : wcmost[i][1]
            }

#------------------------------------------------------------
            coll.insert_one(temp)
            
            
            #alldict.append(temp)


client.close()


#----------------------------------------------------------------------

        #print(alltext)
print(len(allperiod))
print(len(alltext))

print('끝!')
# allperiod = []
# for i in range(len(gd.id)):
#     alltext= []
#     try:
#         temp_date = datetime.datetime.strptime(gd.idate[i], inven_format)
#         year_start = datetime.date(year = temp_date.year, month=1, day =1)
#         idate = datetime.date(year= temp_date.year, month = temp_date.month, day=temp_date.day)

#         total = idate - year_start
#         total = total.days

#         week_number = total // 7
#         period = str(temp_date.year)+'week'+ str(week_number)
#         allperiod.append(period)
#     except:
#         print('error')
#         pass

# allperiod = set(allperiod)
# allperiod = list(allperiod)
# allperiod.sort()

# print(allperiod)
# print('총 구간(+1):',len(allperiod))

# alltext = []
# for i in range(len(allperiod)):
#     alltext.append([])

# postext = []
# for i in range(len(allperiod)):
#     postext.append([])

# print('토큰화 배열 수', len(postext))
# print('텍스트 배열 수 ',len(alltext))
    
# # 구간에 맞게 글을 alltext에 추가     
# for i in range(len(gd.id)):
    
#     try:
#         temp_date = datetime.datetime.strptime(gd.idate[i], inven_format)
#         year_start = datetime.date(year = temp_date.year, month=1, day =1)
#         idate = datetime.date(year= temp_date.year, month = temp_date.month, day=temp_date.day)

#         total = idate - year_start
#         total = total.days

#         week_number = total // 7
#         period = str(temp_date.year)+'week'+ str(week_number)


#         for j in range(len(allperiod)):
#             if period == allperiod[j]: 
#                 if gd.ccontent[i] != 'fillblanks':
#                     alltext[j].append(gd.ccontent[i])
                
#                 if gd.creplies[i] != 'fillblanks':
#                     for l in range(len(gd.creplies[i])):
#                         alltext[j].append(gd.creplies[i][l])

#     except:
#         print('error')
#         pass



# for i in range(len(alltext)):
#     content = []
#     for j in range(len(alltext[i])):
#         temp = twitter.pos(alltext[i][j])
#         for k in range(len(temp)):
#             content.append(temp[k])
    
#     for l in range(len(content)):
#         if content[l][1] == 'Noun' or content[l][1] == 'Adjective' :
#             if len(content[l][0]) >= 2:
#                 postext[i].append(content[l])


# for i in range(len(allperiod)):
#     wc = Counter(alltext[i])
#     print(allperiod[i])
#     print(wc.most_common(20))
#     print()













end_time = time.time()

print(end_time-start_time)


import pickle
