
# for i in range(10):
#     try:
#         a = 4/0
#     except ZeroDivisionError as e:
#         b = 0
#         while b < 100:
#             print(1, e)
#             b += 1
#     else:
#         print(a)
#     finally:
#         print(1)
#     print(1)

# j = 100000
# for i in range(j):
#     if j > 10000:
#         i +=5000
#         print(i)

# import math
# # print(math.ceil(11/2))

# rg = None #range
# exup = 153

# if exup > 100:
#     rg = math.ceil(exup/5)
# # 해당 범위만큼 loop
# for i in range(rg):
#     if i+1 == rg:
#         #나머지만큼
#         for j in range(i*5+1, (i*5+1)+exup % 5):
#             print(j)
#     else:  
#         for j in range(i*5+1, (i+1)*5+1):
#             print(j)
#         print('한 번 쉬어가기 그러니까 sleep')

# import requests

# res = requests.get('http://www.naver.com')
# print(res.status_code)

# import json

a = { 'a':'1', 'b':'1'}

# with open('add.txt', 'w') as f:
# 	json.dump(a, f)
	
b = {'c':'3', 'd':'5'}
# with open('add.txt', 'a') as f:
# 	json.dump(b, f)

print(b.values())

#백업 구상
#에러가 나면, 접속이 끊기건 끊기지 않건 json파일로 백업을 만듦.
#상단 페이지, 하단 페이지, 뉴스링크 각 단계별로 데이터를 추가
#데이터가 파일로 저장된 상태에서는 덮어씌우거나 이어쓰는 방식 밖에 쓸 수 없기 때문에
#데이터를 호출하여 단계마다 데이터를 추가한 후 다시 저장하는 방식

import requests
import json

def backup():
    try:
        # 테스트 톰캣 서버 (에러를 만들기 위해 의도적으로 서버를 끈 상태)
        res = requests.get('http://13.125.221.134:8080/')
    except requests.ConnectionError:
        print('접속 끊겼어!', res.status_code) # 200외 숫자. 실패
    else:
        print('성공', res.status_code) # 200. 성공
    finally:
        # 접속이 끊기건 끊기지 않건 json backup 파일을 만든다.
        #변수
        tmplist = list() #json파일에 여러개의 Dict를 담아야하기 때문에 리스트형식으로 저장한다.
        keys = list(b.keys()) #새로담을 정보
        values = list(b.values())
        bkupfname = 'backup.json'
        # if bkupfname 
            #파일 존재 유무에 따라 백업을 만들고 지울 것을 결정. (미정)
        #파일이 존재할 경우
        with open(bkupfname) as f:
            data = json.load(f)
            for i in range(len(keys)):
                data[keys[i]] = values[i]
            tmplist.append(data)
        with open(bkupfname, 'w') as f:
            json.dump(tmplist, f, indent=4)

try:
    backup()
except NameError:
    print('ssss')