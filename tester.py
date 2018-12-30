
# # for i in range(10):
# #     try:
# #         a = 4/0
# #     except ZeroDivisionError as e:
# #         b = 0
# #         while b < 100:
# #             print(1, e)
# #             b += 1
# #     else:
# #         print(a)
# #     finally:
# #         print(1)
# #     print(1)

# # j = 100000
# # for i in range(j):
# #     if j > 10000:
# #         i +=5000
# #         print(i)

# # import math
# # # print(math.ceil(11/2))

# # rg = None #range
# # exup = 153

# # if exup > 100:
# #     rg = math.ceil(exup/5)
# # # 해당 범위만큼 loop
# # for i in range(rg):
# #     if i+1 == rg:
# #         #나머지만큼
# #         for j in range(i*5+1, (i*5+1)+exup % 5):
# #             print(j)
# #     else:  
# #         for j in range(i*5+1, (i+1)*5+1):
# #             print(j)
# #         print('한 번 쉬어가기 그러니까 sleep')

# # import requests

# # res = requests.get('http://www.naver.com')
# # print(res.status_code)

# # import json

# # a = { 'a':'1', 'b':'1'}

# # # with open('add.txt', 'w') as f:
# # # 	json.dump(a, f)
	
# # b = {'c':'3', 'd':'5'}
# # # with open('add.txt', 'a') as f:
# # # 	json.dump(b, f)

# # print(b.values())

# #백업 구상
# #에러가 나면, 접속이 끊기건 끊기지 않건 json파일로 백업을 만듦.
# #상단 페이지, 하단 페이지, 뉴스링크 각 단계별로 데이터를 추가
# #데이터가 파일로 저장된 상태에서는 덮어씌우거나 이어쓰는 방식 밖에 쓸 수 없기 때문에
# #데이터를 호출하여 단계마다 데이터를 추가한 후 다시 저장하는 방식

# import requests
# import json
# from time import sleep
# import pickle
# import os

# # def repeat():
# #     while True:
# #         print(requests.get('http://13.125.221.134:8080/', timeout=1).status_code)
# #         sleep(2)


# # def backup():
# #     try:
# #         # 테스트 톰캣 서버 (에러를 만들기 위해 의도적으로 서버를 끈 상태)
# #         res = requests.get('http://13.125.221.134:8080/')
# #     except requests.ConnectionError:
# #         print('접속 끊겼어!', res.status_code) # 200외 숫자. 실패
# #     else:
# #         print('성공', res.status_code) # 200. 성공
# #     finally:
# #         # 접속이 끊기건 끊기지 않건 json backup 파일을 만든다.
# #         #변수
# #         tmplist = list() #json파일에 여러개의 Dict를 담아야하기 때문에 리스트형식으로 저장한다.
# #         keys = list(b.keys()) #새로담을 정보
# #         values = list(b.values())
# #         bkupfname = 'backup.json'
# #         # if bkupfname 
# #             #파일 존재 유무에 따라 백업을 만들고 지울 것을 결정. (미정)
# #         #파일이 존재할 경우
# #         with open(bkupfname) as f:
# #             data = json.load(f)
# #             for i in range(len(keys)):
# #                 data[keys[i]] = values[i]
# #             tmplist.append(data)
# #         with open(bkupfname, 'w') as f:
# #             json.dump(tmplist, f, indent=4)

# # def backup2():
# #     with open('backup.json', 'w') as f:
# #         json.dump(b, f)

# # # backup2()

# # def crbackup(data, init=False):
# #     container = list() #upper, lower, newslist를 저장할 리스트
# #     loadcontainer = None
# #     bkupdir = 'backup'
# #     bkupfile = 'backup.pickle'
# #     currentPath = os.path.relpath(os.path.dirname(__file__))
# #     bkupdirPath = os.path.join(currentPath, bkupdir)
# #     bkupfilePath = os.path.join(bkupdirPath, bkupfile)
# #     print(os.path.realpath(bkupdirPath))

# #     #(당연히 최초) 백업디렉토리가 없으면, 디렉토리 생성 파일 생성
# #     if os.path.isdir(bkupdirPath) != True:
# #         os.mkdir(bkupdirPath)
# #         container.append(data)
# #         with open(bkupfilePath, 'wb') as f:
# #             pickle.dump(container, f)
# #     #백업디렉토리가 있으면,
# #     else:
# #         #upper page의 경우
# #         if init == True:
# #             container.append(data)
# #             #바로 디렉토리에 데이터 생성 (덮어쓰기)
# #             with open(bkupfilePath, 'wb') as f:
# #                 pickle.dump(container, f)
# #         #그 외의 경우 (lower, newslinks)
# #         else:
# #             try:
# #                 #데이터를 load한 뒤
# #                 with open(bkupfilePath, 'rb') as f:
# #                     loadcontainer = pickle.load(f)
# #                     #새로운 데이터를 저장한 뒤
# #                     loadcontainer.append(data)
                   
                
# #             except FileNotFoundError:
# #                 #에러가 생긴 경우 강제로 True로 변경하여
# #                 #우선 두번째 파일이라도 백업할 수 있도록 함.
# #                 init = True 
# #                 print("load 할 파일을 못 찾았습니다. 우선 백업 없이 진행합니다.")
            
# #             finally:
# #                 if init == True:
# #                     #데이터 저장
# #                     with open(bkupfilePath, 'wb') as f:
# #                         data = pickle.load(f)
# #                         print(type(data))
# #                 else:
# #                      #다시 저장
# #                     with open(bkupfilePath, 'wb') as f:
# #                         pickle.dump(loadcontainer, f)

# # a = [1,2,3]
# # b = [[1],[2],[3,4]]

                
# # backup3(a, True)


# # try:
# #     backup()
# # except NameError:
# #     print('ssss')

# # try:
# #     repeat()
# # except requests.exceptions.ConnectionError as e:
# #     print(e)

# # target = 'clien'
# # i = 1
# # params = {} #페이지 이동을 위한 파라미터
# # if target == 'clien1':
# #     params = {'po' : i-1}
# # else:
# #     params = {'page': i} 
# # print(params)

# testlist = []

# for i in range(12345):
#     testlist.append(i)
# print(len(testlist))

# full_upper_page_list = [1, testlist]

# print('마지막 숫자 확인 :', full_upper_page_list[1][-1])


# def splitlist(full_upper_page_list, nsplit=10):
#     # 상단페이지에서 나온 링크를 지정한 수 대로 나눠 몫과 나머지를 구하고 
#     nlistquotient = int(len(full_upper_page_list[1])/nsplit)
#     nlistremainder = int(len(full_upper_page_list[1])%nsplit)
#     print('몫과 나머지 : ', nlistquotient, nlistremainder)

#     #피클은 인덱스 번호만 저장해서 그 번호만 내준다.

#     # 리스트의 인덱스를 나눔(몫에 1을 추가하여 나머지부분을 적용).
#     for i in range(nsplit+1):
#         nfrom = i*nlistquotient
#         nto = None
#         if i is not nsplit:
#             # 몫인 경우
#             nto = (i+1)*nlistquotient
#         else:
#             # 나머지인 경우
#             nto =  nfrom + nlistremainder
        
#         upper_page_list = full_upper_page_list[1][nfrom:nto]
#         print('시작 : %d, 끝 : %d, 서치할 페이지 구간 : %d' % (nfrom, nto-1, len(upper_page_list)))

# ftestlist(full_upper_page_list, 7)


# backuploaded = [0,0,1,0,1,0,1]

# if (backuploaded[2] == True) and (backuploaded[4] == True) and (backuploaded[6] == True):
#     # self.cr_backup(backupinfo, True) #상단페이지 백업
#     print('성공')
# else:
#     print('실패')

# # 크롤러 클래스

# # html 처리 함수

# # 백업함수

# # 검사함수

# # 상단처리

# # 하단처리

# # 뉴스처리

# # 전체정리
#     #상, 중, 하를 하나로 보고 짧게 끊자.


#     #상단 => 전체를 쪼개서 DB insert
#     #하단 => DB로부터 부분으로 나눠 데이터를 호출하고 upload
#     #뉴스 => DB로부터 

# for i in range(0,1):
#     print(int(5%3))

# def splitpages(nsplit, lastpage, firstpage):
#     # 상단페이지에서 나온 링크를 지정한 수로 쪼개 페이지의 각 구간 값을 구하고
#     indexdiv = list()
#     startpage = firstpage - 1 #첫 번째 페이지는 1페이지가 기본으로 설정되었기 때문에 구간을 정할 때는 1을 빼줌
#     endpage = lastpage
#     pagerange = endpage-startpage+1 #크롤링 할 목표 구간 범위
#     nlistquotient = int(pagerange/nsplit) #구간 값
#     nlistremainder = int(pagerange%nsplit) #구간 값에 못 미친 나머지
#     # print('페이지의 몫과 나머지 : ', nlistquotient, nlistremainder)
    
#     # 몫과 나머지에 따른 구간 보정
#     r = nsplit
#     # 나머지가 0이 아니면 나머지를 적용하여 구간에 1을 더함.
#     if nlistremainder != 0:
#         r = nsplit+1

#     # 구간을 설정.
#     print('구간번호  |   자료수집중   /   전체 (기준 : 페이지)')
#     print('------------------------------------------------')
#     # 예) 구간을 3으로 쪼개면, loop로 돌려야 할 구간은 총 4개
#     for i in range(r):
#         # 시작페이지를 기본 값으로 갖으면 시작페이지를 조절할 수 있다.
#         nfrom = startpage + i*nlistquotient
#         nto = startpage
#         # 구간 구별,
#         if i is not nsplit:
#             nto = nto + (i+1)*nlistquotient # 일반 구간인 경우 (직접 시작페이지를 적용)
#         else:
#             nto = nfrom + nlistremainder # 마지막 구간인 경우 (nfrom에서 시작페이지를 적용)

#         tmp = [nfrom, nto]
#         indexdiv.append(tmp)
        
#         print('   %2d     | %5d  ~ %5d / %5d' % (i+1, nfrom, nto-1, endpage))
#     return indexdiv

# print(splitpages(10, 7000, 15))

import sys
from time import sleep

def progressBar(value, endvalue, bar_length=20):
    percent = float(value) / endvalue
    arrow = '-' * int(round(percent * bar_length)-1) + '>'
    spaces = ' ' * (bar_length - len(arrow))

    sys.stdout.write("\rPercent: [{0}] {1}%".format(arrow + spaces, int(round(percent * 100))))
    if int(round(percent * 100)) == 100:
        print('\n')
    sys.stdout.flush()

for i in range(1, 11):
    progressBar(i, 10)
    sleep(0.3)
    # print(i)