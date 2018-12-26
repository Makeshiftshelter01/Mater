from ruri_dao import CrwalingDAO
from get_element import getElement
cd = CrwalingDAO()

result = cd.select('clienforcleaning')
# 원하는 콜렉션에서 데이터 가져오기 

gd = getElement(result)
# 가져온 데이터를 항목별로 나누기 


# 아래에 변수 선언 다 해놨으니 이걸 가지고 쓰시면 됩니다~

# 불러오는 법 : gd.컬럼명
# 예) gd.title , gd.ccontent, gd.creplies

# 항목 이름은 현재 크롤러가 db에 저장하는 컬럼명 기준으로 되어있습니다
# 현재 저희 메인 서버 cook 콜렉션은 이 이름대로 되어있지 않으니까
# 한번 싹 지우시고 main에서 크롤링 한번 실행 시키시고 확인해보세요

# 모든 변수는 리스트 형태로 저장이 되어있고
# 한 항목에 여러개가 있는 것들(댓글, 링크 등)은 리스트 안의 각 항목이 다시 리스트로 되어있으니 반복문 처리하시면 됩니다


# print(gd.id)            # object_id (새로 db를 만드는거니 필욘 없겠지만 혹시나..)
# print(gd.no)            # 게시물번호
# print(gd.html)          # 게시글로 가는 링크
#print(gd.title[99])         # 글 제목
# print(gd.thumbup)       # 추천(밖)
# print(gd.thumbdown)     # 비추천(밖)
# #print(gd.date)          # 날짜(밖)
# print(gd.ccontent[1])
# print(gd.ccontent[86])       # 내용(글 내부)
# print(gd.clinks)        # 링크(글 내부)
#print(gd.creplies[99])      # 댓글(글 내부)
print(len(gd.cthumbupl))     # 추천(글 내부)
# print(gd.cthumbdownl)   # 비추천(글 내부) 
# print(gd.idate)         # 날짜(글 내부)
# #print(gd.news_company)  # 언론사(추출)

import re
from datetime import datetime

idate = []
creplies =[]

#for i in range(5):   
rawdata   = "\n\t\t\t\t\t\t\t 2017-05-07 02:00:28\n\t\t\t\t\t\t\t \n\t\t\t\t\t\t\t\t"    
#rawdata = gd.idate[i]
cleaned =re.sub('[\t\r\n\xa0]*','', rawdata)
cleaned =re.sub('수정일 : 2018-06-20 16:57:03','', cleaned)
cleaned =cleaned.lstrip()
cleaned =cleaned[:19]
toDatetype = datetime.strptime(cleaned, '%Y-%m-%d %H:%M:%S')
idate.append(toDatetype)



print(len(idate))
print(idate)
print(cleaned)
print(type(cleaned))
print(type(idate))

