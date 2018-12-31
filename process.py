from ruri_dao import CrwalingDAO
from get_element import getElement
cd = CrwalingDAO()

result = cd.select('cook')
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


# 잘 가져왔는지 확인을 위해 모든 항목의 길이(len) 검사
print(len(gd.id))
print(len(gd.no))
print(len(gd.html))
print(len(gd.title))
print(len(gd.thumbup))
print(len(gd.thumbdown))
#print(len(gd.date)) # 인서트 할 때 문제인건지 현재 이 항목이 없음 (무시)
print(len(gd.ccontent))
print(len(gd.clinks))
print(len(gd.creplies))
print(len(gd.cthumbupl))
print(len(gd.cthumbdownl))
print(len(gd.idate))
#print(len(gd.news_company)) # 몽고에 현재 언론사 항목이 있는 레코드가 있고 없는 레코드가 섞여있어서 불러올 때 에러, 한번 싹 밀고 다시 크롤링하면 문제없이 불러질 듯
# 
#  # 모든 항목이 길이가 같음


# 항목별 변수 목록
# 주석 풀고 프린트 해보셔서 확인해보세요~
 
# print(gd.id)            # object_id (새로 db를 만드는거니 필욘 없겠지만 혹시나..)
# print(gd.no)            # 게시물번호
# print(gd.html)          # 게시글로 가는 링크
# print(gd.title)         # 글 제목
# print(gd.thumbup)       # 추천(밖)
# print(gd.thumbdown)     # 비추천(밖)
# print(gd.date)          # 날짜(밖)
# print(gd.ccontent)      # 내용(글 내부)
# print(gd.clinks)        # 링크(글 내부)
# print(gd.creplies)      # 댓글(글 내부)
# print(gd.cthumbupl)     # 추천(글 내부)
# print(gd.cthumbdownl)   # 비추천(글 내부) 
# print(gd.idate)         # 날짜(글 내부)
# print(gd.news_company)  # 언론사(추출)
