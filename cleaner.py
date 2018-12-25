from cleaning import *


##### 전처리하고자하는 콜랙션 이름을 넣으시오
collectionname =  'clienforcleaning'


# 타이틀 전처리
new_title = cleaner(gd.title, title,collectionname)
new_title.cleaning()
#print(title)

# 내용 전처리 
new_ccontent = cleaner(gd.ccontent, ccontent,collectionname)
new_ccontent.cleaning()
#print(ccontent)

# 댓글 전처리 
new_replies = cleaner(gd.creplies, creplies,collectionname)
new_replies.cleaning()
#print(creplies)

# 날짜 전처리 
new_replies = cleaner(gd.idate, idate,collectionname)
new_replies.cleaning()

# 링크 전처리 
new_replies = cleaner(gd.clinks, clinks,collectionname)
new_replies.cleaning()

# upper page 추천수
new_replies = cleaner(gd.thumbup, thumbup,collectionname)
new_replies.cleaning()

# # 추천수
# new_replies = cleaner(gd.cthumbupl, cthumbupl,collectionname)
# new_replies.cleaning()

# # 반대수
# new_replies = cleaner(gd.cthumbdownl, cthumbdownl,collectionname)
# new_replies.cleaning()


# mongoDB 에 입력
collectionname = update_db()   # 전처리하고자하는 collection명을 인스턴스로 바꿀 것
collectionname.setdbinfo()     # collection명.setdbinfo()  
collectionname.insertone()     # collection명.insertone() 
