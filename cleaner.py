from cleaning import *



# 타이틀 전처리
new_title = cleaner(gd.title, title)
new_title.cleaning()
print(title)

# 내용 전처리 
new_ccontent = cleaner(gd.ccontent, ccontent)
new_ccontent.cleaning()
print(ccontent)
# 댓글 전처리 
new_replies = cleaner(gd.creplies, creplies)
new_replies.cleaning()
print(creplies)

# mongoDB 에 입력
cook3 = update_db()   # 전처리하고자하는 collection명을 인스턴스로 바꿀 것
cook3.setdbinfo()     # collection명.setdbinfo()  
cook3.insertone()     # collection명.insertone() 
