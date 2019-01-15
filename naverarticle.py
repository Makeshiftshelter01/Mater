import requests
import lxml.html
import cssselect
import datetime

def naver_news(lastpage):
    # phase 1------------------------------------------------------------------

    topic_html_list = [] # 메인 주제 파싱용 html 리스트
    # css 
    topic_section_css = 'div.officialeditorial_item'

    # 먼저 각 주제를 구분하기 위해 주제마다 파싱용 html 만 가져온다 
    for i in range(1, int(lastpage)+1):
        url = 'https://news.naver.com/main/opinion/officialEditorial.nhn'
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36'}
        params = {'page' : i}
        res = requests.get(url, headers=headers, params=params)
        html = res.text
        root = lxml.html.fromstring(html)

        for topic in root.cssselect(topic_section_css):
            topic_html_list.append(lxml.html.tostring(topic))

    #phase2 ----------------------------------------------------------------

    topic_list = [] # 메인 주제 제목 리스트
    news_title_list = [] # 사설 제목 리스트
    news_company_list = [] # 해당 사설 언론사 리스트
    news_link_list = [] # 사설 링크 리스트

    # css
    topic_title_css = 'div.officialeditorial_topic h4'
    news_title_css = 'ul.officialeditorial_article li a'
    news_company_css = 'ul.officialeditorial_article li em'

    # 가져온 파싱용 html 하위에 있는 사설들의 제목, 각 사설의 언론사, 각 사설의 링크를 가져온다
    # 리스트의 개수에 맞춰서 주제의 제목도 반복해서 넣어준다  
    for i in range(len(topic_html_list)):
        root = lxml.html.fromstring(topic_html_list[i])
            
        cnt = 0 # 사설의 개수에 맞춰서 주제도 중복해서 넣기 위한 카운트

        # 사설 제목
        for title in root.cssselect(news_title_css):
            news_title_list.append(title.text_content())
            cnt += 1

        # 사설 언론사
        for company in root.cssselect(news_company_css):
            news_company_list.append(company.text_content())

        # 사설 링크(내용 추출용)
        # 링크의 경우 제목과 css는 같고, href만 get해서 가져오면 된다 
        for link in root.cssselect(news_title_css):
            news_link_list.append(link.get('href'))

        topic_title = root.cssselect(topic_title_css)[0]

        for j in range(cnt):
            topic_list.append(topic_title.text_content())

    # phase 3 -------------------------------------------------------------------

    content_list = []
    content_date_list = []

    # css
    content_css = 'div#articleBodyContents'
    content_date_css = 'span.t11'

    # phase2 에서 가져온 링크에 접속해서 해당 사설의 내용 가져오기 

    for i in range(len(news_link_list)):
        inurl = news_link_list[i]
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36'}
        res = requests.get(inurl, headers=headers)
        html = res.text
        root = lxml.html.fromstring(html)

        for content in root.cssselect(content_css):
            content_list.append(content.text_content())

        # 날짜는 변환해서 넣음
        for date in root.cssselect(content_date_css):
            strdate = date.text_content()
            realdate = datetime.datetime.strptime(strdate, '%Y-%m-%d %H:%M')
            content_date_list.append(realdate)

    # phase 4---------------------------------------------------------
    
    # 함수의 리턴을 위한 사전 생성
    # 사전을 담기 위한 리스트
    news_dict_list = []

    for i in range(len(news_title_list)):
        temp = {
            'news_topic' : topic_list[i],
            'news_link' : news_link_list[i],
            'news_title' : news_title_list[i],
            'news_company' : news_company_list[i],
            'news_date' : content_date_list[i],
            'news_content' : content_list[i]
        }

        news_dict_list.append(temp)

    return news_dict_list