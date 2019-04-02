import re
import json
import requests
import lxml.html


def get_ygosu_replies(link):
    try:
        # 리플 정보를 가지고 있는 스크립트를 가져오기 위해 get 리퀘스트부터 먼저 실행
        res = requests.get(link)
        html = res.text
        root = lxml.html.fromstring(html)
        reply_info = ''

        for p in root.cssselect('script'):  # 여기에 리플 정보가 있음
            if 'var reply_info_str' in p.text_content():
                reply_info = p.text_content()

        reply_info = re.findall("reply_info_str=.*\'", reply_info)[0]
        reply_info = re.sub('reply_info_str=', '', reply_info)

        ygosu_headers = {
            'Origin': 'https://www.ygosu.com',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Connection': 'keep-alive',
            'X-Requested-With': 'XMLHttpRequest',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9,ko;q=0.8,la;q=0.7'
        }

        res = requests.post('https://www.ygosu.com/reply/reply_list.yg',
                            headers=ygosu_headers, data={'reply_info': reply_info})
        test_response = res.text

        jj = json.loads(test_response)

        root = lxml.html.fromstring(jj['html'])

        ygosu_replies = []
        for p in root.cssselect('td.comment'):
            ygosu_replies.append(p.text_content())

        if ygosu_replies == []:
            ygosu_replies = ''
        elif len(ygosu_replies) == 1:
            ygosu_replies = ygosu_replies[0]
    except:
        ygosu_replies = ''
    return ygosu_replies

def get_clien_replies(link):
    try:
        board_number = re.sub(r'\?.*','',link)
        board_number= re.sub('.*park/','', board_number)

        comment_link = 'https://www.clien.net/service/board/park/'+board_number+ '/comment'
       
        # 클리앙은 겟 방식으로 
        res = requests.get(comment_link)
        html= res.text
        root = lxml.html.fromstring(html)
        
        # 리플 담을 
        clien_replies = []
        for p in root.cssselect('div.comment_view'):
            clien_replies.append(p.text_content())

        if clien_replies == []:
            clien_replies = ''
        elif len(clien_replies) == 1:
            clien_replies = clien_replies[0]
    except:
        clien_replies = ''
    return clien_replies

def get_theqoo_replies(link):
    try:
        document_srl = re.sub('.*document_srl=','', link)

        headers = {'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Content-Type': 'application/json; charset=UTF-8',
        'Origin': 'https://theqoo.net',
        'Referer': link,
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest'}

        data = 'act=dispBoardContentCommentListTheqoo&document_srl='+document_srl+'&cpage=0'

        res = requests.post('https://theqoo.net/index.php', headers=headers,data=data)
        html = res.text
        jj = json.loads(html[1:]) # 숨어있는 BOM 문자열 때문에 일반적으로 파싱이 안되므로 첫번째 문자를 자르고 파싱 

        theqoo_replies = []
        for comment in jj['comment_list']:
            theqoo_replies.append(comment['content'])

        if theqoo_replies == []:
            theqoo_replies = ''
        elif len(theqoo_replies) == 1:
            theqoo_replies = theqoo_replies[0]
    except:
        theqoo_replies = ''
    return theqoo_replies